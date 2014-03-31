/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mark and sweep garbage collector.
 * 
 * Uses the file system to store internal state while in process to account for huge data.
 * 
 */
public class MarkSweepGarbageCollector implements BlobGarbageCollector {

    public static final Logger LOG = LoggerFactory.getLogger(MarkSweepGarbageCollector.class);

    public static final String NEWLINE = StandardSystemProperty.LINE_SEPARATOR.value();

    public static final String TEMP_DIR = StandardSystemProperty.JAVA_IO_TMPDIR.value();

    public static final int DEFAULT_BATCH_COUNT = 2048;

    public static final String NOT_RUNNING = "NotRunning";

    public static final String MARKING = "Running-Marking";

    public static final String SWEEPING = "Running-Sweeping";

    /** The last modified time before current time of blobs to consider for garbage collection. */
    private long maxLastModifiedInterval = TimeUnit.HOURS.toMillis(24);

    /** Run concurrently when possible. */
    private boolean runConcurrently = true;

    /** The number of sweeper threads to use. */
    private int numSweepers = 1;

    /** The blob store to be garbage collected. */
    private GarbageCollectableBlobStore blobStore;

    /** Helper class to mark blob references which **/
    private BlobReferenceRetriever marker;
    
    /** The garbage collector file state */
    private GarbageCollectorFileState fs;

    /** The configured root to store gc process files. */
    private String root = TEMP_DIR;

    /** The batch count. */
    private int batchCount = DEFAULT_BATCH_COUNT;

    /** Flag to indicate whether to run in a debug mode **/
    private final boolean debugMode = Boolean.getBoolean("debugModeGC") || LOG.isDebugEnabled();

    /** Flag to indicate the state of the gc **/
    private String state = NOT_RUNNING;

    /**
     * Gets the max last modified interval considered for garbage collection.
     * 
     * @return the max last modified interval
     */
    protected long getMaxLastModifiedInterval() {
        return maxLastModifiedInterval;
    }

    /**
     * Sets the max last modified interval considered for garbage collection.
     * 
     * @param maxLastModifiedInterval the new max last modified interval
     */
    protected void setMaxLastModifiedInterval(long maxLastModifiedInterval) {
        this.maxLastModifiedInterval = maxLastModifiedInterval;
    }
    
    /**
     * Gets the root.
     * 
     * @return the root
     */
    protected String getRoot() {
        return root;
    }

    /**
     * Gets the batch count.
     * 
     * @return the batch count
     */
    protected int getBatchCount() {
        return batchCount;
    }

    /**
     * Checks if run concurrently.
     * 
     * @return true, if is run concurrently
     */
    protected boolean isRunConcurrently() {
        return runConcurrently;
    }

    /**
     * Gets the number sweepers.
     * 
     * @return the number sweepers
     */
    protected int getNumSweepers() {
        return numSweepers;
    }

    /**
     * Gets the state of the gc process.
     * 
     * @return the state
     */
    protected String getState() {
        return state;
    }

    /**
     * @param marker
     * @param blobStore
     * @param root the root
     * @param batchCount the batch count
     * @param runBackendConcurrently - run the backend iterate concurrently
     * @param maxSweeperThreads the max sweeper threads
     * @param maxLastModifiedInterval
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MarkSweepGarbageCollector(
            BlobReferenceRetriever marker,
            GarbageCollectableBlobStore blobStore,
            String root,
            int batchCount,
            boolean runBackendConcurrently,
            int maxSweeperThreads,
            long maxLastModifiedInterval)
            throws IOException {
        this.blobStore = blobStore;
        this.marker = marker;
        this.batchCount = batchCount;
        this.root = root;
        this.runConcurrently = runBackendConcurrently;
        this.numSweepers = maxSweeperThreads;
        this.maxLastModifiedInterval = maxLastModifiedInterval;
        fs = new GarbageCollectorFileState(root);        
    }

    /**
     * Instantiates a new blob garbage collector.
     * 
     * @param marker
     * @param blobStore
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MarkSweepGarbageCollector(
            BlobReferenceRetriever marker, 
            GarbageCollectableBlobStore blobStore)
            throws IOException {
        Preconditions.checkState(!Strings.isNullOrEmpty(root));

        this.blobStore = blobStore;
        this.marker = marker;
        fs = new GarbageCollectorFileState(root);
    }

    @Override
    public void collectGarbage() throws Exception {
        markAndSweep();
    }

    /**
     * Mark and sweep. Main method for GC.
     * 
     * @throws Exception
     *             the exception
     */
    public void markAndSweep() throws Exception {
        try {
            LOG.debug("Starting garbage collector");

            mark();
            sweep();

            LOG.debug("garbage collector finished");
        } finally {
            fs.complete();
            state = NOT_RUNNING;
        }
    }

    /**
     * Mark phase of the GC.
     * 
     * @throws Exception
     *             the exception
     */
    public void mark() throws Exception {
        state = MARKING;
        LOG.debug("Starting mark phase of the garbage collector");

        // Find all blobs available in the blob store
        Thread blobIdRetrieverThread = null;
        if (runConcurrently) {
            blobIdRetrieverThread = new Thread(new BlobIdRetriever(), 
                    this.getClass().getSimpleName() + "-MarkThread");
            blobIdRetrieverThread.setDaemon(true);
            blobIdRetrieverThread.start();
        } else {
            (new BlobIdRetriever()).retrieve();
        }

        // Find all blob references after iterating over the whole repository
        iterateNodeTree();

        if (runConcurrently) {
            if (blobIdRetrieverThread.isAlive()) {
                blobIdRetrieverThread.join();
            }
        }
        difference();
        LOG.debug("Ending mark phase of the garbage collector");
    }

    /**
     * Difference phase where the GC candidates are identified.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    protected void difference() throws IOException {
        LOG.debug("Starting difference phase of the garbage collector");

        FileLineDifferenceIterator<String> iter = new FileLineDifferenceIterator<String>(
                fs.getMarkedRefs(),
                fs.getAvailableRefs());

        BufferedWriter bufferWriter = null;
        try {
            bufferWriter = Files.newWriter(fs.getGcCandidates(), Charsets.UTF_8);
            List<String> expiredSet = Lists.newArrayList();

            int numCandidates = 0;
            while (iter.hasNext()) {
                expiredSet.add(iter.next());
                if (expiredSet.size() > getBatchCount()) {
                    numCandidates += expiredSet.size();
                    saveBatchToFile(expiredSet, bufferWriter);
                }
            }

            if (!expiredSet.isEmpty()) {
                numCandidates += expiredSet.size();
                saveBatchToFile(expiredSet, bufferWriter);
            }
            LOG.debug("Found GC candidates - " + numCandidates);
        } finally {
            IOUtils.closeQuietly(bufferWriter);
        }

        LOG.debug("Ending difference phase of the garbage collector");
    }

    /**
     * Sweep phase of gc candidate deletion.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public void sweep() throws IOException {
        try {        
            state = SWEEPING;        
            LOG.debug("Starting sweep phase of the garbage collector");
    
            ConcurrentLinkedQueue<String> exceptionQueue = new ConcurrentLinkedQueue<String>();
            ExecutorService executorService =
                    new ThreadPoolExecutor(getNumSweepers(), getNumSweepers(), 1,
                            TimeUnit.MINUTES,
                            new LinkedBlockingQueue<Runnable>(),
                            new ThreadFactory() {
                                private final AtomicInteger threadCounter = new AtomicInteger();
    
                                private String getName() {
                                    return "MarkSweepGarbageCollector-Sweeper-" + threadCounter.getAndIncrement();
                                }
    
                                @Override
                                public Thread newThread(Runnable r) {
                                    Thread thread = new Thread(r, getName());
                                    thread.setDaemon(true);
                                    return thread;
                                }
                            });
    
            LineIterator iterator = 
                    FileUtils.lineIterator(fs.getGcCandidates(), Charsets.UTF_8.name());
            List<String> ids = Lists.newArrayList();
            int count = 0;
            while (iterator.hasNext()) {
                ids.add(iterator.next());
    
                if (ids.size() > getBatchCount()) {
                    count += ids.size();
                    executorService.execute(new Sweeper(ids, exceptionQueue));
                    ids = Lists.newArrayList();
                }
            }
            if (!ids.isEmpty()) {
                count += ids.size();
                executorService.execute(new Sweeper(ids, exceptionQueue));
            }
    
            try {
                executorService.shutdown();
                executorService.awaitTermination(100, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                LOG.error("Exception while waiting for termination of the executor service", e);
                LOG.error("Immediately shutdown");
                executorService.shutdownNow();
            }
    
            count -= exceptionQueue.size();
            BufferedWriter writer = null;
            try {
                if (!exceptionQueue.isEmpty()) {
                    writer = Files.newWriter(fs.getGarbage(), Charsets.UTF_8);
                    saveBatchToFile(Lists.newArrayList(exceptionQueue), writer);
                }
            } finally {
                LineIterator.closeQuietly(iterator);
                IOUtils.closeQuietly(writer);
            }
    
            LOG.info("Blobs deleted count - " + count);
            LOG.debug("Ending sweep phase of the garbage collector");
        } finally {
            fs.complete();
            state = NOT_RUNNING;
        }
    }

    /**
     * Save batch to file.
     * 
     * @param ids
     *            the ids
     * @param writer
     *            the writer
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    static void saveBatchToFile(List<String> ids, BufferedWriter writer) throws IOException {
        writer.append(Joiner.on(NEWLINE).join(ids));
        writer.append(NEWLINE);
        ids.clear();
        writer.flush();
    }

    /**
     * Sweeper thread.
     */
    class Sweeper implements Runnable {

        /** The exception queue. */
        private final ConcurrentLinkedQueue<String> exceptionQueue;

        /** The ids to sweep. */
        private final List<String> ids;

        /**
         * Instantiates a new sweeper.
         * 
         * @param ids
         *            the ids
         * @param exceptionQueue
         *            the exception queue
         */
        public Sweeper(List<String> ids, ConcurrentLinkedQueue<String> exceptionQueue) {
            this.exceptionQueue = exceptionQueue;
            this.ids = ids;
        }

        @Override
        public void run() {
            try {
                LOG.debug("Deleting blobs : " + ids);
                boolean deleted =
                        blobStore.deleteChunks(ids,
                                        (maxLastModifiedInterval > 0 ? System.currentTimeMillis()
                                                - maxLastModifiedInterval : 0));
                if (!deleted) {
                    exceptionQueue.addAll(ids);
                }
            } catch (Exception e) {
                LOG.error("Error in deleting blobs - " + ids, e);
                exceptionQueue.addAll(ids);
            }
        }
    }

    /**
     * Iterates the complete node tree.
     * 
     * @return the list
     * @throws Exception
     *             the exception
     */
    private void iterateNodeTree() throws Exception {
        final BufferedWriter writer = Files.newWriter(fs.getMarkedRefs(), Charsets.UTF_8);
        try {
            marker.collectReferences(
                    new ReferenceCollector() {
                        private final List<String> idBatch = Lists
                                .newArrayListWithCapacity(getBatchCount());

                        private int count = 0;

                        @Override
                        public void addReference(String blobId) {
                            if (debugMode) {
                                LOG.debug("BlobId : " + blobId);
                            }

                            try {
                                Iterator<String> idIter = blobStore.resolveChunks(blobId);
                                while (idIter.hasNext()) {
                                    String id = idIter.next();
                                    idBatch.add(id);

                                    if (idBatch.size() >= getBatchCount()) {
                                        saveBatchToFile(idBatch, writer);
                                        idBatch.clear();
                                    }

                                    if (debugMode) {
                                        LOG.debug("chunkId : " + id);
                                    }
                                    count++;
                                }

                                if (!idBatch.isEmpty()) {
                                    saveBatchToFile(idBatch, writer);
                                    idBatch.clear();
                                }
                            } catch (Exception e) {
                                throw new RuntimeException("Error in retrieving references", e);
                            }

                            LOG.info("Marked Reference : " + count);
                        }
                    }
            );

            // sort the marked references
            fs.sort(fs.getMarkedRefs());
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }


    /**
     * BlobIdRetriever class to retrieve all blob ids.
     */
    class BlobIdRetriever implements Runnable {
        @Override
        public void run() {
            retrieve();
        }

        /**
         * Retrieve.
         */
        protected void retrieve() {
            LOG.debug("Starting retrieve of all blobs");

            BufferedWriter bufferWriter = null;
            try {
                bufferWriter = new BufferedWriter(
                        new FileWriter(fs.getAvailableRefs()));
                Iterator<String> idsIter = blobStore.getAllChunkIds(maxLastModifiedInterval);
                
                List<String> ids = Lists.newArrayList();
                int blobsCount = 0;
                while (idsIter.hasNext()) {
                    ids.add(idsIter.next());
                    if (ids.size() > getBatchCount()) {
                        blobsCount += ids.size();
                        saveBatchToFile(ids, bufferWriter);
                    }
                }

                if (!ids.isEmpty()) {
                    blobsCount += ids.size();
                    saveBatchToFile(ids, bufferWriter);
                }

                // sort the file
                fs.sort(fs.getAvailableRefs());
                LOG.debug("Ending retrieving all blobs : " + blobsCount);
            } catch (Exception e) {
                LOG.error("Error retrieving available blob ids", e);
            } finally {
                IOUtils.closeQuietly(bufferWriter);
            }
        }
    }

    /**
     * FileLineDifferenceIterator class which iterates over the difference of 2 files line by line.
     * 
     * @param <T>
     *            the generic type
     */
    class FileLineDifferenceIterator<T> implements Iterator<String> {

        /** The marked references iterator. */
        private final LineIterator markedIter;

        /** The available references iter. */
        private final LineIterator allIter;

        private final ArrayDeque<String> queue;

        private boolean done;

        /** Temporary buffer. */
        private TreeSet<String> markedBuffer;

        /**
         * Instantiates a new file line difference iterator.
         * 
         * @param marked
         *            the marked
         * @param available
         *            the available
         * @throws IOException
         *             Signals that an I/O exception has occurred.
         */
        public FileLineDifferenceIterator(File marked, File available) throws IOException {
            this.markedIter = FileUtils.lineIterator(marked);
            this.allIter = FileUtils.lineIterator(available);
            queue = new ArrayDeque<String>(getBatchCount());
            markedBuffer = Sets.newTreeSet();
        }

        /**
         * Close.
         */
        private void close() {
            LineIterator.closeQuietly(markedIter);
            LineIterator.closeQuietly(allIter);
        }

        @Override
        public boolean hasNext() {
            if (!queue.isEmpty()) {
                return true;
            } else if (done) {
                return false;
            } else {
                if (!markedIter.hasNext() && !allIter.hasNext()) {
                    done = true;
                    close();
                    return false;
                } else {
                    queue.addAll(difference());
                    if (!queue.isEmpty()) {
                        return true;
                    } else {
                        done = true;
                        close();
                    }
                }
            }

            return false;
        }

        @Override
        public String next() {
            return nextDifference();
        }

        /**
         * Next difference.
         * 
         * @return the string
         */
        public String nextDifference() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more difference");
            }
            return queue.remove();
        }

        /**
         * Difference.
         * 
         * @return the sets the
         */
        protected Set<String> difference() {
            TreeSet<String> gcSet = new TreeSet<String>();

            // Iterate till the gc candidate set is at least SAVE_BATCH_COUNT or
            // the
            // blob id set iteration is complete
            while (allIter.hasNext() &&
                    gcSet.size() < getBatchCount()) {
                TreeSet<String> allBuffer = new TreeSet<String>();

                while (markedIter.hasNext() &&
                        markedBuffer.size() < getBatchCount()) {
                    String stre = markedIter.next();
                    markedBuffer.add(stre);
                }
                while (allIter.hasNext() &&
                        allBuffer.size() < getBatchCount()) {
                    String stre = allIter.next();
                    allBuffer.add(stre);
                }

                if (markedBuffer.isEmpty()) {
                    gcSet = allBuffer;
                } else {
                    gcSet.addAll(
                            Sets.difference(allBuffer, markedBuffer));

                    if (allBuffer.last().compareTo(markedBuffer.last()) < 0) {
                        // filling markedLeftoverBuffer
                        TreeSet<String> markedLeftoverBuffer = Sets.newTreeSet();
                        markedLeftoverBuffer.addAll(markedBuffer.tailSet(allBuffer.last(), false));
                        markedBuffer = markedLeftoverBuffer;
                        markedLeftoverBuffer = null;
                    } else {
                        markedBuffer.clear();
                    }
                }
            }

            return gcSet;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
