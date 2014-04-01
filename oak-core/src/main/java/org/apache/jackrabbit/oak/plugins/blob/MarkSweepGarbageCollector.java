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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.MoreExecutors;
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
 * This class is not thread safe.
 * 
 */
public class MarkSweepGarbageCollector implements BlobGarbageCollector {

    public static final Logger LOG = LoggerFactory.getLogger(MarkSweepGarbageCollector.class);

    public static final String NEWLINE = StandardSystemProperty.LINE_SEPARATOR.value();

    public static final String TEMP_DIR = StandardSystemProperty.JAVA_IO_TMPDIR.value();

    public static final int DEFAULT_BATCH_COUNT = 2048;

    public static enum State {NOT_RUNNING, MARKING, SWEEPING}

    /** The last modified time before current time of blobs to consider for garbage collection. */
    private final long maxLastModifiedInterval;

    /** Run concurrently when possible. */
    private final boolean runConcurrently;

    /** The blob store to be garbage collected. */
    private final GarbageCollectableBlobStore blobStore;

    /** Helper class to mark blob references which **/
    private final BlobReferenceRetriever marker;
    
    /** The garbage collector file state */
    private final GarbageCollectorFileState fs;

    private final Executor executor;

    /** The batch count. */
    private final int batchCount;

    /** Flag to indicate the state of the gc **/
    private State state = State.NOT_RUNNING;

    /**
     * Creates an instance of MarkSweepGarbageCollector
     *
     * @param marker BlobReferenceRetriever instanced used to fetch refereedd blob entries
     * @param blobStore
     * @param root the root absolute path of directory under which temporary
     *             files would be created
     * @param batchCount batch sized used for saving intermediate state
     * @param runBackendConcurrently - run the backend iterate concurrently
     * @param maxLastModifiedInterval - lastModifiedTime in millis. Only files with time
     *                                less than this time would be considered for GC
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public MarkSweepGarbageCollector(
            BlobReferenceRetriever marker,
            GarbageCollectableBlobStore blobStore,
            Executor executor,
            String root,
            int batchCount,
            boolean runBackendConcurrently,
            long maxLastModifiedInterval)
            throws IOException {
        this.executor = executor;
        this.blobStore = blobStore;
        this.marker = marker;
        this.batchCount = batchCount;
        this.runConcurrently = runBackendConcurrently;
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
            GarbageCollectableBlobStore blobStore,
            Executor executor)
            throws IOException {
        this(marker, blobStore, executor, TEMP_DIR, DEFAULT_BATCH_COUNT, true, TimeUnit.HOURS.toMillis(24));
    }

    @Override
    public void collectGarbage() throws Exception {
        markAndSweep();
    }

    /**
     * Gets the state of the gc process.
     *
     * @return the state
     */
    public State getState() {
        return state;
    }

    /**
     * Mark and sweep. Main method for GC.
     * 
     * @throws Exception
     *             the exception
     */
    private void markAndSweep() throws IOException, InterruptedException {
        boolean threw = true;
        try {
            Stopwatch sw = Stopwatch.createStarted();
            LOG.info("Starting Blob garbage collection");

            mark();
            int deleteCount = sweep();
            threw = false;

            LOG.info("Blob garbage collection completed in {}. Number of blobs " +
                    "deleted [{}]", sw.toString(), deleteCount);
        } finally {
            Closeables.close(fs, threw);
            state = State.NOT_RUNNING;
        }
    }

    /**
     * Mark phase of the GC.
     */
    private void mark() throws IOException, InterruptedException {
        state = State.MARKING;
        LOG.debug("Starting mark phase of the garbage collector");

        // Find all blobs available in the blob store
        ListenableFutureTask<Integer> blobIdRetriever = ListenableFutureTask.create(new BlobIdRetriever());
        if (runConcurrently) {
            executor.execute(blobIdRetriever);
        } else {
            MoreExecutors.sameThreadExecutor().execute(blobIdRetriever);
        }

        // Find all blob references after iterating over the whole repository
        iterateNodeTree();

        try {
            blobIdRetriever.get();
        } catch (ExecutionException e) {
           LOG.warn("Error occurred while fetching all the blobIds from the BlobStore. GC would " +
                   "continue with the blobIds retrieved so far", e.getCause());
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
    private void difference() throws IOException {
        LOG.debug("Starting difference phase of the garbage collector");

        FileLineDifferenceIterator iter = new FileLineDifferenceIterator(
                fs.getMarkedRefs(),
                fs.getAvailableRefs(), batchCount);

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
    private int sweep() throws IOException {
        int count = 0;
        state = State.SWEEPING;
        LOG.debug("Starting sweep phase of the garbage collector");

        ConcurrentLinkedQueue<String> exceptionQueue = new ConcurrentLinkedQueue<String>();

        LineIterator iterator =
                FileUtils.lineIterator(fs.getGcCandidates(), Charsets.UTF_8.name());
        List<String> ids = Lists.newArrayList();

        while (iterator.hasNext()) {
            ids.add(iterator.next());

            if (ids.size() > getBatchCount()) {
                count += ids.size();
                executor.execute(new Sweeper(ids, exceptionQueue));
                ids = Lists.newArrayList();
            }
        }
        if (!ids.isEmpty()) {
            count += ids.size();
            executor.execute(new Sweeper(ids, exceptionQueue));
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
        if(!exceptionQueue.isEmpty()) {
            LOG.warn("Unable to delete some blob entries from the blob store. Details around such blob entries " +
                    "can be found in [{}]", fs.getGarbage().getAbsolutePath());
        }
        LOG.debug("Ending sweep phase of the garbage collector");
        return count;
    }

    private int getBatchCount() {
        return batchCount;
    }

    /**
     * Save batch to file.
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

        public Sweeper(List<String> ids, ConcurrentLinkedQueue<String> exceptionQueue) {
            this.exceptionQueue = exceptionQueue;
            this.ids = ids;
        }

        @Override
        public void run() {
            try {
                LOG.debug("Blob ids to be deleted {}", ids);
                boolean deleted =
                        blobStore.deleteChunks(ids,
                                        (maxLastModifiedInterval > 0 ? System.currentTimeMillis()
                                                - maxLastModifiedInterval : 0));
                if (!deleted) {
                    exceptionQueue.addAll(ids);
                }
            } catch (Exception e) {
                LOG.warn("Error occurred while deleting blob with ids [{}]", ids, e);
                exceptionQueue.addAll(ids);
            }
        }
    }

    /**
     * Iterates the complete node tree and collect all blob references
     */
    private void iterateNodeTree() throws IOException {
        final BufferedWriter writer = Files.newWriter(fs.getMarkedRefs(), Charsets.UTF_8);
        try {
            marker.collectReferences(
                    new ReferenceCollector() {
                        private final List<String> idBatch = Lists.newArrayListWithCapacity(getBatchCount());

                        private int count = 0;

                        private final boolean debugMode = LOG.isTraceEnabled();

                        @Override
                        public void addReference(String blobId) {
                            if (debugMode) {
                                LOG.trace("BlobId : {}",blobId);
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
                                        LOG.trace("chunkId : {}",id);
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

                            LOG.info("Number of valid blob references marked under mark phase of " +
                                    "Blob garbage collection [{}]",count);
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
    private class BlobIdRetriever implements Callable<Integer> {
        @Override
        public Integer call() throws Exception {
            LOG.debug("Starting retrieve of all blobs");
            BufferedWriter bufferWriter = null;
            int blobsCount = 0;
            try {
                bufferWriter = new BufferedWriter(
                        new FileWriter(fs.getAvailableRefs()));
                Iterator<String> idsIter = blobStore.getAllChunkIds(maxLastModifiedInterval);
                
                List<String> ids = Lists.newArrayList();

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
                LOG.debug("Ending retrieving all blobs : {}", blobsCount);
            } finally {
                IOUtils.closeQuietly(bufferWriter);
            }
            return blobsCount;
        }


    }

    /**
     * FileLineDifferenceIterator class which iterates over the difference of 2 files line by line.
     */
    static class FileLineDifferenceIterator implements Iterator<String> {

        /** The marked references iterator. */
        private final LineIterator markedIter;

        /** The available references iter. */
        private final LineIterator allIter;

        private final ArrayDeque<String> queue;

        private final int batchSize;

        private boolean done;

        /** Temporary buffer. */
        private TreeSet<String> markedBuffer;

        /**
         * Instantiates a new file line difference iterator.
         */
        public FileLineDifferenceIterator(File marked, File available, int batchSize) throws IOException {
            this.markedIter = FileUtils.lineIterator(marked);
            this.allIter = FileUtils.lineIterator(available);
            this.batchSize = batchSize;
            queue = new ArrayDeque<String>(batchSize);
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
                    gcSet.size() < batchSize) {
                TreeSet<String> allBuffer = new TreeSet<String>();

                while (markedIter.hasNext() &&
                        markedBuffer.size() < batchSize) {
                    String stre = markedIter.next();
                    markedBuffer.add(stre);
                }
                while (allIter.hasNext() &&
                        allBuffer.size() < batchSize) {
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
