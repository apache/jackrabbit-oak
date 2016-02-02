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
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
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
    protected final GarbageCollectorFileState fs;

    private final Executor executor;

    /** The batch count. */
    private final int batchCount;

    /** Flag to indicate the state of the gc **/
    private State state = State.NOT_RUNNING;

    /**
     * Creates an instance of MarkSweepGarbageCollector
     *
     * @param marker BlobReferenceRetriever instanced used to fetch refereedd blob entries
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
     */
    public MarkSweepGarbageCollector(
            BlobReferenceRetriever marker, 
            GarbageCollectableBlobStore blobStore,
            Executor executor)
            throws IOException {
        this(marker, blobStore, executor, TEMP_DIR, DEFAULT_BATCH_COUNT, true, TimeUnit.HOURS.toMillis(24));
    }

    public MarkSweepGarbageCollector(
            BlobReferenceRetriever marker,
            GarbageCollectableBlobStore blobStore,
            Executor executor,
            long maxLastModifiedInterval)
            throws IOException {
        this(marker, blobStore, executor, TEMP_DIR, DEFAULT_BATCH_COUNT, true, maxLastModifiedInterval);
    }

    @Override
    public void collectGarbage() throws Exception {
        try {
            markAndSweep();
        } catch (Exception e) {
            LOG.error("Blob garbage collection error", e);
            throw e;
        }
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
     */
    protected void markAndSweep() throws IOException, InterruptedException {
        boolean threw = true;
        try {
            Stopwatch sw = Stopwatch.createStarted();
            LOG.info("Starting Blob garbage collection");
            
            long markStart = System.currentTimeMillis();
            mark();
            int deleteCount = sweep(markStart);
            threw = false;
    
            long maxTime = getLastMaxModifiedTime(markStart) > 0 ? getLastMaxModifiedTime(markStart) : markStart;
            LOG.info(
                "Blob garbage collection completed in {}. Number of blobs identified for deletion [{}] (This "
                    + "includes blobs newer than configured interval [{}] which are ignored for deletion)",
                sw.toString(), deleteCount, timestampToString(maxTime));
        } finally {
            if (!LOG.isTraceEnabled()) {
                Closeables.close(fs, threw);
            }
            state = State.NOT_RUNNING;
        }
    }

    /**
     * Mark phase of the GC.
     */
    protected void mark() throws IOException, InterruptedException {
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
    protected void difference() throws IOException {
        LOG.debug("Starting difference phase of the garbage collector");

        FileLineDifferenceIterator iter = new FileLineDifferenceIterator(
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
            IOUtils.closeQuietly(iter);
        }

        LOG.debug("Ending difference phase of the garbage collector");
    }

    /**
     * Sweep phase of gc candidate deletion.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @param markStart the start time of mark to take as reference for deletion
     */
    protected int sweep(long markStart) throws IOException {
        int count = 0;
        state = State.SWEEPING;
        long lastMaxModifiedTime = getLastMaxModifiedTime(markStart);
        LOG.debug("Starting sweep phase of the garbage collector");
        LOG.debug("Sweeping blobs with modified time > than the configured max deleted time ({}). " +
                timestampToString(lastMaxModifiedTime));

        ConcurrentLinkedQueue<String> exceptionQueue = new ConcurrentLinkedQueue<String>();

        LineIterator iterator =
                FileUtils.lineIterator(fs.getGcCandidates(), Charsets.UTF_8.name());
        List<String> ids = Lists.newArrayList();

        while (iterator.hasNext()) {
            ids.add(iterator.next());

            if (ids.size() > getBatchCount()) {
                count += ids.size();
                sweepInternal(ids, exceptionQueue, lastMaxModifiedTime);
                ids = Lists.newArrayList();
            }
        }
        if (!ids.isEmpty()) {
            count += ids.size();
            sweepInternal(ids, exceptionQueue, lastMaxModifiedTime);
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
            LOG.warn("Unable to delete some blobs entries from the blob store. " +
                    "This may happen if blob modified time is > than the max deleted time ({}). " +
                    "Details around such blob entries can be found in [{}]",
                    timestampToString(lastMaxModifiedTime), fs.getGarbage().getAbsolutePath());
        }
        LOG.debug("Ending sweep phase of the garbage collector");
        return count;
    }

    private int getBatchCount() {
        return batchCount;
    }

    private long getLastMaxModifiedTime(long maxModified) {
        return maxLastModifiedInterval > 0 ?
            ((maxModified <= 0 ? System.currentTimeMillis() : maxModified) - maxLastModifiedInterval) :
            0;
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
     * Deletes a batch of blobs from blob store.
     * @param ids the ids to delete
     * @param exceptionQueue the queue to save exceptions
     * @param lastMaxModifiedTime the max modified time of blobs to delete
     */
    private void sweepInternal(List<String> ids, ConcurrentLinkedQueue<String> exceptionQueue, long lastMaxModifiedTime) {
        try {
            LOG.debug("Blob ids to be deleted {}", ids);
            boolean deleted = blobStore.deleteChunks(ids, lastMaxModifiedTime);
            if (!deleted) {
                // Only log and do not add to exception queue since some blobs may not match the
                // lastMaxModifiedTime criteria.
                LOG.debug("Some blobs were not deleted from the batch : [{}]", ids);
            }
        } catch (Exception e) {
            LOG.warn("Error occurred while deleting blob with ids [{}]", ids, e);
            exceptionQueue.addAll(ids);
        }
    }

    /**
     * Iterates the complete node tree and collect all blob references
     */
    protected void iterateNodeTree() throws IOException {
        final BufferedWriter writer = Files.newWriter(fs.getMarkedRefs(), Charsets.UTF_8);
        final AtomicInteger count = new AtomicInteger();
        try {
            marker.collectReferences(
                    new ReferenceCollector() {
                        private final List<String> idBatch = Lists.newArrayListWithCapacity(getBatchCount());

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
                                    count.getAndIncrement();
                                }

                                if (!idBatch.isEmpty()) {
                                    saveBatchToFile(idBatch, writer);
                                    idBatch.clear();
                                }

                                if (count.get() % getBatchCount() == 0) {
                                    LOG.info("Collected ({}) blob references", count.get());
                                }
                            } catch (Exception e) {
                                throw new RuntimeException("Error in retrieving references", e);
                            }
                        }
                    }
            );
            LOG.info("Number of valid blob references marked under mark phase of " +
                    "Blob garbage collection [{}]",count.get());
            // sort the marked references
            fs.sort(fs.getMarkedRefs());
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }


    /**
     * BlobIdRetriever class to retrieve all blob ids.
     */
    public class BlobIdRetriever implements Callable<Integer> {
        @Override
        public Integer call() throws Exception {
            LOG.debug("Starting retrieve of all blobs");
            BufferedWriter bufferWriter = null;
            int blobsCount = 0;
            try {
                bufferWriter = new BufferedWriter(
                        new FileWriter(fs.getAvailableRefs()));
                Iterator<String> idsIter = blobStore.getAllChunkIds(0);
                List<String> ids = Lists.newArrayList();

                while (idsIter.hasNext()) {
                    ids.add(idsIter.next());
                    if (ids.size() > getBatchCount()) {
                        blobsCount += ids.size();
                        saveBatchToFile(ids, bufferWriter);
                        LOG.info("Retrieved ({}) blobs", blobsCount);
                    }
                }

                if (!ids.isEmpty()) {
                    blobsCount += ids.size();
                    saveBatchToFile(ids, bufferWriter);
                    LOG.info("Retrieved ({}) blobs", blobsCount);
                }

                // sort the file
                fs.sort(fs.getAvailableRefs());
                LOG.info("Number of blobs present in BlobStore : [{}] ", blobsCount);
            } finally {
                IOUtils.closeQuietly(bufferWriter);
            }
            return blobsCount;
        }


    }


    /**
     * FileLineDifferenceIterator class which iterates over the difference of 2 files line by line.
     */
    static class FileLineDifferenceIterator extends AbstractIterator<String> implements Closeable{
        private final PeekingIterator<String> peekMarked;
        private final LineIterator marked;
        private final LineIterator all;

        public FileLineDifferenceIterator(File marked, File available) throws IOException {
            this(FileUtils.lineIterator(marked), FileUtils.lineIterator(available));
        }

        public FileLineDifferenceIterator(LineIterator marked, LineIterator available) throws IOException {
            this.marked = marked;
            this.peekMarked = Iterators.peekingIterator(marked);
            this.all = available;
        }

        @Override
        protected String computeNext() {
            String diff = computeNextDiff();
            if (diff == null) {
                close();
                return endOfData();
            }
            return diff;
        }

        @Override
        public void close() {
            LineIterator.closeQuietly(marked);
            LineIterator.closeQuietly(all);
        }

        private String computeNextDiff() {
            if (!all.hasNext()) {
                return null;
            }

            //Marked finish the rest of all are part of diff
            if (!peekMarked.hasNext()) {
                return all.next();
            }
            
            String diff = null;
            while (all.hasNext() && diff == null) {
                diff = all.next();
                while (peekMarked.hasNext()) {
                    String marked = peekMarked.peek();
                    int comparisonResult = diff.compareTo(marked);
                    if (comparisonResult > 0) {
                        //Extra entries in marked. Ignore them and move on
                        peekMarked.next();
                    } else if (comparisonResult == 0) {
                        //Matching entry found in marked move past it. Not a
                        //dif candidate
                        peekMarked.next();
                        diff = null;
                        break;
                    } else {
                        //This entry is not found in marked entries
                        //hence part of diff
                        return diff;
                    }
                }
            }
            return diff;
        }
    }

    /**
     * Provides a readable string for given timestamp
     */
    private static String timestampToString(long timestamp){
        return (new Timestamp(timestamp) + "00").substring(0, 23);
    }
}
