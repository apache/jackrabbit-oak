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
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.InMemoryDataRecord;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static com.google.common.collect.Lists.newArrayList;

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
    
    public static final String DELIM = ",";
    
    /** The last modified time before current time of blobs to consider for garbage collection. */
    private final long maxLastModifiedInterval;

    /** The blob store to be garbage collected. */
    private final GarbageCollectableBlobStore blobStore;

    /** Helper class to mark blob references which **/
    private final BlobReferenceRetriever marker;

    private final Executor executor;

    /** The batch count. */
    private final int batchCount;

    private final String repoId;

    private final String root;

    /**
     * Creates an instance of MarkSweepGarbageCollector
     *
     * @param marker BlobReferenceRetriever instanced used to fetch refereed blob entries
     * @param blobStore the blob store instance
     * @param executor executor
     * @param root the root absolute path of directory under which temporary
     *             files would be created
     * @param batchCount batch sized used for saving intermediate state
     * @param maxLastModifiedInterval lastModifiedTime in millis. Only files with time
     *                                less than this time would be considered for GC
     * @param repositoryId - unique repository id for this node
     * @throws IOException
     */
    public MarkSweepGarbageCollector(
            BlobReferenceRetriever marker,
            GarbageCollectableBlobStore blobStore,
            Executor executor,
            String root,
            int batchCount,
            long maxLastModifiedInterval,
            @Nullable String repositoryId)
            throws IOException {
        this.executor = executor;
        this.blobStore = blobStore;
        this.marker = marker;
        this.batchCount = batchCount;
        this.maxLastModifiedInterval = maxLastModifiedInterval;
        this.repoId = repositoryId;
        this.root = root;
    }

    /**
     * Instantiates a new blob garbage collector.
     */
    public MarkSweepGarbageCollector(
            BlobReferenceRetriever marker,
            GarbageCollectableBlobStore blobStore,
            Executor executor,
            @Nullable String repositoryId)
            throws IOException {
        this(marker, blobStore, executor, TEMP_DIR, DEFAULT_BATCH_COUNT, TimeUnit.HOURS
                .toMillis(24), repositoryId);
    }

    public MarkSweepGarbageCollector(
            BlobReferenceRetriever marker,
            GarbageCollectableBlobStore blobStore,
            Executor executor,
            long maxLastModifiedInterval,
            @Nullable String repositoryId) throws IOException {
        this(marker, blobStore, executor, TEMP_DIR, DEFAULT_BATCH_COUNT, maxLastModifiedInterval, repositoryId);
    }

    @Override
    public void collectGarbage(boolean markOnly) throws Exception {
        markAndSweep(markOnly);
    }

    /**
     * Returns the stats related to GC for all repos
     * 
     * @return a list of GarbageCollectionRepoStats objects
     * @throws Exception
     */
    @Override
    public List<GarbageCollectionRepoStats> getStats() throws Exception {
        List<GarbageCollectionRepoStats> stats = newArrayList();
        if (SharedDataStoreUtils.isShared(blobStore)) {
            // Get all the references available
            List<DataRecord> refFiles =
                ((SharedDataStore) blobStore).getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());
            Map<String, DataRecord> references = Maps.uniqueIndex(refFiles, new Function<DataRecord, String>() {
                @Override 
                public String apply(DataRecord input) {
                    return SharedStoreRecordType.REFERENCES.getIdFromName(input.getIdentifier().toString());
                }
            });
    
            // Get all the markers available
            List<DataRecord> markerFiles =
                ((SharedDataStore) blobStore).getAllMetadataRecords(SharedStoreRecordType.MARKED_START_MARKER.getType());
            Map<String, DataRecord> markers = Maps.uniqueIndex(markerFiles, new Function<DataRecord, String>() {
                @Override
                public String apply(DataRecord input) {
                    return SharedStoreRecordType.MARKED_START_MARKER.getIdFromName(input.getIdentifier().toString());
                }
            });
            
            // Get all the repositories registered
            List<DataRecord> repoFiles =
                ((SharedDataStore) blobStore).getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType());
    
            for (DataRecord repoRec : repoFiles) {
                String repoId = SharedStoreRecordType.REFERENCES.getIdFromName(repoRec.getIdentifier().toString());
                GarbageCollectionRepoStats stat = new GarbageCollectionRepoStats();
                stat.setRepositoryId(repoId);
                if (references.containsKey(repoId)) {
                    DataRecord refRec = references.get(repoId);
                    stat.setEndTime(refRec.getLastModified());
                    stat.setLength(refRec.getLength());
                    
                    if (markers.containsKey(repoId)) {
                        stat.setStartTime(markers.get(repoId).getLastModified());    
                    }
                    
                    LineNumberReader reader = null;
                    try {
                        reader = new LineNumberReader(new InputStreamReader(refRec.getStream()));
                        while (reader.readLine() != null) {
                        }
                        stat.setNumLines(reader.getLineNumber());
                    } finally {
                        Closeables.close(reader, true);
                    }
                }
                stats.add(stat);
            }
        }
        return stats;
    }


    /**
     * Mark and sweep. Main entry method for GC.
     *
     * @param markOnly whether to mark only
     * @throws Exception the exception
     */
    protected void markAndSweep(boolean markOnly) throws Exception {
        boolean threw = true;
        GarbageCollectorFileState fs = new GarbageCollectorFileState(root);
        try {
            Stopwatch sw = Stopwatch.createStarted();
            LOG.info("Starting Blob garbage collection");
            
            long markStart = System.currentTimeMillis();
            mark(fs);
            if (!markOnly) {
                long deleteCount = sweep(fs, markStart);
                threw = false;

                LOG.info("Blob garbage collection completed in {}. Number of blobs deleted [{}]", sw.toString(),
                    deleteCount, maxLastModifiedInterval);
            }
        } finally {
            if (!LOG.isTraceEnabled()) {
                Closeables.close(fs, threw);
            }
        }
    }

    /**
     * Mark phase of the GC.
     * @param fs the garbage collector file state
     */
    protected void mark(GarbageCollectorFileState fs) throws IOException, DataStoreException {
        LOG.debug("Starting mark phase of the garbage collector");
        
        // Create a time marker in the data store if applicable
        GarbageCollectionType.get(blobStore).addMarkedStartMarker(blobStore, repoId);
        
        // Mark all used references
        iterateNodeTree(fs);

        // Move the marked references file to the data store meta area if applicable
        GarbageCollectionType.get(blobStore).addMarked(blobStore, fs, repoId);

        LOG.debug("Ending mark phase of the garbage collector");
    }

    /**
     * Difference phase where the GC candidates are identified.
     * 
     * @param fs the garbage collector file state
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private void difference(GarbageCollectorFileState fs) throws IOException {
        LOG.debug("Starting difference phase of the garbage collector");

        FileLineDifferenceIterator iter = new FileLineDifferenceIterator(
                fs.getMarkedRefs(),
                fs.getAvailableRefs());
        calculateDifference(fs, iter);

        LOG.debug("Ending difference phase of the garbage collector");
    }
    
    private long calculateDifference(GarbageCollectorFileState fs, FileLineDifferenceIterator iter) throws IOException {
        long numCandidates = 0;
        BufferedWriter bufferWriter = null;
        try {
            bufferWriter = Files.newWriter(fs.getGcCandidates(), Charsets.UTF_8);
            List<String> expiredSet = newArrayList();

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
            LOG.debug("Found candidates - " + numCandidates);
        } finally {
            IOUtils.closeQuietly(bufferWriter);
            IOUtils.closeQuietly(iter);
        }
        return numCandidates;
    }
    
    /**
     * Sweep phase of gc candidate deletion.
     * <p>
     * Performs the following steps depending upon the type of the blob store refer
     * {@link org.apache.jackrabbit.oak.plugins.blob.SharedDataStore.Type}:
     *
     * <ul>
     *     <li>Shared</li>
     *     <ul>
     *      <li> Merge all marked references (from the mark phase run independently) available in the data store meta
     *          store (from all configured independent repositories).
     *      <li> Retrieve all blob ids available.
     *      <li> Diffs the 2 sets above to retrieve list of blob ids not used.
     *      <li> Deletes only blobs created after
     *          (earliest time stamp of the marked references - #maxLastModifiedInterval) from the above set.
     *     </ul>
     *
     *     <li>Default</li>
     *     <ul>
     *      <li> Mark phase already run.
     *      <li> Retrieve all blob ids available.
     *      <li> Diffs the 2 sets above to retrieve list of blob ids not used.
     *      <li> Deletes only blobs created after
     *          (time stamp of the marked references - #maxLastModifiedInterval).
     *     </ul>
     * </ul>
     *
     * @return the number of blobs deleted
     * @throws Exception the exception
     * @param fs the garbage collector file state
     * @param markStart the start time of mark to take as reference for deletion
     */
    protected long sweep(GarbageCollectorFileState fs, long markStart) throws Exception {
        long earliestRefAvailTime;
        // Merge all the blob references available from all the reference files in the data store meta store
        // Only go ahead if merge succeeded
        try {
            earliestRefAvailTime =
                    GarbageCollectionType.get(blobStore).mergeAllMarkedReferences(blobStore, fs);
            LOG.debug("Earliest reference available for timestamp [{}]", earliestRefAvailTime);
            earliestRefAvailTime = (earliestRefAvailTime < markStart ? earliestRefAvailTime : markStart);
        } catch (Exception e) {
            return 0;
        }

        // Find all blob references after iterating over the whole repository
        (new BlobIdRetriever(fs)).call();

        // Calculate the references not used
        difference(fs);
        long count = 0;
        long deleted = 0;
        
        long lastMaxModifiedTime = getLastMaxModifiedTime(earliestRefAvailTime); 
        LOG.debug("Starting sweep phase of the garbage collector");
        LOG.debug("Sweeping blobs with modified time > than the configured max deleted time ({}). ",
                timestampToString(lastMaxModifiedTime));

        ConcurrentLinkedQueue<String> exceptionQueue = new ConcurrentLinkedQueue<String>();

        LineIterator iterator =
                FileUtils.lineIterator(fs.getGcCandidates(), Charsets.UTF_8.name());
        List<String> ids = newArrayList();

        while (iterator.hasNext()) {
            ids.add(iterator.next());

            if (ids.size() >= getBatchCount()) {
                count += ids.size();
                deleted += sweepInternal(ids, exceptionQueue, lastMaxModifiedTime);
                ids = newArrayList();
            }
        }
        if (!ids.isEmpty()) {
            count += ids.size();
            deleted += sweepInternal(ids, exceptionQueue, lastMaxModifiedTime);
        }

        BufferedWriter writer = null;
        try {
            if (!exceptionQueue.isEmpty()) {
                writer = Files.newWriter(fs.getGarbage(), Charsets.UTF_8);
                saveBatchToFile(newArrayList(exceptionQueue), writer);
            }
        } finally {
            LineIterator.closeQuietly(iterator);
            IOUtils.closeQuietly(writer);
        }

        if(!exceptionQueue.isEmpty()) {
            LOG.warn("Unable to delete some blobs entries from the blob store. Details around such blob entries can " 
                         + "be found in [{}]",
                        fs.getGarbage().getAbsolutePath());
        }
        if(count != deleted) {
            LOG.warn("Deleted only [{}] blobs entries from the [{}] candidates identified. This may happen if blob " 
                         + "modified time is > "
                         + "than the max deleted time ({})", deleted, count,
                        timestampToString(lastMaxModifiedTime));
        }

        // Remove all the merged marked references
        GarbageCollectionType.get(blobStore).removeAllMarkedReferences(blobStore);
        LOG.debug("Ending sweep phase of the garbage collector");
        return deleted;
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
     * 
     * @param ids
     * @param exceptionQueue
     * @param maxModified
     */
    private long sweepInternal(List<String> ids, ConcurrentLinkedQueue<String> exceptionQueue, long maxModified) {
        long deleted = 0;
        try {
            LOG.trace("Blob ids to be deleted {}", ids);
            deleted = blobStore.countDeleteChunks(ids, maxModified);
            if (deleted != ids.size()) {
                // Only log and do not add to exception queue since some blobs may not match the
                // lastMaxModifiedTime criteria.
                LOG.debug("Some [{}] blobs were not deleted from the batch : [{}]", ids.size() - deleted, ids);
            }
        } catch (Exception e) {
            LOG.warn("Error occurred while deleting blob with ids [{}]", ids, e);
            exceptionQueue.addAll(ids);
        }
        return deleted;
    }

    /**
     * Iterates the complete node tree and collect all blob references
     * @param fs the garbage collector file state
     */
    protected void iterateNodeTree(GarbageCollectorFileState fs) throws IOException {
        final BufferedWriter writer = Files.newWriter(fs.getMarkedRefs(), Charsets.UTF_8);
        final AtomicInteger count = new AtomicInteger();
        try {
            marker.collectReferences(
                    new ReferenceCollector() {
                        private final List<String> idBatch = Lists.newArrayListWithCapacity(getBatchCount());

                        private final boolean debugMode = LOG.isTraceEnabled();

                        @Override
                        public void addReference(String blobId, String nodeId) {
                            if (debugMode) {
                                LOG.trace("BlobId : {}, NodeId : {}", blobId, nodeId);
                            }

                            try {
                                Iterator<String> idIter = blobStore.resolveChunks(blobId);
                                Joiner delimJoiner = Joiner.on(DELIM).skipNulls();
                                while (idIter.hasNext()) {
                                    String id = idIter.next();
                                    
                                    idBatch.add(delimJoiner.join(id, nodeId));

                                    if (idBatch.size() >= getBatchCount()) {
                                        saveBatchToFile(idBatch, writer);
                                        idBatch.clear();
                                    }

                                    if (debugMode) {
                                        LOG.trace("chunkId : {}", id);
                                    }
                                    count.getAndIncrement();
                                }

                                if (!idBatch.isEmpty()) {
                                    saveBatchToFile(idBatch, writer);
                                    idBatch.clear();
                                }
                            } catch (Exception e) {
                                throw new RuntimeException("Error in retrieving references", e);
                            }
                        }
                    }
            );
            LOG.info("Number of valid blob references marked under mark phase of " +
                    "Blob garbage collection [{}]", count.get());
            // sort the marked references with the first part of the key
            GarbageCollectorFileState.sort(fs.getMarkedRefs(), 
                                              new Comparator<String>() {
                                                    @Override
                                                    public int compare(String s1, String s2) {
                                                        return s1.split(DELIM)[0].compareTo(s2.split(DELIM)[0]);
                                                    }
                                                });
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }
    
    /**
     * Checks for the DataStore consistency and reports the number of missing blobs still referenced.
     * 
     * @return the missing blobs
     * @throws Exception
     */
    @Override
    public long checkConsistency() throws Exception {
        boolean threw = true;
        GarbageCollectorFileState fs = new GarbageCollectorFileState(root);
        long candidates = 0;
        
        try {
            Stopwatch sw = Stopwatch.createStarted();
            LOG.info("Starting blob consistency check");
    
            // Find all blobs available in the blob store
            ListenableFutureTask<Integer> blobIdRetriever = ListenableFutureTask.create(new BlobIdRetriever(fs));
            executor.execute(blobIdRetriever);
    
            // Mark all used blob references
            iterateNodeTree(fs);
            
            try {
                blobIdRetriever.get();
            } catch (ExecutionException e) {
                LOG.warn("Error occurred while fetching all the blobIds from the BlobStore");
                threw = false;
                throw e;
            }
            
            LOG.trace("Starting difference phase of the consistency check");
            FileLineDifferenceIterator iter = new FileLineDifferenceIterator(fs.getAvailableRefs(), fs.getMarkedRefs());
            candidates = calculateDifference(fs, iter);
            LOG.trace("Ending difference phase of the consistency check");
            
            LOG.info("Consistency check found [{}] missing blobs", candidates);
            if (candidates > 0) {
                LOG.warn("Consistency check failure in the the blob store : {}, check missing candidates in file {}",
                            blobStore, fs.getGcCandidates().getAbsolutePath());
            }
        } finally {
            if (!LOG.isTraceEnabled() && candidates == 0) {
                Closeables.close(fs, threw);
            }
        }
        return candidates;
    }
    /**
     * BlobIdRetriever class to retrieve all blob ids.
     */
    private class BlobIdRetriever implements Callable<Integer> {
        private final GarbageCollectorFileState fs;
    
        public BlobIdRetriever(GarbageCollectorFileState fs) {
            this.fs = fs;
        }
    
        @Override
        public Integer call() throws Exception {
            LOG.debug("Starting retrieve of all blobs");
            BufferedWriter bufferWriter = null;
            int blobsCount = 0;
            try {
                bufferWriter = new BufferedWriter(
                        new FileWriter(fs.getAvailableRefs()));
                Iterator<String> idsIter = blobStore.getAllChunkIds(0);
                List<String> ids = newArrayList();

                while (idsIter.hasNext()) {
                    ids.add(idsIter.next());
                    if (ids.size() > getBatchCount()) {
                        blobsCount += ids.size();
                        saveBatchToFile(ids, bufferWriter);
                        LOG.debug("retrieved {} blobs", blobsCount);
                    }
                }

                if (!ids.isEmpty()) {
                    blobsCount += ids.size();
                    saveBatchToFile(ids, bufferWriter);
                    LOG.debug("retrieved {} blobs", blobsCount);
                }

                // sort the file
                GarbageCollectorFileState.sort(fs.getAvailableRefs());
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
    static class FileLineDifferenceIterator extends AbstractIterator<String> implements Closeable {
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
        
        private String getKey(String row) {
            return row.split(DELIM)[0];
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
                    int comparisonResult = getKey(diff).compareTo(getKey(marked));
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
                        if (!InMemoryDataRecord.isInstance(getKey(diff))) {
                            return diff;
                        } else {
                            diff = null;
                            break;
                        }
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

    /**
     * Defines different data store types from the garbage collection perspective and encodes the divergent behavior.
     * <ul></ul>
     */
    enum GarbageCollectionType {
        SHARED {
            /**
             * Remove the maked references and the marked markers from the blob store root. Default NOOP.
             * 
             * @param blobStore the blobStore instance
             */
            @Override
            void removeAllMarkedReferences(GarbageCollectableBlobStore blobStore) {
                ((SharedDataStore) blobStore).deleteAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());
                ((SharedDataStore) blobStore).deleteAllMetadataRecords(SharedStoreRecordType.MARKED_START_MARKER.getType());
            }

            /**
             * Merge all marked references available from all repositories and return the earliest time of the references.
             * 
             * @param blobStore the blob store
             * @param fs the fs
             * @return the long the earliest time of the available references
             * @throws IOException Signals that an I/O exception has occurred.
             * @throws DataStoreException the data store exception
             */
            @Override
            long mergeAllMarkedReferences(GarbageCollectableBlobStore blobStore,
                    GarbageCollectorFileState fs)
                    throws IOException, DataStoreException {

                List<DataRecord> refFiles =
                    ((SharedDataStore) blobStore).getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());

                // Get all the repositories registered
                List<DataRecord> repoFiles =
                    ((SharedDataStore) blobStore).getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType());

                // Retrieve repos for which reference files have not been created
                Set<String> unAvailRepos =
                        SharedDataStoreUtils.refsNotAvailableFromRepos(repoFiles, refFiles);
                if (unAvailRepos.isEmpty()) {
                    // List of files to be merged
                    List<File> files = newArrayList();
                    for (DataRecord refFile : refFiles) {
                        File file = GarbageCollectorFileState.copy(refFile.getStream());
                        files.add(file);
                    }

                    GarbageCollectorFileState.merge(files, fs.getMarkedRefs());
                    
                    // Get the timestamp to indicate the earliest mark phase start
                    List<DataRecord> markerFiles =
                        ((SharedDataStore) blobStore).getAllMetadataRecords(
                                                        SharedStoreRecordType.MARKED_START_MARKER.getType());
                    long earliestMarker = SharedDataStoreUtils.getEarliestRecord(markerFiles).getLastModified();
                    LOG.trace("Earliest marker timestamp {}", earliestMarker);

                    long earliestRef = SharedDataStoreUtils.getEarliestRecord(refFiles).getLastModified();
                    LOG.trace("Earliest ref timestamp {}", earliestRef);

                    return (earliestMarker < earliestRef ? earliestMarker : earliestRef);
                } else {
                    LOG.error("Not all repositories have marked references available : {}", unAvailRepos);
                    throw new IOException("Not all repositories have marked references available");
                }
            }

            /**
             * Adds the marked references to the blob store root. Default NOOP
             * 
             * @param blobStore the blob store
             * @param fs the fs
             * @param repoId the repo id
             * @throws DataStoreException the data store exception
             * @throws IOException Signals that an I/O exception has occurred.
             */
            @Override
            void addMarked(GarbageCollectableBlobStore blobStore, GarbageCollectorFileState fs,
                    String repoId) throws DataStoreException, IOException {
                InputStream is = new FileInputStream(fs.getMarkedRefs());
                try {
                    ((SharedDataStore) blobStore)
                        .addMetadataRecord(is, SharedStoreRecordType.REFERENCES.getNameFromId(repoId));
                } finally {
                    Closeables.close(is, false);
                }
            }
            
            @Override
            public void addMarkedStartMarker(GarbageCollectableBlobStore blobStore, String repoId) {
                try {
                    ((SharedDataStore) blobStore).addMetadataRecord(new ByteArrayInputStream(new byte[0]),
                                                                       SharedStoreRecordType.MARKED_START_MARKER
                                                                           .getNameFromId(repoId));
                } catch (DataStoreException e) {
                    LOG.debug("Error creating marked time marker for repo : {}", repoId);
                }
            }
        },
        DEFAULT;

        void removeAllMarkedReferences(GarbageCollectableBlobStore blobStore) {}

        void addMarked(GarbageCollectableBlobStore blobStore, GarbageCollectorFileState fs,
                String repoId) throws DataStoreException, IOException {}

        long mergeAllMarkedReferences(GarbageCollectableBlobStore blobStore,
                GarbageCollectorFileState fs)
                throws IOException, DataStoreException {
            // throw id the marked refs not available.
            if (!fs.getMarkedRefs().exists() || fs.getMarkedRefs().length() == 0) {
                throw new IOException("Marked references not available");
            }
            return fs.getMarkedRefs().lastModified();
        }

        public static GarbageCollectionType get(GarbageCollectableBlobStore blobStore) {
            if (SharedDataStoreUtils.isShared(blobStore)) {
                return SHARED;
            }
            return DEFAULT;
        }
    
        public void addMarkedStartMarker(GarbageCollectableBlobStore blobStore, String repoId) {}
    }
}
