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

import static java.io.File.createTempFile;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.io.FileUtils.copyFile;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.copy;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.merge;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.sort;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.collect.FluentIterable;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.ImmutableListMultimap;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.guava.common.io.Closeables;
import org.apache.jackrabbit.guava.common.io.Files;
import org.apache.jackrabbit.guava.common.util.concurrent.ListenableFutureTask;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.jmx.CheckpointMBean;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.commons.io.FileLineDifferenceIterator;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobIdTracker;
import org.apache.jackrabbit.oak.plugins.blob.datastore.BlobTracker;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.Nullable;
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

    public static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    public static final int DEFAULT_BATCH_COUNT = 1024;

    public static final String DELIM = ",";

    private static final java.util.function.Function<String, String> transformer = new java.util.function.Function<String, String>() {
        @Nullable
        @Override
        public String apply(@Nullable String input) {
            if (input != null) {
                return input.split(DELIM)[0];
            }
            return "";
        }};

    /** The last modified time before current time of blobs to consider for garbage collection. */
    private final long maxLastModifiedInterval;

    /** The blob store to be garbage collected. */
    private final GarbageCollectableBlobStore blobStore;

    /** Flag to enable low cost consistency check after DSGC */
    private boolean checkConsistencyAfterGc;

    /* Flag to stop sweep if references not old enough */
    private final boolean sweepIfRefsPastRetention;

    /** Helper class to mark blob references which **/
    private final BlobReferenceRetriever marker;

    private final Executor executor;

    /** The batch count. */
    private final int batchCount;

    private final String repoId;

    private final String root;

    private final Whiteboard whiteboard;

    private CheckpointMBean checkpointMbean;

    /** Operation stats object **/
    private final GarbageCollectionOperationStats stats;

    private final OperationStatsCollector statsCollector;

    /** Operation consistency stats object **/
    private final GarbageCollectionOperationStats consistencyStats;

    private final OperationStatsCollector consistencyStatsCollector;

    private boolean traceOutput;

    private Clock clock;

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
     * @param repositoryId unique repository id for this node
     * @param whiteboard whiteboard instance
     * @param statisticsProvider statistics provider instance
     * @throws IOException
     */
    public MarkSweepGarbageCollector(
            BlobReferenceRetriever marker,
            GarbageCollectableBlobStore blobStore,
            Executor executor,
            String root,
            int batchCount,
            long maxLastModifiedInterval,
            boolean checkConsistencyAfterGc,
            boolean sweepIfRefsPastRetention,
            @Nullable String repositoryId,
            @Nullable Whiteboard whiteboard,
            @Nullable StatisticsProvider statisticsProvider)
            throws IOException {
        this.executor = executor;
        this.blobStore = blobStore;
        this.checkConsistencyAfterGc = checkConsistencyAfterGc;
        this.sweepIfRefsPastRetention = sweepIfRefsPastRetention;
        requireNonNull(blobStore, "BlobStore cannot be null");
        this.marker = marker;
        this.batchCount = batchCount;
        this.maxLastModifiedInterval = maxLastModifiedInterval;
        this.repoId = repositoryId;
        this.root = root;
        this.whiteboard = whiteboard;
        if (whiteboard != null) {
            this.checkpointMbean = WhiteboardUtils.getService(whiteboard, CheckpointMBean.class);
        }

        // re-initialize the statsProvider if passed as parameter
        if (statisticsProvider == null) {
            statisticsProvider = StatisticsProvider.NOOP;
        }
        this.stats = new GarbageCollectionOperationStats(statisticsProvider);
        this.statsCollector = stats.getCollector();
        this.consistencyStats =
            new GarbageCollectionOperationStats(statisticsProvider, GarbageCollectionOperationStats.CONSISTENCY_NAME);
        this.consistencyStatsCollector = consistencyStats.getCollector();
        this.clock = Clock.SIMPLE;
    }

    public MarkSweepGarbageCollector(
            BlobReferenceRetriever marker,
            GarbageCollectableBlobStore blobStore,
            Executor executor,
            String root,
            int batchCount,
            long maxLastModifiedInterval,
            @Nullable String repositoryId)
            throws IOException {
        this(marker, blobStore, executor, root, batchCount, maxLastModifiedInterval, false, false, repositoryId, null, null);
    }

    /**
     * Instantiates a new blob garbage collector.
     */
    public MarkSweepGarbageCollector(
            BlobReferenceRetriever marker,
            GarbageCollectableBlobStore blobStore,
            Executor executor,
            long maxLastModifiedInterval,
            @Nullable String repositoryId,
            @Nullable Whiteboard whiteboard,
            @Nullable StatisticsProvider statisticsProvider)
            throws IOException {
        this(marker, blobStore, executor, TEMP_DIR, DEFAULT_BATCH_COUNT, maxLastModifiedInterval, false, false, repositoryId, whiteboard, statisticsProvider);
    }

    @Override
    public void collectGarbage(boolean markOnly) throws Exception {
        markAndSweep(markOnly, false);
    }

    @Override
    public void collectGarbage(boolean markOnly, boolean forceBlobRetrieve) throws Exception {
        markAndSweep(markOnly, forceBlobRetrieve);
    }

    /**
     * Returns the stats related to GC for all repos
     *
     * @return a list of GarbageCollectionRepoStats objects
     * @throws Exception
     */
    @Override
    public List<GarbageCollectionRepoStats> getStats() throws Exception {
        List<GarbageCollectionRepoStats> stats = new ArrayList<>();
        if (SharedDataStoreUtils.isShared(blobStore)) {
            // Get all the references available
            List<DataRecord> refFiles =
                ((SharedDataStore) blobStore).getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());
            ImmutableListMultimap<String, DataRecord> references =
                FluentIterable.from(refFiles).index(
                        input -> SharedStoreRecordType.REFERENCES.getIdFromName(input.getIdentifier().toString()));

            // Get all the markers available
            List<DataRecord> markerFiles =
                ((SharedDataStore) blobStore).getAllMetadataRecords(SharedStoreRecordType.MARKED_START_MARKER.getType());
            Map<String, DataRecord> markers = Maps.uniqueIndex(markerFiles,
                    input -> input.getIdentifier().toString().substring(SharedStoreRecordType.MARKED_START_MARKER.getType().length() + 1));

            // Get all the repositories registered
            List<DataRecord> repoFiles =
                ((SharedDataStore) blobStore).getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType());

            for (DataRecord repoRec : repoFiles) {
                String id = SharedStoreRecordType.REPOSITORY.getIdFromName(repoRec.getIdentifier().toString());

                GarbageCollectionRepoStats stat = new GarbageCollectionRepoStats();
                stats.add(stat);

                stat.setRepositoryId(id);
                if (id != null && id.equals(repoId)) {
                    stat.setLocal(true);
                }

                if (references.containsKey(id)) {
                    ImmutableList<DataRecord> refRecs = references.get(id);
                    for(DataRecord refRec : refRecs) {
                        String uniqueSessionId = refRec.getIdentifier().toString()
                            .substring(SharedStoreRecordType.REFERENCES.getType().length() + 1);

                        stat.setEndTime(refRec.getLastModified());
                        stat.setLength(refRec.getLength());

                        if (markers.containsKey(uniqueSessionId)) {
                            stat.setStartTime(markers.get(uniqueSessionId).getLastModified());
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
                }
            }
        }
        return stats;
    }

    @Override
    public OperationsStatsMBean getOperationStats() {
        return stats;
    }

    @Override
    public OperationsStatsMBean getConsistencyOperationStats() {
        return consistencyStats;
    }

    /**
     * Mark and sweep. Main entry method for GC.
     *
     * @param markOnly whether to mark only
     * @param forceBlobRetrieve force retrieve blob ids
     * @throws Exception the exception
     */
    protected void markAndSweep(boolean markOnly, boolean forceBlobRetrieve) throws Exception {
        statsCollector.start();
        boolean threw = true;
        GarbageCollectorFileState fs = new GarbageCollectorFileState(root);
        Stopwatch sw = Stopwatch.createStarted();

        try {
            LOG.info("Starting Blob garbage collection with markOnly [{}] for repositoryId [{}]", markOnly, repoId);

            long markStart = System.currentTimeMillis();
            long markFinish;
            try {
                mark(fs);
            } finally {
                markFinish = sw.elapsed(TimeUnit.MILLISECONDS);
                statsCollector.updateMarkDuration(markFinish, TimeUnit.MILLISECONDS);
                LOG.info("Blob garbage collection Mark completed in {} ({} ms).",
                    sw.toString(), sw.elapsed(TimeUnit.MILLISECONDS));
            }

            if (!markOnly) {
                long deleteCount;
                try {
                    deleteCount = sweep(fs, markStart, forceBlobRetrieve);

                    long maxTime = getMaxModifiedTime(markStart) > 0 ? getMaxModifiedTime(markStart) : markStart;
                    LOG.info("Blob garbage collection completed in {} ({} ms). Number of blobs deleted [{}] with max modification time of [{}]",
                        sw.toString(), sw.elapsed(TimeUnit.MILLISECONDS), deleteCount, timestampToString(maxTime));

                    threw = false;
                } catch (NotAllRepositoryMarkedException rm) {
                    statsCollector.finishFailure();
                } finally {
                    sw.stop();
                    statsCollector.updateSweepDuration(sw.elapsed(TimeUnit.MILLISECONDS) - markFinish, TimeUnit.MILLISECONDS);
                }
            }
        } catch (Exception e) {
            statsCollector.finishFailure();
            LOG.error("Blob garbage collection error", e);
            throw e;
        } finally {
            statsCollector.updateDuration(sw.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            if (!LOG.isTraceEnabled() && !traceOutput) {
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

        String uniqueSuffix = UUID.randomUUID().toString();

        // Create a time marker in the data store if applicable
        GarbageCollectionType.get(blobStore).addMarkedStartMarker(blobStore, repoId, uniqueSuffix);

        // Mark all used references
        iterateNodeTree(fs, false);
        
        // Get size
        getBlobReferencesSize(fs, stats);
        
        // Move the marked references file to the data store meta area if applicable
        GarbageCollectionType.get(blobStore).addMarked(blobStore, fs, repoId, uniqueSuffix);

        LOG.debug("Ending mark phase of the garbage collector");
    }

    private static void getBlobReferencesSize(GarbageCollectorFileState fs, GarbageCollectionOperationStats stats)
        throws IOException {
        try (LineIterator lineIterator = new LineIterator(new FileReader(fs.getMarkedRefs()))) {
            lineIterator.forEachRemaining(line -> {
                String id = line.split(DELIM)[0];
                long length = DataStoreBlobStore.BlobId.of(id).getLength();
                LOG.debug("Blob {} has size {}", id, length);

                stats.getCollector().updateNumBlobReferences(1);

                if (length != -1) {
                    stats.getCollector().updateBlobReferencesSize(length);
                }
            });
        }
        LOG.info("Blob references found : {} with size : {}", stats.getNumBlobReferences(),
            stats.getBlobReferencesSize());
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
                fs.getAvailableRefs(),
                transformer);
        int candidates = FileIOUtils.writeStrings(iter, fs.getGcCandidates(), true);
        LOG.debug("Found candidates - " + candidates);

        LOG.debug("Ending difference phase of the garbage collector");
    }

    /**
     * Sweep phase of gc candidate deletion.
     * <p>
     * Performs the following steps depending upon the type of the blob store refer
     * {@link org.apache.jackrabbit.oak.plugins.blob.SharedDataStore.Type}:
     *
     * <ul>
     *     <li>Shared</li>
     *     <li>
     *     <ul>
     *      <li> Merge all marked references (from the mark phase run independently) available in the data store meta
     *          store (from all configured independent repositories).
     *      <li> Retrieve all blob ids available.
     *      <li> Diffs the 2 sets above to retrieve list of blob ids not used.
     *      <li> Deletes only blobs created after
     *          (earliest time stamp of the marked references - #maxLastModifiedInterval) from the above set.
     *     </ul>
     *     </li>
     *
     *     <li>Default</li>
     *     <li>
     *     <ul>
     *      <li> Mark phase already run.
     *      <li> Retrieve all blob ids available.
     *      <li> Diffs the 2 sets above to retrieve list of blob ids not used.
     *      <li> Deletes only blobs created after
     *          (time stamp of the marked references - #maxLastModifiedInterval).
     *     </ul>
     *     </li>
     * </ul>
     *
     * @return the number of blobs deleted
     * @throws Exception the exception
     * @param fs the garbage collector file state
     * @param markStart the start time of mark to take as reference for deletion
     * @param forceBlobRetrieve
     */
    protected long sweep(GarbageCollectorFileState fs, long markStart, boolean forceBlobRetrieve) throws Exception {
        long earliestRefAvailTime;
        // Merge all the blob references available from all the reference files in the data store meta store
        // Only go ahead if merge succeeded
        earliestRefAvailTime =
          GarbageCollectionType.get(blobStore).mergeAllMarkedReferences(blobStore, fs, clock, maxLastModifiedInterval,
              sweepIfRefsPastRetention);
        LOG.debug("Earliest reference available for timestamp [{}]", earliestRefAvailTime);
        earliestRefAvailTime = (earliestRefAvailTime < markStart ? earliestRefAvailTime : markStart);

        // Find all blob references after iterating over the whole repository
        (new BlobIdRetriever(fs, forceBlobRetrieve)).call();

        // Calculate the references not used
        difference(fs);
        long count = 0;
        long deleted = 0;

        long maxModifiedTime = getMaxModifiedTime(earliestRefAvailTime);
        LOG.debug("Starting sweep phase of the garbage collector");
        LOG.debug("Sweeping blobs with modified time > than the configured max deleted time ({}). ",
                timestampToString(maxModifiedTime));

        BufferedWriter removesWriter = null;
        LineIterator iterator = null;
        long deletedSize = 0;
        int numDeletedSizeAvailable = 0;
        try {
            removesWriter = Files.newWriter(fs.getGarbage(), StandardCharsets.UTF_8);
            ArrayDeque<String> removesQueue = new ArrayDeque<String>();
            iterator =
                    FileUtils.lineIterator(fs.getGcCandidates(), StandardCharsets.UTF_8.name());

            Iterator<List<String>> partitions = Iterators.partition(iterator, getBatchCount());
            while (partitions.hasNext()) {
                List<String> ids = partitions.next();
                count += ids.size();
                deleted += BlobCollectionType.get(blobStore)
                    .sweepInternal(blobStore, ids, removesQueue, maxModifiedTime);
                saveBatchToFile(new ArrayList<>(removesQueue), removesWriter);

                for(String deletedId : removesQueue) {
                    // Estimate the size of the blob
                    long length = DataStoreBlobStore.BlobId.of(deletedId).getLength();
                    if (length != -1) {
                        deletedSize += length;
                        numDeletedSizeAvailable += 1;
                    }
                }
                removesQueue.clear();
            }
        } finally {
            LineIterator.closeQuietly(iterator);
            closeQuietly(removesWriter);
        }

        if (checkConsistencyAfterGc) {
            BlobCollectionType.get(blobStore).checkConsistencyAfterGC(blobStore, fs, consistencyStats);
        }
        BlobCollectionType.get(blobStore).handleRemoves(blobStore, fs.getGarbage(), fs.getMarkedRefs());

        if(count != deleted) {
            LOG.warn("Deleted only [{}] blobs entries from the [{}] candidates identified. This may happen if blob "
                         + "modified time is > "
                         + "than the max deleted time ({})", deleted, count,
                        timestampToString(maxModifiedTime));
        }

        if (deletedSize > 0) {
            LOG.info("Estimated size recovered for {} deleted blobs is {} ({} bytes)",
                numDeletedSizeAvailable,
                org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount(deletedSize), deletedSize);
        }

        statsCollector.updateNumCandidates(count);
        statsCollector.updateNumDeleted(deleted);
        statsCollector.updateTotalSizeDeleted(deletedSize);

        // Remove all the merged marked references
        GarbageCollectionType.get(blobStore).removeAllMarkedReferences(blobStore);
        LOG.debug("Ending sweep phase of the garbage collector");
        return deleted;
    }

    private int getBatchCount() {
        return batchCount;
    }

    /**
     * 3 possibilities
     *  - If maxLastModifiedInterval <= 0 then return 0 which is interpreted as current by delete call
     *      (For testing purposes only)
     *  - If oldest checkpoint creation date > 0 then reference time is the earliest of that and the parameter
     *      maxModificationReferenceTime
     *  - Else the parameter maxModificationReferenceTime is used as the reference time
     *
     * @param maxModificationReferenceTime typically the mark phase start time (could be 0 for tests)
     * @return max modified time of blobs to be considered for deletion
     */
    private long getMaxModifiedTime(long maxModificationReferenceTime) {
        if (maxLastModifiedInterval <= 0) {
            return 0;
        }

        long oldestCheckpoint = -1;
        if (checkpointMbean != null) {
            oldestCheckpoint = checkpointMbean.getOldestCheckpointCreationDate().getTime();
            LOG.debug("Oldest checkpoint data retrieved {} ", oldestCheckpoint);
        }
        LOG.debug("maxModificationReferenceTime {} ", maxModificationReferenceTime);

        maxModificationReferenceTime = maxModificationReferenceTime <= 0 ?
                                            System.currentTimeMillis() : maxModificationReferenceTime;
        long calculatedReferenceTime = (oldestCheckpoint <= 0 ? maxModificationReferenceTime :
                Math.min(maxModificationReferenceTime, oldestCheckpoint));
        LOG.debug("Calculated reference time {} ", calculatedReferenceTime);

        return (calculatedReferenceTime - maxLastModifiedInterval);
    }

    /**
     * Save batch to file.
     */
    static void saveBatchToFile(List<String> ids, BufferedWriter writer) throws IOException {
        for (String id : ids) {
            FileIOUtils.writeAsLine(writer, id, true);
        }
        writer.flush();
    }

    /**
     * Iterates the complete node tree and collect all blob references
     * @param fs the garbage collector file state
     * @param logPath whether to log path in the file or not
     */
    protected void iterateNodeTree(GarbageCollectorFileState fs, final boolean logPath) throws IOException {
        final BufferedWriter writer = Files.newWriter(fs.getMarkedRefs(), StandardCharsets.UTF_8);
        final AtomicInteger count = new AtomicInteger();
        try {
            marker.collectReferences(
                    new ReferenceCollector() {
                        private final boolean debugMode = LOG.isTraceEnabled();

                        @Override
                        public void addReference(String blobId, final String nodeId) {
                            if (debugMode) {
                                LOG.trace("BlobId : {}, NodeId : {}", blobId, nodeId);
                            }

                            try {
                                Iterator<String> idIter = blobStore.resolveChunks(blobId);
                                Iterator<List<String>> partitions = Iterators.partition(idIter, getBatchCount());
                                while (partitions.hasNext()) {
                                    List<String> idBatch = Lists.transform(partitions.next(), id -> {
                                            if (logPath && nodeId != null) {
                                                return id + DELIM + nodeId;
                                            } else {
                                                return id;
                                            }
                                        });
                                    if (debugMode) {
                                        LOG.trace("chunkIds : {}", idBatch);
                                    }
                                    count.getAndAdd(idBatch.size());
                                    saveBatchToFile(idBatch, writer);
                                }

                                if (count.get() > 0 && count.get() % getBatchCount() == 0) {
                                    LOG.info("Collected ({}) blob references", count.get());
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
            sort(fs.getMarkedRefs(),
                new Comparator<String>() {
                    @Override
                    public int compare(String s1, String s2) {
                        return s1.split(DELIM)[0].compareTo(s2.split(DELIM)[0]);
                    }
                });
        } finally {
            closeQuietly(writer);
        }
    }
    
    @Override
    public long checkConsistency(boolean markOnly) throws Exception {
        consistencyStatsCollector.start();
        Stopwatch sw = Stopwatch.createStarted();

        boolean threw = true;
        GarbageCollectorFileState fs = new GarbageCollectorFileState(root);
        long candidates = 0;

        try {
            LOG.info("Starting blob consistency check with markOnly = {}", markOnly);

            // Mark all used blob references
            // Create a time marker in the data store if applicable
            String uniqueSuffix = UUID.randomUUID().toString();
            GarbageCollectionType.get(blobStore).addMarkedStartMarker(blobStore, repoId, uniqueSuffix);
            iterateNodeTree(fs, true);
            // Move the marked references file to the data store meta area if applicable
            GarbageCollectionType.get(blobStore).addMarked(blobStore, fs, repoId, uniqueSuffix);
            consistencyStatsCollector.updateMarkDuration(sw.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);

            if (SharedDataStoreUtils.isShared(blobStore)) {
                // Retrieve all other marked present in the datastore
                List<DataRecord> refFiles =
                        ((SharedDataStore) blobStore).getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());

                // Get all the repositories registered
                List<DataRecord> repoFiles =
                        ((SharedDataStore) blobStore).getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType());
                LOG.info("Repositories registered {}", repoFiles);

                // Retrieve repos for which reference files have not been created
                Set<String> unAvailRepos =
                        SharedDataStoreUtils.refsNotAvailableFromRepos(repoFiles, refFiles);
                LOG.info("Repositories with unavailable references {}", unAvailRepos);

                if (!unAvailRepos.isEmpty()) {
                    throw new NotAllRepositoryMarkedException("Not all repositories have marked references available");
                }

                if (refFiles.size() > 0) {
                    File temp = new File(root, repoId + UUID.randomUUID().toString());
                    copyFile(fs.getMarkedRefs(), temp);

                    // List of files to be merged
                    List<File> files = new ArrayList<>();
                    files.add(temp);
                    for (DataRecord refFile : refFiles) {
                        File file = copy(refFile.getStream());
                        files.add(file);
                    }
                    merge(files, fs.getMarkedRefs(), comparing(s -> s.split(DELIM)[0]));
                }
            }

            // Get size
            getBlobReferencesSize(fs, consistencyStats);
            
            if (!markOnly) {
                // Find all blobs available in the blob store
                ListenableFutureTask<Integer> blobIdRetriever = ListenableFutureTask.create(new BlobIdRetriever(fs,
                        true));
                executor.execute(blobIdRetriever);

                try {
                    blobIdRetriever.get();
                } catch (ExecutionException e) {
                    LOG.warn("Error occurred while fetching all the blobIds from the BlobStore");
                    threw = false;
                    throw e;
                }

                LOG.trace("Starting difference phase of the consistency check");
                FileLineDifferenceIterator iter = new FileLineDifferenceIterator(
                        fs.getAvailableRefs(),
                        fs.getMarkedRefs(),
                        transformer);
                // If tracking then also filter ids being tracked which are active deletions for lucene
                candidates = BlobCollectionType.get(blobStore).filter(blobStore, iter, fs);

                GarbageCollectionType.get(blobStore).removeAllMarkedReferences(blobStore);

                LOG.trace("Ending difference phase of the consistency check");
                LOG.info("Consistency check found [{}] missing blobs", candidates);

                if (candidates > 0) {
                    try (LineIterator lineIterator = new LineIterator(new FileReader(fs.getGcCandidates()))) {
                        while (lineIterator.hasNext()) {
                            LOG.warn("Missing Blob [{}]", lineIterator.nextLine());
                        }
                    }
                    LOG.warn("Consistency check failure in the the blob store : {}, check missing candidates in file {}",
                            blobStore, fs.getGcCandidates().getAbsolutePath());
                    consistencyStatsCollector.finishFailure();
                    consistencyStatsCollector.updateNumDeleted(candidates);
                }
            }
        } finally {
            if (!traceOutput && (!LOG.isTraceEnabled() && candidates == 0)) {
                Closeables.close(fs, threw);
            }
            sw.stop();
            consistencyStatsCollector.updateDuration(sw.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        }
        return candidates;
    }

    /**
     * Checks for the DataStore consistency and reports the number of missing blobs still referenced.
     *
     * @return the missing blobs
     * @throws Exception
     */
    @Override
    public long checkConsistency() throws Exception {
        return checkConsistency(false);
    }

    public void setTraceOutput(boolean trace) {
        traceOutput = trace;
    }

    public void setClock(Clock clock) {
        this.clock = clock;
    }

    /**
     * BlobIdRetriever class to retrieve all blob ids.
     */
    private class BlobIdRetriever implements Callable<Integer> {
        private final GarbageCollectorFileState fs;
        private final boolean forceRetrieve;

        public BlobIdRetriever(GarbageCollectorFileState fs, boolean forceBlobRetrieve) {
            this.fs = fs;
            this.forceRetrieve = forceBlobRetrieve;
        }

        @Override
        public Integer call() throws Exception {
            if (!forceRetrieve) {
                BlobCollectionType.get(blobStore).retrieve(blobStore, fs, getBatchCount());
                LOG.info("Length of blob ids file retrieved from tracker {}", fs.getAvailableRefs().length());
            }

            // If the length is 0 then references not available from the tracker
            // retrieve from the data store
            if (fs.getAvailableRefs().length() <= 0) {
                BlobCollectionType.DEFAULT.retrieve(blobStore, fs, getBatchCount());
                LOG.info("Length of blob ids file retrieved {}", fs.getAvailableRefs().length());

                BlobCollectionType.get(blobStore).track(blobStore, fs);
            }
            return 0;
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
             * @param maxLastModifiedInterval
             * @param sweepIfRefsPastRetention
             * @return the long the earliest time of the available references
             * @throws IOException Signals that an I/O exception has occurred.
             * @throws DataStoreException the data store exception
             */
            @Override
            long mergeAllMarkedReferences(GarbageCollectableBlobStore blobStore, GarbageCollectorFileState fs,
                Clock clock, long maxLastModifiedInterval, boolean sweepIfRefsPastRetention)
                    throws IOException, DataStoreException {

                List<DataRecord> refFiles =
                    ((SharedDataStore) blobStore).getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());
                LOG.info("References available {}", refFiles);

                // Get all the repositories registered
                List<DataRecord> repoFiles =
                    ((SharedDataStore) blobStore).getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType());
                LOG.info("Repositories registered {}", repoFiles);

                // Retrieve repos for which reference files have not been created
                Set<String> unAvailRepos =
                        SharedDataStoreUtils.refsNotAvailableFromRepos(repoFiles, refFiles);
                LOG.info("Repositories with unavailable references {}", unAvailRepos);

                Set<String> notOldRefs = Collections.EMPTY_SET;
                long retentionTime = clock.getTime() - maxLastModifiedInterval;
                LOG.info("Retention time calculated [{}]", retentionTime);

                if (sweepIfRefsPastRetention) {
                    notOldRefs =
                        SharedDataStoreUtils.refsNotOld(repoFiles, refFiles, retentionTime);
                    LOG.info("Repositories not having older references than retention time {}", notOldRefs);
                }

                if (unAvailRepos.isEmpty() && notOldRefs.isEmpty()) {
                    // List of files to be merged
                    List<File> files = new ArrayList<>();
                    for (DataRecord refFile : refFiles) {
                        File file = copy(refFile.getStream());
                        files.add(file);
                    }

                    merge(files, fs.getMarkedRefs());

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
                    LOG.error("Not all repositories have marked references available : {} or older than retention time: {}",
                        unAvailRepos, notOldRefs);
                    throw new NotAllRepositoryMarkedException("Not all repositories have marked references available");
                }
            }

            /**
             * Adds the marked references to the blob store root. Default NOOP
             *
             * @param blobStore the blob store
             * @param fs the fs
             * @param repoId the repo id
             * @param uniqueSuffix the unique session suffix
             * @throws DataStoreException the data store exception
             * @throws IOException Signals that an I/O exception has occurred.
             */
            @Override
            void addMarked(GarbageCollectableBlobStore blobStore, GarbageCollectorFileState fs, String repoId,
                String uniqueSuffix) throws DataStoreException, IOException {
                boolean exists = ((SharedDataStore) blobStore)
                    .metadataRecordExists(SharedStoreRecordType.REFERENCES.getNameFromId(repoId));
                if (exists) {
                    LOG.info("References for repository id {} already exists. Creating a duplicate one. "
                        + "Please check for inadvertent sharing of repository id by different repositories", repoId);
                }

                ((SharedDataStore) blobStore).addMetadataRecord(fs.getMarkedRefs(), SharedStoreRecordType.REFERENCES
                    .getNameFromIdPrefix(repoId, uniqueSuffix));
            }

            @Override
            public void addMarkedStartMarker(GarbageCollectableBlobStore blobStore, String repoId,
                String uniqueSuffix) {
                try {
                    ((SharedDataStore) blobStore).addMetadataRecord(new ByteArrayInputStream(new byte[0]),
                        SharedStoreRecordType.MARKED_START_MARKER
                            .getNameFromIdPrefix(repoId, uniqueSuffix));
                } catch (DataStoreException e) {
                    LOG.debug("Error creating marked time marker for repo : {}", repoId);
                }
            }
        },
        DEFAULT;

        void removeAllMarkedReferences(GarbageCollectableBlobStore blobStore) {}

        void addMarked(GarbageCollectableBlobStore blobStore, GarbageCollectorFileState fs, String repoId,
            String uniqueSuffix) throws DataStoreException, IOException {}

        long mergeAllMarkedReferences(GarbageCollectableBlobStore blobStore, GarbageCollectorFileState fs,
            Clock clock, long maxLastModifiedInterval, boolean sweepIfRefsPastRetention)
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

        public void addMarkedStartMarker(GarbageCollectableBlobStore blobStore, String repoId, String uniqueSuffix) {}
    }

    /**
     * Defines different blob collection types and encodes the divergent behavior.
     * <ul></ul>
     */
    private enum BlobCollectionType {
        TRACKER {
            @Override
            void retrieve(GarbageCollectableBlobStore blobStore,
                    GarbageCollectorFileState fs, int batchCount) throws Exception {
                ((BlobTrackingStore) blobStore).getTracker()
                    .get(fs.getAvailableRefs().getAbsolutePath());
            }

            @Override
            void handleRemoves(GarbageCollectableBlobStore blobStore, File removedIds, File markedRefs) throws IOException {
                BlobTrackingStore store = (BlobTrackingStore) blobStore;
                BlobIdTracker tracker = (BlobIdTracker) store.getTracker();
                tracker.remove(removedIds);
                tracker.getDeleteTracker().reconcile(markedRefs);
            }

            @Override
            void track(GarbageCollectableBlobStore blobStore,
                GarbageCollectorFileState fs) {
                try {
                    File f = File.createTempFile("blobiddownload", null);
                    copyFile(fs.getAvailableRefs(), f);
                    ((BlobTrackingStore) blobStore).getTracker().add(f);
                } catch (IOException e) {
                    LOG.warn("Unable to track blob ids locally");
                }
            }

            @Override
            public int filter(GarbageCollectableBlobStore blobStore, FileLineDifferenceIterator iter,
                GarbageCollectorFileState fs) throws IOException {
                // Write the original candidates
                FileIOUtils.writeStrings(iter, fs.getGcCandidates(), true);

                // Filter the ids actively deleted
                BlobTrackingStore store = (BlobTrackingStore) blobStore;
                BlobIdTracker tracker = (BlobIdTracker) store.getTracker();

                // Move the candidates identified to a temp file
                File candTemp = createTempFile("candTemp", null);
                copyFile(fs.getGcCandidates(), candTemp);

                Iterator<String> filter = tracker.getDeleteTracker().filter(candTemp);
                try {
                    return FileIOUtils.writeStrings(filter, fs.getGcCandidates(), true);
                } finally {
                    if (filter != null && filter instanceof FileLineDifferenceIterator) {
                        ((FileLineDifferenceIterator) filter).close();
                    }

                    if (candTemp != null) {
                        candTemp.delete();
                    }
                }
            }
        },
        DEFAULT;

        /**
         * Deletes the given batch by deleting individually to exactly know the actual deletes.
         */
        long sweepInternal(GarbageCollectableBlobStore blobStore, List<String> ids,
            ArrayDeque<String> exceptionQueue, long maxModified) {
            long totalDeleted = 0;
            LOG.trace("Blob ids to be deleted {}", ids);
            for (String id : ids) {
                try {
                    long deleted = blobStore.countDeleteChunks(new ArrayList<>(Arrays.asList(id)), maxModified);
                    if (deleted != 1) {
                        LOG.debug("Blob [{}] not deleted", id);
                    } else {
                        exceptionQueue.add(id);
                        totalDeleted += 1;
                    }
                } catch (Exception e) {
                    LOG.warn("Error occurred while deleting blob with id [{}]", id, e);
                }
            }
            return totalDeleted;
        }

        /**
         * Retrieve the put the list of available blobs in the file.
         *
         * @param blobStore
         * @param fs
         * @param batchCount
         * @throws Exception
         */
        void retrieve(GarbageCollectableBlobStore blobStore,
                GarbageCollectorFileState fs, int batchCount) throws Exception {
            LOG.debug("Starting retrieve of all blobs");
            int blobsCount = 0;
            Iterator<String> idsIter = null;
            try {
                idsIter = blobStore.getAllChunkIds(0);
                blobsCount = FileIOUtils.writeStrings(idsIter, fs.getAvailableRefs(), true, LOG, "Retrieved blobs - ");

                // sort the file
                sort(fs.getAvailableRefs());
                LOG.info("Number of blobs present in BlobStore : [{}] ", blobsCount);
            } finally {
                if (idsIter instanceof Closeable) {
                    try {
                        Closeables.close((Closeable) idsIter, false);
                    } catch (Exception e) {
                        LOG.debug("Error closing iterator");
                    }
                }
            }
        }

        /**
         * Hook to handle all the removed ids.
         *
         * @param blobStore
         * @param removedIds
         * @param markedRefs
         * @throws IOException
         */
        void handleRemoves(GarbageCollectableBlobStore blobStore, File removedIds, File markedRefs) throws IOException {
            FileUtils.forceDelete(removedIds);
        }

        void checkConsistencyAfterGC(GarbageCollectableBlobStore blobStore, GarbageCollectorFileState fs,
            GarbageCollectionOperationStats stats) throws IOException {
            stats.getCollector().start();
            Stopwatch sw = Stopwatch.createStarted();

            try {
                // Remove and spool the remaining ids into a temp file
                File availAfterGC = new File(fs.getAvailableRefs().getParent(), "availAfterGC");
                FileLineDifferenceIterator iterator = null;
                try {
                    iterator = new FileLineDifferenceIterator(fs.getGarbage(), fs.getAvailableRefs(), null);
                    writeStrings(iterator, availAfterGC, false);
                } finally {
                    if (iterator != null) {
                        iterator.close();
                    }
                }

                LOG.trace("Starting difference phase of the consistency check");
                FileLineDifferenceIterator iter =
                    new FileLineDifferenceIterator(availAfterGC, fs.getMarkedRefs(), transformer);
                File consistencyCandidatesAfterGC = new File(fs.getGcCandidates().getParent(), "consistencyCandidatesAfterGC");
                // Write the original candidates
                int candidates = FileIOUtils.writeStrings(iter, consistencyCandidatesAfterGC, true);

                LOG.trace("Ending difference phase of the consistency check");
                LOG.warn("Consistency check found [{}] missing blobs", candidates);

                if (candidates > 0) {
                    try (LineIterator lineIterator = new LineIterator(new FileReader(consistencyCandidatesAfterGC))) {
                        while (lineIterator.hasNext()) {
                            LOG.warn("Missing Blob [{}]", lineIterator.nextLine());
                        }
                    }
                    LOG.warn(
                        "Consistency check failure in the the blob store after GC : {}", blobStore);
                    stats.getCollector().finishFailure();
                    stats.getCollector().updateNumDeleted(candidates);
                }
                
                // Update the size of the referenced blobs
                getBlobReferencesSize(fs, stats);
            } finally {
                sw.stop();
                stats.getCollector().updateDuration(sw.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
            }
        }

        /**
         * Tracker may want to track this file
         *
         * @param blobStore
         * @param fs
         */
        void track(GarbageCollectableBlobStore blobStore, GarbageCollectorFileState fs) {
        }

        public static BlobCollectionType get(GarbageCollectableBlobStore blobStore) {
            if (blobStore instanceof BlobTrackingStore) {
                BlobTracker tracker = ((BlobTrackingStore) blobStore).getTracker();
                if (tracker != null) {
                    return TRACKER;
                }
            }
            return DEFAULT;
        }

        public int filter(GarbageCollectableBlobStore blobStore, FileLineDifferenceIterator iter,
            GarbageCollectorFileState fs) throws IOException {
            return FileIOUtils.writeStrings(iter, fs.getGcCandidates(), true);
        }
    }

    class GarbageCollectionOperationStats implements OperationsStatsMBean {
        static final String NAME = "DataStoreGarbageCollection";
        static final String CONSISTENCY_NAME = "DataStoreConsistencyCheck";
        static final String START = "COUNTER";
        static final String FINISH_FAILURE = "FAILURE";
        static final String DURATION = "ACTIVE_TIMER";
        static final String MARK_DURATION = "MARK_TIMER";
        static final String SWEEP_DURATION = "SWEEP_TIMER";
        static final String NUM_BLOBS_DELETED = "NUM_BLOBS_DELETED";
        static final String TOTAL_SIZE_DELETED = "TOTAL_SIZE_DELETED";
        static final String NUM_CANDIDATES = "NUM_CANDIDATES";
        
        static final String NUM_BLOB_REFERENCES = "NUM_BLOB_REFERENCES";

        static final String BLOB_REFERENCES_SIZE = "BLOB_REFERENCES_SIZE";

        private final String typeName;

        private CounterStats startCounter;
        private CounterStats finishFailureCounter;
        private CounterStats numDeletedCounter;
        private CounterStats totalSizeDeletedCounter;
        private CounterStats numCandidatesCounter;
        private CounterStats numBlobReferencesCounter;
        private CounterStats blobReferencesSizeCounter;
        private TimerStats duration;
        private final TimerStats markDuration;
        private final TimerStats sweepDuration;
        private final OperationStatsCollector collector;

        GarbageCollectionOperationStats(StatisticsProvider sp, String typeName) {
            this.typeName = typeName;

            this.startCounter = sp.getCounterStats(getMetricName(START), StatsOptions.METRICS_ONLY);
            this.finishFailureCounter = sp.getCounterStats(getMetricName(FINISH_FAILURE), StatsOptions.METRICS_ONLY);
            this.numDeletedCounter = sp.getCounterStats(getMetricName(NUM_BLOBS_DELETED), StatsOptions.METRICS_ONLY);
            this.totalSizeDeletedCounter = sp.getCounterStats(getMetricName(TOTAL_SIZE_DELETED), StatsOptions.METRICS_ONLY);
            this.numCandidatesCounter = sp.getCounterStats(getMetricName(NUM_CANDIDATES), StatsOptions.METRICS_ONLY);
            this.numBlobReferencesCounter = sp.getCounterStats(getMetricName(NUM_BLOB_REFERENCES), StatsOptions.METRICS_ONLY);
            this.blobReferencesSizeCounter = sp.getCounterStats(getMetricName(BLOB_REFERENCES_SIZE), StatsOptions.METRICS_ONLY);
            this.duration = sp.getTimer(getMetricName(DURATION), StatsOptions.METRICS_ONLY);
            this.markDuration = sp.getTimer(getMetricName(MARK_DURATION), StatsOptions.METRICS_ONLY);
            this.sweepDuration = sp.getTimer(getMetricName(SWEEP_DURATION), StatsOptions.METRICS_ONLY);

            this.collector = new OperationStatsCollector() {
                @Override
                public void start() {
                    startCounter.inc();
                }

                @Override
                public void finishFailure() {
                    finishFailureCounter.inc();
                }

                @Override
                public void updateNumDeleted(long num) {
                    numDeletedCounter.inc(num);
                }

                @Override
                public void updateNumCandidates(long num) {
                    numCandidatesCounter.inc(num);
                }

                @Override
                public void updateTotalSizeDeleted(long size) {
                    totalSizeDeletedCounter.inc(size);
                }

                @Override
                public void updateNumBlobReferences(long num) {
                    numBlobReferencesCounter.inc(num);
                }

                @Override
                public void updateBlobReferencesSize(long size) {
                    blobReferencesSizeCounter.inc(size);
                }
                
                @Override
                public void updateDuration(long time, TimeUnit timeUnit) {
                    duration.update(time, timeUnit);
                }

                @Override public void updateMarkDuration(long time, TimeUnit timeUnit) {
                    markDuration.update(time, timeUnit);
                }

                @Override public void updateSweepDuration(long time, TimeUnit timeUnit) {
                    sweepDuration.update(time, timeUnit);
                }
            };
        }


        GarbageCollectionOperationStats(StatisticsProvider sp) {
            this(sp, NAME);
        }

        private String getMetricName(String name) {
            return getName() + "." + name;
        }

        protected OperationStatsCollector getCollector() {
            return collector;
        }

        @Override public String getName() {
            return TYPE + "." + typeName;
        }

        @Override public long getStartCount() {
            return startCounter.getCount();
        }

        @Override public long getFailureCount() {
            return finishFailureCounter.getCount();
        }

        @Override public long duration() {
            return duration.getCount();
        }

        @Override public long markDuration() {
            return markDuration.getCount();
        }

        @Override public long numDeleted() {
            return numDeletedCounter.getCount();
        }

        @Override public long sizeDeleted() {
            return totalSizeDeletedCounter.getCount();
        }

        @Override public long getNumBlobReferences() {
            return numBlobReferencesCounter.getCount();
        }

        @Override public long getBlobReferencesSize() {
            return blobReferencesSizeCounter.getCount();
        }
    }


    /**
     * Marker IOException to identify sweep phase failure because of some
     * repositories not having finished Mark phase.
     */
    static class NotAllRepositoryMarkedException extends IOException {
        public NotAllRepositoryMarkedException(String message) {
            super(message);
       }
   }
}
