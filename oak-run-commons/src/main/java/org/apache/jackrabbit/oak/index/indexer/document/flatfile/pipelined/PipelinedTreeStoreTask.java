/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCountBin;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_NSE_BUFFER;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedUtils.formatAsPercentage;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.INDEXING_PHASE_LOGGER;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedSortBatchTask.Result;
import org.apache.jackrabbit.oak.index.indexer.document.tree.TreeStore;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Session;
import org.apache.jackrabbit.oak.plugins.index.IndexingReporter;
import org.apache.jackrabbit.oak.plugins.index.MetricsFormatter;
import org.apache.jackrabbit.oak.plugins.index.MetricsUtils;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receives batches of node state entries, sorts then in memory, and finally writes them to a tree store.
 */
public class PipelinedTreeStoreTask implements Callable<PipelinedSortBatchTask.Result> {

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedTreeStoreTask.class);
    private static final String THREAD_NAME = "tree-store-task";

    private static final int MERGE_BATCH = 5;

    private final TreeStore treeStore;
    private final BlockingQueue<NodeStateEntryBatch> emptyBuffersQueue;
    private final BlockingQueue<NodeStateEntryBatch> nonEmptyBuffersQueue;
    private final StatisticsProvider statisticsProvider;
    private final IndexingReporter reporter;

    private long entriesProcessed = 0;
    private long batchesProcessed = 0;
    private long timeCreatingSortArrayMillis = 0;
    private long timeSortingMillis = 0;
    private long timeWritingMillis = 0;
    private int unmergedRoots;

    public PipelinedTreeStoreTask(TreeStore treeStore,
            ArrayBlockingQueue<NodeStateEntryBatch> emptyBuffersQueue,
            ArrayBlockingQueue<NodeStateEntryBatch> nonEmptyBuffersQueue,
            StatisticsProvider statisticsProvider,
            IndexingReporter reporter) {
        this.treeStore = treeStore;
        this.emptyBuffersQueue = emptyBuffersQueue;
        this.nonEmptyBuffersQueue = nonEmptyBuffersQueue;
        this.statisticsProvider = statisticsProvider;
        this.reporter = reporter;

    }

    @Override
    public Result call() throws Exception {
        Stopwatch taskStartTime = Stopwatch.createStarted();
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName(THREAD_NAME);
        INDEXING_PHASE_LOGGER.info("[TASK:{}:START] Starting tree store task", THREAD_NAME.toUpperCase(Locale.ROOT));
        try {
            while (true) {
                LOG.info("Waiting for next batch");
                NodeStateEntryBatch nseBuffer = nonEmptyBuffersQueue.take();
                if (nseBuffer == SENTINEL_NSE_BUFFER) {
                    synchronized (treeStore) {
                        Session session = treeStore.getSession();
                        LOG.info("Final merge(s)");
                        Stopwatch start = Stopwatch.createStarted();
                        while (session.getRootCount() > MERGE_BATCH) {
                            LOG.info("Merging {} roots; we have {} roots",
                                    MERGE_BATCH, session.getRootCount());
                            session.mergeRoots(MERGE_BATCH);
                            session.runGC();
                        }
                        LOG.info("Final merge; we have {} roots", session.getRootCount());
                        session.mergeRoots(Integer.MAX_VALUE);
                        session.runGC();
                        long durationSeconds = start.elapsed(TimeUnit.SECONDS);
                        MetricsUtils.addMetric(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_DURATION_SECONDS, durationSeconds);
                        MetricsUtils.addMetric(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_INTERMEDIATE_FILES_TOTAL, 0);
                        MetricsUtils.addMetric(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_EAGER_MERGES_RUNS_TOTAL, 0);
                        MetricsUtils.addMetric(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_FILES_COUNT_TOTAL, 0);
                        MetricsUtils.addMetricByteSize(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_FLAT_FILE_STORE_SIZE_BYTES, 0);
                        LOG.info("Final merge done, new root count {}", session.getRootCount());
                    }
                    long totalTimeMillis = taskStartTime.elapsed().toMillis();
                    String timeCreatingSortArrayPercentage = formatAsPercentage(timeCreatingSortArrayMillis, totalTimeMillis);
                    String timeSortingPercentage = formatAsPercentage(timeSortingMillis, totalTimeMillis);
                    String timeWritingPercentage = formatAsPercentage(timeWritingMillis, totalTimeMillis);
                    String metrics = MetricsFormatter.newBuilder()
                            .add("batchesProcessed", batchesProcessed)
                            .add("entriesProcessed", entriesProcessed)
                            .add("timeCreatingSortArrayMillis", timeCreatingSortArrayMillis)
                            .add("timeCreatingSortArrayPercentage", timeCreatingSortArrayPercentage)
                            .add("timeSortingMillis", timeSortingMillis)
                            .add("timeSortingPercentage", timeSortingPercentage)
                            .add("timeWritingMillis", timeWritingMillis)
                            .add("timeWritingPercentage", timeWritingPercentage)
                            .add("totalTimeSeconds", totalTimeMillis / 1000)
                            .build();
                    INDEXING_PHASE_LOGGER.info("[TASK:{}:END] Metrics: {}", THREAD_NAME.toUpperCase(Locale.ROOT), metrics);
                    MetricsUtils.addMetric(statisticsProvider, reporter,
                            PipelinedMetrics.OAK_INDEXER_PIPELINED_SORT_BATCH_PHASE_CREATE_SORT_ARRAY_PERCENTAGE,
                            PipelinedUtils.toPercentage(timeCreatingSortArrayMillis, totalTimeMillis));
                    MetricsUtils.addMetric(statisticsProvider, reporter,
                            PipelinedMetrics.OAK_INDEXER_PIPELINED_SORT_BATCH_PHASE_SORT_ARRAY_PERCENTAGE,
                            PipelinedUtils.toPercentage(timeSortingMillis, totalTimeMillis));
                    MetricsUtils.addMetric(statisticsProvider, reporter,
                            PipelinedMetrics.OAK_INDEXER_PIPELINED_SORT_BATCH_PHASE_WRITE_TO_DISK_PERCENTAGE,
                            PipelinedUtils.toPercentage(timeWritingMillis, totalTimeMillis));
                    return new Result(entriesProcessed);
                }
                sortAndSaveBatch(nseBuffer);
                nseBuffer.reset();
                emptyBuffersQueue.put(nseBuffer);
            }
        } catch (Throwable t) {
            INDEXING_PHASE_LOGGER.info("[TASK:{}:FAIL] Metrics: {}, Error: {}",
                    THREAD_NAME.toUpperCase(Locale.ROOT),
                    MetricsFormatter.createMetricsWithDurationOnly(taskStartTime),
                    t.toString());
            LOG.warn("Thread terminating with exception", t);
            throw t;
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }

    private void sortAndSaveBatch(NodeStateEntryBatch nseb) throws Exception {
        LOG.info("Going to sort batch in memory. Entries: {}, Size: {}",
                nseb.numberOfEntries(), humanReadableByteCountBin(nseb.sizeOfEntriesBytes()));
        TreeMap<String, String> sortBuffer = buildTreeMap(nseb);
        if (sortBuffer.isEmpty()) {
            return;
        }
        Stopwatch sortClock = Stopwatch.createStarted();
        timeSortingMillis += sortClock.elapsed().toMillis();
        LOG.info("Sorted batch in {}. Saving to disk", sortClock);
        Stopwatch saveClock = Stopwatch.createStarted();
        long textSize = 0;
        batchesProcessed++;
        int sortBufferSize = sortBuffer.size();
        synchronized (treeStore) {
            Session session = treeStore.getSession();
            for (Entry<String, String> e : sortBuffer.entrySet()) {
                session.put(e.getKey(), e.getValue());
                textSize += e.getKey().length() + e.getValue().length() + 2;
                entriesProcessed++;
            }
            // free memory, before we merge, because merging needs memory as well
            sortBuffer = null;
            session.checkpoint();
            unmergedRoots++;
            LOG.info("Root count is {}, we have {} small unmerged roots",
                    session.getRootCount(), unmergedRoots);
            if (unmergedRoots == MERGE_BATCH) {
                LOG.info("Merging {} roots, as we have {} new ones; root count is {}",
                        MERGE_BATCH, unmergedRoots, session.getRootCount());
                session.mergeRoots(MERGE_BATCH);
                session.runGC();
                LOG.info("New root count is {}", session.getRootCount());
                unmergedRoots = 0;
            }
        }
        timeWritingMillis += saveClock.elapsed().toMillis();
        LOG.info("Wrote batch of size {} (uncompressed) with {} entries in {} at {}",
                humanReadableByteCountBin(textSize),
                sortBufferSize, saveClock,
                PipelinedUtils.formatAsTransferSpeedMBs(textSize, saveClock.elapsed().toMillis())
        );
    }

    private TreeMap<String, String> buildTreeMap(NodeStateEntryBatch nseb) {
        Stopwatch startTime = Stopwatch.createStarted();
        ByteBuffer buffer = nseb.getBuffer();
        int totalPathSize = 0;
        TreeMap<String, String> map = new TreeMap<>();
        while (buffer.hasRemaining()) {
            int pathLength = buffer.getInt();
            totalPathSize += pathLength;
            String path = new String(buffer.array(), buffer.position() + buffer.arrayOffset(), pathLength, StandardCharsets.UTF_8);
            buffer.position(buffer.position() + pathLength);
            int entryLength = buffer.getInt();
            String data = new String(buffer.array(), buffer.position() + buffer.arrayOffset(), entryLength, StandardCharsets.UTF_8);
            addEntry(path, data, map);
            buffer.position(buffer.position() + entryLength);
        }
        timeCreatingSortArrayMillis += startTime.elapsed().toMillis();
        LOG.info("Built tree map in {}. Entries: {}, Total size of path strings: {}", startTime, map.size(),
                humanReadableByteCountBin(totalPathSize));
        return map;
    }

    private static void addEntry(String path, String data, TreeMap<String, String> target) {
        target.put(path, data);
        if (!path.equals("/")) {
            String nodeName = PathUtils.getName(path);
            String parentPath = PathUtils.getParentPath(path);
            target.put(parentPath + "\t" + nodeName, "");
        }
    }
}
