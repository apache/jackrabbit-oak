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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedSortBatchTask.Result;
import org.apache.jackrabbit.oak.index.indexer.document.tree.TreeStore;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.TreeSession;
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

    private static final int MERGE_BATCH = 25;
    private static final boolean SKIP_FINAL_MERGE = false;

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
        INDEXING_PHASE_LOGGER.info("[TASK:{}:START] Starting sort-and-save task", THREAD_NAME.toUpperCase(Locale.ROOT));
        try {
            while (true) {
                NodeStateEntryBatch nseBuffer = nonEmptyBuffersQueue.take();
                if (nseBuffer == SENTINEL_NSE_BUFFER) {
                    synchronized (treeStore) {
                        TreeSession session = treeStore.getSession();
                        Stopwatch start = Stopwatch.createStarted();
                        while (session.getRootCount() > MERGE_BATCH) {
                            LOG.info("Merging {} roots; there are {} roots",
                                    MERGE_BATCH, session.getRootCount());
                            session.mergeRoots(MERGE_BATCH);
                            session.runGC();
                        }
                        if (SKIP_FINAL_MERGE) {
                            LOG.info("Final merge is skipped");
                        } else {
                            LOG.info("Final merge; {} roots", session.getRootCount());
                            session.mergeRoots(Integer.MAX_VALUE);
                            session.runGC();
                        }
                        long durationSeconds = start.elapsed(TimeUnit.SECONDS);
                        MetricsUtils.addMetric(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_DURATION_SECONDS, durationSeconds);
                        MetricsUtils.addMetric(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_INTERMEDIATE_FILES_TOTAL, 0);
                        MetricsUtils.addMetric(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_EAGER_MERGES_RUNS_TOTAL, 0);
                        MetricsUtils.addMetric(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_FILES_COUNT_TOTAL, 0);
                        MetricsUtils.addMetricByteSize(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_FLAT_FILE_STORE_SIZE_BYTES, 0);
                        LOG.info("Final merge done, {} roots", session.getRootCount());
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

    private ArrayList<SortKeyPath> buildSortArray(NodeStateEntryBatch nseb) {
        Stopwatch startTime = Stopwatch.createStarted();
        ByteBuffer buffer = nseb.getBuffer();
        int totalPathSize = 0;
        ArrayList<SortKeyPath> sortBuffer = new ArrayList<>(nseb.numberOfEntries());
        while (buffer.hasRemaining()) {
            // Read the next key from the buffer
            int pathLength = buffer.getInt();
            totalPathSize += pathLength;
            // Create the String directly from the buffer without creating an intermediate byte[]
            String path = new String(buffer.array(), buffer.arrayOffset() + buffer.position(), pathLength, StandardCharsets.UTF_8);
            buffer.position(buffer.position() + pathLength);
            int valuePosition = buffer.position();
            // Skip the json
            int entryLength = buffer.getInt();
            buffer.position(buffer.position() + entryLength);
            SortKeyPath entry = new SortKeyPath(path, valuePosition);
            sortBuffer.add(entry);
        }
        timeCreatingSortArrayMillis += startTime.elapsed().toMillis();
        LOG.info("Built sort array in {}. Entries: {}, Total size of path strings: {}",
                startTime, sortBuffer.size(), humanReadableByteCountBin(totalPathSize));
        return sortBuffer;
    }

    private void sortAndSaveBatch(NodeStateEntryBatch nseb) throws Exception {
        ByteBuffer buffer = nseb.getBuffer();
        LOG.info("Going to sort batch in memory. Entries: {}, Size: {}",
                nseb.numberOfEntries(), humanReadableByteCountBin(nseb.sizeOfEntriesBytes()));
        ArrayList<SortKeyPath> sortBuffer = buildSortArray(nseb);
        if (sortBuffer.isEmpty()) {
            return;
        }
        Stopwatch sortClock = Stopwatch.createStarted();
        Collections.sort(sortBuffer);
        timeSortingMillis += sortClock.elapsed().toMillis();
        LOG.info("Sorted batch in {}. Saving.", sortClock);
        Stopwatch saveClock = Stopwatch.createStarted();
        long textSize = 0;
        batchesProcessed++;
        synchronized (treeStore) {
            TreeSession session = treeStore.getSession();
            for (SortKeyPath entry : sortBuffer) {
                entriesProcessed++;
                // Retrieve the entry from the buffer
                int posInBuffer = entry.getBufferPos();
                buffer.position(posInBuffer);
                int valueLength = buffer.getInt();
                String value = new String(buffer.array(), buffer.arrayOffset() + buffer.position(), valueLength, StandardCharsets.UTF_8);
                textSize += entry.getPath().length() + value.length() + 2;
                treeStore.putNode(entry.getPath(), value);
            }
            session.checkpoint();
            unmergedRoots++;
            LOG.info("Root count is {}, we have {} small unmerged roots",
                    session.getRootCount(), unmergedRoots);
            if (unmergedRoots == MERGE_BATCH) {
                session.mergeRoots(MERGE_BATCH);
                session.runGC();
                LOG.info("Merged {} roots, root count is now {}",
                        unmergedRoots, session.getRootCount());
                unmergedRoots = 0;
            }
            timeWritingMillis += saveClock.elapsed().toMillis();
            batchesProcessed++;
            LOG.info("Wrote batch of size {} (uncompressed) in {} at {}",
                    humanReadableByteCountBin(textSize),
                    saveClock,
                    PipelinedUtils.formatAsTransferSpeedMBs(textSize, saveClock.elapsed().toMillis())
            );
        }
    }

}
