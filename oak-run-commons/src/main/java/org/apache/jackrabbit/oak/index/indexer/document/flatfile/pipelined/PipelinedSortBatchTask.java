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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.apache.jackrabbit.oak.plugins.index.MetricsFormatter;
import org.apache.jackrabbit.oak.plugins.index.IndexingReporter;
import org.apache.jackrabbit.oak.plugins.index.MetricsUtils;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCountBin;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_NSE_BUFFER;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedUtils.formatAsPercentage;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.INDEXING_PHASE_LOGGER;

/**
 * Receives batches of node state entries, sorts then in memory, and finally writes them to a file.
 */
class PipelinedSortBatchTask implements Callable<PipelinedSortBatchTask.Result> {
    public static class Result {
        private final long totalEntries;

        public Result(long totalEntries) {
            this.totalEntries = totalEntries;
        }

        public long getTotalEntries() {
            return totalEntries;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedSortBatchTask.class);

    private static final String THREAD_NAME = "mongo-sort-batch";

    private final Comparator<SortKey> pathComparator;
    private final Compression algorithm;
    private final BlockingQueue<NodeStateEntryBatch> emptyBuffersQueue;
    private final BlockingQueue<NodeStateEntryBatch> nonEmptyBuffersQueue;
    private final BlockingQueue<Path> sortedFilesQueue;
    private final Path sortWorkDir;
    private final StatisticsProvider statisticsProvider;
    private final IndexingReporter reporter;
    private final ArrayList<SortKey> sortBuffer = new ArrayList<>(32 * 1024);
    private long entriesProcessed = 0;
    private long batchesProcessed = 0;
    private long timeCreatingSortArrayMillis = 0;
    private long timeSortingMillis = 0;
    private long timeWritingMillis = 0;

    public PipelinedSortBatchTask(Path storeDir,
                                  PathElementComparator pathComparator,
                                  Compression algorithm,
                                  BlockingQueue<NodeStateEntryBatch> emptyBuffersQueue,
                                  BlockingQueue<NodeStateEntryBatch> nonEmptyBuffersQueue,
                                  BlockingQueue<Path> sortedFilesQueue,
                                  StatisticsProvider statisticsProvider,
                                  IndexingReporter reporter) throws IOException {
        this.pathComparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.algorithm = algorithm;
        this.emptyBuffersQueue = emptyBuffersQueue;
        this.nonEmptyBuffersQueue = nonEmptyBuffersQueue;
        this.sortedFilesQueue = sortedFilesQueue;
        this.sortWorkDir = createdSortWorkDir(storeDir);
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
                LOG.info("Waiting for next batch");
                NodeStateEntryBatch nseBuffer = nonEmptyBuffersQueue.take();
                if (nseBuffer == SENTINEL_NSE_BUFFER) {
                    long totalTimeMillis = taskStartTime.elapsed().toMillis();
                    sortBuffer.clear(); // It should be empty already
                    sortBuffer.trimToSize();  // Release the internal array which may be very large, several millions
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

    private void buildSortArray(NodeStateEntryBatch nseb) {
        Stopwatch startTime = Stopwatch.createStarted();
        ByteBuffer buffer = nseb.getBuffer();
        int totalPathSize = 0;
        while (buffer.hasRemaining()) {
            int positionInBuffer = buffer.position();
            // Read the next key from the buffer
            int pathLength = buffer.getInt();
            totalPathSize += pathLength;
            // Create the String directly from the buffer without creating an intermediate byte[]
            String path = new String(buffer.array(), buffer.arrayOffset() + buffer.position(), pathLength, StandardCharsets.UTF_8);
            buffer.position(buffer.position() + pathLength);
            // Skip the json
            int entryLength = buffer.getInt();
            buffer.position(buffer.position() + entryLength);
            String[] pathSegments = SortKey.genSortKeyPathElements(path);
            sortBuffer.add(new SortKey(pathSegments, positionInBuffer));
        }
        timeCreatingSortArrayMillis += startTime.elapsed().toMillis();
        LOG.info("Built sort array in {}. Entries: {}, Total size of path strings: {}",
                startTime, sortBuffer.size(), humanReadableByteCountBin(totalPathSize));
    }

    private void sortAndSaveBatch(NodeStateEntryBatch nseb) throws Exception {
        ByteBuffer buffer = nseb.getBuffer();
        LOG.info("Going to sort batch in memory. Entries: {}, Size: {}",
                nseb.numberOfEntries(), humanReadableByteCountBin(nseb.sizeOfEntriesBytes()));
        sortBuffer.clear();
        buildSortArray(nseb);
        if (sortBuffer.isEmpty()) {
            return;
        }
        Stopwatch sortClock = Stopwatch.createStarted();
        sortBuffer.sort(pathComparator);
        timeSortingMillis += sortClock.elapsed().toMillis();
        LOG.info("Sorted batch in {}. Saving to disk", sortClock);
        Stopwatch saveClock = Stopwatch.createStarted();
        Path newtmpfile = Files.createTempFile(sortWorkDir, "sortInBatch", "flatfile");
        long textSize = 0;
        batchesProcessed++;
        String[] prevPathElements = null;
        int duplicateEntriesSkippped = 0;
        try (OutputStream os = IndexStoreUtils.createOutputStream(newtmpfile, algorithm)) {
            for (SortKey entry : sortBuffer) {
                entriesProcessed++;
                if (Arrays.equals(prevPathElements, entry.getPathElements())) {
                    duplicateEntriesSkippped++;
                    continue;
                }
                prevPathElements = entry.getPathElements();
                // Retrieve the entry from the buffer
                int posInBuffer = entry.getBufferPos();
                buffer.position(posInBuffer);

                int pathSize = buffer.getInt();
                copyField(os, buffer, pathSize);
                os.write(PipelinedStrategy.FLATFILESTORE_DELIMITER);
                int jsonSize = buffer.getInt();
                copyField(os, buffer, jsonSize);
                os.write(PipelinedStrategy.FLATFILESTORE_LINE_SEPARATOR);
                textSize += pathSize + jsonSize + 2;
            }
        }
        timeWritingMillis += saveClock.elapsed().toMillis();
        long compressedSize = Files.size(newtmpfile);
        int entriesWritten = sortBuffer.size() - duplicateEntriesSkippped;
        LOG.info("Wrote batch of size {} (uncompressed {}) with {} entries in {} at {}. Duplicate entries skipped: {}",
                humanReadableByteCountBin(compressedSize),
                humanReadableByteCountBin(textSize),
                entriesWritten, saveClock,
                PipelinedUtils.formatAsTransferSpeedMBs(compressedSize, saveClock.elapsed().toMillis()),
                duplicateEntriesSkippped
        );
        // Free the memory taken by the entries in the buffer
        sortBuffer.clear();
        sortedFilesQueue.put(newtmpfile);
    }

    private void copyField(OutputStream writer, ByteBuffer buffer, int fieldSize) throws IOException {
        // Write the entry to the file without creating intermediate byte[]
        writer.write(buffer.array(), buffer.position(), fieldSize);
        buffer.position(buffer.position() + fieldSize);
    }

    private static Path createdSortWorkDir(Path storeDir) throws IOException {
        Path sortedFileDir = storeDir.resolve("sort-work-dir");
        FileUtils.forceMkdir(sortedFileDir.toFile());
        return sortedFileDir;
    }
}
