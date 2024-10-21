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
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.commons.sort.ExternalSortByteArray;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.apache.jackrabbit.oak.plugins.index.MetricsFormatter;
import org.apache.jackrabbit.oak.plugins.index.IndexingReporter;
import org.apache.jackrabbit.oak.plugins.index.MetricsUtils;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_SORTED_FILES_QUEUE;
import static org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils.getSortedStoreFileName;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.INDEXING_PHASE_LOGGER;

/**
 * Accumulates the intermediate sorted files and, when all files are generated, merges them into a single sorted file,
 * the flat file store
 */
public class PipelinedMergeSortTask implements Callable<PipelinedMergeSortTask.Result> {
    /**
     * Minimum number of intermediate files that must exist before trying to do an eager merge
     */
    public static final String OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD = "oak.indexer.pipelined.eagerMergeTriggerThreshold";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD = 64;
    /*
     * Maximum number of files to eagerly merge. This is to keep the eager merges efficient, as efficiency decreases
     * with the number of files in a merge
     */
    public static final String OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE = "oak.indexer.pipelined.eagerMergeMaxFilesToMerge";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE = 32;
    /*
     * Minimum number of files to eagerly merge at a time.
     */
    public static final String OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE = "oak.indexer.pipelined.eagerMergeMinFilesToMerge";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE = 4;
    /*
     * Maximum total size of intermediate files that can be eagerly merged.
     */
    public static final String OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB = "oak.indexer.pipelined.eagerMergeMaxSizeToMergeMB";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB = 2048;

    public final static String OAK_INDEXER_PIPELINED_EXTERNAL_MERGE_READ_BUFFER_SIZE = "oak.indexer.pipelined.externalMerge.readBufferSize";
    public final static int DEFAULT_OAK_INDEXER_PIPELINED_EXTERNAL_MERGE_READ_BUFFER_SIZE = 16 * 1024;

    public static class Result {
        private final Path flatFileStoreFile;
        private final int intermediateFilesCount;
        private final int finalMergeFilesCount;
        private final int eagerMergeRuns;

        public Result(Path flatFileStoreFile, int intermediateFilesCount, int finalMergeFilesCount, int eagerMergeRuns) {
            this.flatFileStoreFile = flatFileStoreFile;
            this.intermediateFilesCount = intermediateFilesCount;
            this.finalMergeFilesCount = finalMergeFilesCount;
            this.eagerMergeRuns = eagerMergeRuns;
        }

        public Path getFlatFileStoreFile() {
            return flatFileStoreFile;
        }

        public int getFinalMergeFilesCount() {
            return finalMergeFilesCount;
        }

        public int getEagerMergeRuns() {
            return eagerMergeRuns;
        }

        public int getIntermediateFilesCount() {
            return intermediateFilesCount;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "flatFileStoreFile=" + flatFileStoreFile +
                    ", intermediateFilesCount=" + intermediateFilesCount +
                    ", finalMergeFilesCount=" + finalMergeFilesCount +
                    ", eagerMergeRuns=" + eagerMergeRuns +
                    '}';
        }
    }

    private static class PathAndSize implements Comparable<PathAndSize> {
        final Path file;
        final long size;

        public PathAndSize(Path file, long size) {
            this.file = file;
            this.size = size;
        }

        @Override
        public String toString() {
            return "FileAndSize{" +
                    "file=" + file.toString() +
                    ", size=" + IOUtils.humanReadableByteCountBin(size) +
                    '}';
        }

        @Override
        public int compareTo(@NotNull PipelinedMergeSortTask.PathAndSize other) {
            return Long.compare(this.size, other.size);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMergeSortTask.class);
    private static final String THREAD_NAME = "mongo-merge-sort-files";

    private final Path storeDir;
    private final Comparator<NodeStateHolder> comparator;
    private final Compression algorithm;
    private final BlockingQueue<Path> sortedFilesQueue;
    private final StatisticsProvider statisticsProvider;
    private final IndexingReporter reporter;
    private final PriorityQueue<PathAndSize> sortedFiles = new PriorityQueue<>();
    private final AtomicBoolean stopEagerMerging = new AtomicBoolean(false);
    private final int mergeTriggerThreshold;
    private final int minFilesToMerge;
    private final int maxFilesToMerge;
    private final int maxSizeToMergeMB;
    private final int externalMergeReadBufferSize;
    private int eagerMergeRuns;
    private int mergedFilesCounter = 0;

    public PipelinedMergeSortTask(Path storeDir,
                                  PathElementComparator pathComparator,
                                  Compression algorithm,
                                  BlockingQueue<Path> sortedFilesQueue,
                                  StatisticsProvider statisticsProvider,
                                  IndexingReporter reporter) {
        this.storeDir = storeDir;
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.algorithm = algorithm;
        this.sortedFilesQueue = sortedFilesQueue;
        this.statisticsProvider = statisticsProvider;
        this.reporter = reporter;

        this.mergeTriggerThreshold = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD, DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD);
        Validate.checkArgument(mergeTriggerThreshold >= 16,
                "Invalid value for property " + OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD + ": " + mergeTriggerThreshold + ". Must be >= 16");
        reporter.addConfig(OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD, mergeTriggerThreshold);

        this.minFilesToMerge = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE, DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE);
        Validate.checkArgument(minFilesToMerge >= 2,
                "Invalid value for property " + OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE + ": " + minFilesToMerge + ". Must be >= 2");
        reporter.addConfig(OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE, minFilesToMerge);

        this.maxFilesToMerge = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE, DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE);
        Validate.checkArgument(maxFilesToMerge >= minFilesToMerge,
                "Invalid value for property " + OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE + ": " + maxFilesToMerge + ". Must be >= " + OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE + " (" + minFilesToMerge + ")");
        reporter.addConfig(OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE, maxFilesToMerge);

        this.maxSizeToMergeMB = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB, DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB);
        Validate.checkArgument(maxSizeToMergeMB >= 1,
                "Invalid value for property " + OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB + ": " + maxSizeToMergeMB + ". Must be >= 1");
        reporter.addConfig(OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB, maxSizeToMergeMB);

        this.externalMergeReadBufferSize = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_EXTERNAL_MERGE_READ_BUFFER_SIZE, DEFAULT_OAK_INDEXER_PIPELINED_EXTERNAL_MERGE_READ_BUFFER_SIZE);
        Validate.checkArgument(externalMergeReadBufferSize >= FileUtils.ONE_KB,
                "Invalid value for property " + OAK_INDEXER_PIPELINED_EXTERNAL_MERGE_READ_BUFFER_SIZE + ": " + externalMergeReadBufferSize + ". Must be >= 1 KB");
        reporter.addConfig(OAK_INDEXER_PIPELINED_EXTERNAL_MERGE_READ_BUFFER_SIZE, externalMergeReadBufferSize);
    }

    @Override
    public Result call() throws Exception {
        this.eagerMergeRuns = 0;
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName(THREAD_NAME);
        int intermediateFilesCount = 0;
        INDEXING_PHASE_LOGGER.info("[TASK:{}:START] Starting merge sort task", THREAD_NAME.toUpperCase(Locale.ROOT));
        Stopwatch finalMergeWatch = Stopwatch.createUnstarted();
        try {
            while (true) {
                LOG.debug("Waiting for next intermediate sorted file");
                Path sortedIntermediateFile = sortedFilesQueue.take();
                if (sortedIntermediateFile == SENTINEL_SORTED_FILES_QUEUE) {
                    long sortedFilesSizeBytes = sizeOf(sortedFiles);
                    LOG.info("Going to sort {} files, total size {}", sortedFiles.size(), IOUtils.humanReadableByteCountBin(sortedFilesSizeBytes));
                    finalMergeWatch.start();
                    List<Path> simpleFileList = sortedFiles.stream().map(f -> f.file).collect(Collectors.toList());
                    Path flatFileStore = sortStoreFile(simpleFileList);

                    LOG.info("Final merge completed in {}. Created file: {}", FormattingUtils.formatToSeconds(finalMergeWatch), flatFileStore.toAbsolutePath());
                    long ffsSizeBytes = Files.size(flatFileStore);
                    long durationSeconds = finalMergeWatch.elapsed(TimeUnit.SECONDS);
                    String metrics = MetricsFormatter.newBuilder()
                            .add("duration", FormattingUtils.formatToSeconds(finalMergeWatch))
                            .add("durationSeconds", durationSeconds)
                            .add("intermediateFilesCount", intermediateFilesCount)
                            .add("eagerMergesRuns", eagerMergeRuns)
                            .add("filesMerged", sortedFiles.size())
                            .add("ffsSizeBytes", ffsSizeBytes)
                            .add("ffsSize", IOUtils.humanReadableByteCountBin(ffsSizeBytes))
                            .build();

                    INDEXING_PHASE_LOGGER.info("[TASK:{}:END] Metrics: {}", THREAD_NAME.toUpperCase(Locale.ROOT), metrics);
                    reporter.addTiming("Merge sort", FormattingUtils.formatToSeconds(finalMergeWatch));
                    MetricsUtils.addMetric(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_DURATION_SECONDS, durationSeconds);
                    MetricsUtils.addMetric(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_INTERMEDIATE_FILES_TOTAL, intermediateFilesCount);
                    MetricsUtils.addMetric(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_EAGER_MERGES_RUNS_TOTAL, eagerMergeRuns);
                    MetricsUtils.addMetric(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_FINAL_MERGE_FILES_COUNT_TOTAL, sortedFiles.size());
                    MetricsUtils.addMetricByteSize(statisticsProvider, reporter, PipelinedMetrics.OAK_INDEXER_PIPELINED_MERGE_SORT_FLAT_FILE_STORE_SIZE_BYTES, ffsSizeBytes);
                    return new Result(flatFileStore, intermediateFilesCount, sortedFiles.size(), eagerMergeRuns);

                } else {
                    sortedFiles.add(new PathAndSize(sortedIntermediateFile, Files.size(sortedIntermediateFile)));
                    intermediateFilesCount++;
                    LOG.info("Received new intermediate sorted file {}. Size: {}. Total files: {} of size {}",
                            sortedIntermediateFile, IOUtils.humanReadableByteCountBin(Files.size(sortedIntermediateFile)),
                            sortedFiles.size(), IOUtils.humanReadableByteCountBin(sizeOf(sortedFiles)));
                    // No point in doing eager merging if we already finished downloading from Mongo.
                    // In this case, we do only the final merge.
                    if (stopEagerMerging.get()) {
                        LOG.debug("Skipping eager merging because download from Mongo has finished");
                    } else {
                        tryMergeIntermediateFilesEagerly();
                    }
                }
            }
        } catch (Throwable t) {
            INDEXING_PHASE_LOGGER.info("[TASK:{}:FAIL] Metrics: {}, Error: {}", THREAD_NAME.toUpperCase(Locale.ROOT),
                    MetricsFormatter.createMetricsWithDurationOnly(finalMergeWatch),
                    t.toString());
            LOG.warn("Thread terminating with exception", t);
            throw t;
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }

    /**
     * Stops eager merging. After this method is called, eager merging will no longer be performed, even if the
     * conditions for merging are met.
     * This method should be called when the download terminates, to avoid starting new eager merges which would delay
     * the final merge.
     */
    public void stopEagerMerging() {
        stopEagerMerging.set(true);
    }

    private static long sizeOf(PriorityQueue<PathAndSize> sortedFiles) {
        return sortedFiles.stream().mapToLong(f -> f.size).sum();
    }

    private void tryMergeIntermediateFilesEagerly() throws IOException {
        if (sortedFiles.size() < mergeTriggerThreshold) {
            // Not enough intermediate files to merge.
            return;
        }

        ArrayList<PathAndSize> filesAndSizeToSort = new ArrayList<>();
        long sumOfSizesBytes = 0;
        while (!sortedFiles.isEmpty() &&
                filesAndSizeToSort.size() < maxFilesToMerge &&
                sumOfSizesBytes / FileUtils.ONE_MB < maxSizeToMergeMB) {
            // Get the next candidate. Do not remove the file from the queue because it may be too large for merging eagerly
            PathAndSize pathAndSize = sortedFiles.peek();
            if (pathAndSize.size / FileUtils.ONE_MB > maxSizeToMergeMB) {
                LOG.debug("File {} is too large to be merged. Size: {}, max allowed: {} MB. Stopping searching for intermediate files to merge because all other files are larger.",
                        pathAndSize.file.toAbsolutePath(),
                        IOUtils.humanReadableByteCountBin(pathAndSize.size),
                        maxSizeToMergeMB);
                break;
            }
            // Remove the file from the sorted files queue and add it to the list of files to sort
            sortedFiles.poll();
            filesAndSizeToSort.add(pathAndSize);
            sumOfSizesBytes += pathAndSize.size;
        }
        if (filesAndSizeToSort.size() < minFilesToMerge) {
            // Not enough candidate files to merge. Put back the candidate files in the sorted files queue
            sortedFiles.addAll(filesAndSizeToSort);
            LOG.debug("Not enough candidate files to merge. Found {} candidates of size {}, minimum for merging is {}",
                    filesAndSizeToSort.size(), IOUtils.humanReadableByteCountBin(sumOfSizesBytes), minFilesToMerge);
            return;
        }
        LOG.info("Merge threshold reached: {} > {}. Going to merge the following {} files {} of total size {}.",
                sortedFiles.size() + filesAndSizeToSort.size(), mergeTriggerThreshold,
                filesAndSizeToSort.size(),
                filesAndSizeToSort.stream()
                        .map(fs -> fs.file.getFileName() + ": " + IOUtils.humanReadableByteCountBin(fs.size))
                        .collect(Collectors.joining(", ", "[", "]")),
                IOUtils.humanReadableByteCountBin(sumOfSizesBytes));
        Stopwatch start = Stopwatch.createStarted();
        Path mergedFiled = sortStoreFile(filesAndSizeToSort.stream().map(f -> f.file).collect(Collectors.toList()));
        eagerMergeRuns++;
        Path destFile = mergedFiled.getParent().resolve("merged-" + mergedFilesCounter++);
        Files.move(mergedFiled, destFile);
        PathAndSize mergedPathAndSize = new PathAndSize(destFile, Files.size(destFile));
        sortedFiles.add(mergedPathAndSize);
        LOG.info("{} files merged in {} seconds. New file {}, size: {}",
                filesAndSizeToSort.size(), start.elapsed(TimeUnit.SECONDS),
                mergedPathAndSize.file.getFileName(), IOUtils.humanReadableByteCountBin(mergedPathAndSize.size));
    }

    private Path sortStoreFile(List<Path> sortedFilesBatch) throws IOException {
        Path sortedFile = storeDir.resolve(getSortedStoreFileName(algorithm));
        try (OutputStream writer = IndexStoreUtils.createOutputStream(sortedFile, algorithm)) {
            Function<byte[], NodeStateHolder> byteArrayToType = new NodeStateHolderFactory();
            Function<NodeStateHolder, byte[]> typeToByteArray = holder -> holder == null ? null : holder.getLine();
            ExternalSortByteArray.mergeSortedFilesBinary(sortedFilesBatch,
                    writer,
                    comparator,
                    true, //distinct
                    algorithm,
                    typeToByteArray,
                    byteArrayToType,
                    externalMergeReadBufferSize
            );
        }
        return sortedFile;
    }
}
