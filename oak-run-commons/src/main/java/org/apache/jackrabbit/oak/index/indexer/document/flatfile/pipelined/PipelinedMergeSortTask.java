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
import org.apache.jackrabbit.guava.common.base.Preconditions;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.sort.ExternalSortByteArray;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.apache.jackrabbit.oak.plugins.index.MetricsFormatter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
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

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createOutputStream;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_SORTED_FILES_QUEUE;

/**
 * Accumulates the intermediate sorted files and, when all files are generated, merges them into a single sorted file,
 * the flat file store
 */
public class PipelinedMergeSortTask implements Callable<PipelinedMergeSortTask.Result> {

    public static final String OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD = "oak.indexer.pipelined.eagerMergeTriggerThreshold";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD = 64;

    /*
     * Maximum number of files to eagerly merge. This is to keep the eager merges efficient, as efficiency decreases
     * with the number of files in a merge
     */
    public static final String OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE = "oak.indexer.pipelined.eagerMergeMaxFilesToMerge";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE = 32;

    /*
     * Minimum number of files to eagerly merge at a time. Merging only a few files will not significantly reduce the
     * duration of the final merge, so it might end up being more expensive than having a final merge with a larger
     * number of files.
     */
    public static final String OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE = "oak.indexer.pipelined.eagerMergeMinFilesToMerge";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE = 8;

    /*
     * Maximum size of intermediate files that can be eagerly merged.
     */
    public static final String OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB = "oak.indexer.pipelined.eagerMergeMaxSizeToMergeMB";
    public static final int DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB = 1024;

    private final int mergeTriggerThreshold;
    private final int mergeMinFilesToMerge;
    private final int mergeMaxFilesToMerge;
    private final int mergeMaxSizeToMergeMB;

    // TODO: start merging small files into larger files to avoid having too many "small" files at the end.
    //  Idea: when there are more than k (for instance, 10) intermediate files whose size is under a certain limit
    //  (for instance, 1GB),  merge them into a  single file. And repeat the test whenever this task receives a new
    //  intermediate file.
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

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMergeSortTask.class);

    private static final String THREAD_NAME = "mongo-merge-sort-files";

    private final Path storeDir;
    private final Comparator<NodeStateHolder> comparator;
    private final Compression algorithm;
    private final BlockingQueue<Path> sortedFilesQueue;
    private final PriorityQueue<FileAndSize> sortedFiles = new PriorityQueue<>();
    private final AtomicBoolean stopEagerMerging = new AtomicBoolean(false);

    private int eagerMergeRuns;
    private int mergedFilesCounter = 0;

    public PipelinedMergeSortTask(Path storeDir,
                                  PathElementComparator pathComparator,
                                  Compression algorithm,
                                  BlockingQueue<Path> sortedFilesQueue) {
        this.storeDir = storeDir;
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.algorithm = algorithm;
        this.sortedFilesQueue = sortedFilesQueue;

        this.mergeTriggerThreshold = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD, DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD);
        Preconditions.checkArgument(mergeTriggerThreshold >= 16,
                "Invalid value for property " + OAK_INDEXER_PIPELINED_EAGER_MERGE_TRIGGER_THRESHOLD + ": " + mergeTriggerThreshold + ". Must be >= 16");

        this.mergeMinFilesToMerge = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE, DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE);
        Preconditions.checkArgument(mergeMinFilesToMerge >= 2,
                "Invalid value for property " + OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE + ": " + mergeMinFilesToMerge + ". Must be >= 2");

        this.mergeMaxFilesToMerge = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE, DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE);
        Preconditions.checkArgument(mergeMaxFilesToMerge >= mergeMinFilesToMerge,
                "Invalid value for property " + OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_FILES_TO_MERGE + ": " + mergeMaxFilesToMerge + ". Must be >= " + OAK_INDEXER_PIPELINED_EAGER_MERGE_MIN_FILES_TO_MERGE + " (" + mergeMinFilesToMerge + ")");

        this.mergeMaxSizeToMergeMB = ConfigHelper.getSystemPropertyAsInt(OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB, DEFAULT_OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB);
        Preconditions.checkArgument(mergeMaxSizeToMergeMB >= 1,
                "Invalid value for property " + OAK_INDEXER_PIPELINED_EAGER_MERGE_MAX_SIZE_TO_MERGE_MB + ": " + mergeMaxSizeToMergeMB + ". Must be >= 1");
    }

    @Override
    public Result call() throws Exception {
        this.eagerMergeRuns = 0;
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName(THREAD_NAME);
        int intermediateFilesCount = 0;
        try {
            LOG.info("[TASK:{}:START] Starting merge sort task", THREAD_NAME.toUpperCase(Locale.ROOT));
            while (true) {
                LOG.debug("Waiting for next intermediate sorted file");
                Path sortedIntermediateFile = sortedFilesQueue.take();
                if (sortedIntermediateFile == SENTINEL_SORTED_FILES_QUEUE) {
                    long sortedFilesSizeBytes = sizeOf(sortedFiles);
                    LOG.info("Going to sort {} files, total size {}", sortedFiles.size(), FileUtils.byteCountToDisplaySize(sortedFilesSizeBytes));
                    Stopwatch w = Stopwatch.createStarted();
                    List<Path> simpleFileList = sortedFiles.stream().map(f -> f.file).collect(Collectors.toList());
                    Path flatFileStore = sortStoreFile(simpleFileList);

                    LOG.info("Final merge completed in {}. Created file: {}", FormattingUtils.formatToSeconds(w), flatFileStore.toAbsolutePath());
                    long ffsSizeBytes = Files.size(flatFileStore);
                    String metrics = MetricsFormatter.newBuilder()
                            .add("duration", FormattingUtils.formatToSeconds(w))
                            .add("durationSeconds", w.elapsed(TimeUnit.SECONDS))
                            .add("filesMerged", sortedFiles.size())
                            .add("ffsSizeBytes", ffsSizeBytes)
                            .add("ffsSize", FileUtils.byteCountToDisplaySize(ffsSizeBytes))
                            .build();

                    LOG.info("[TASK:{}:END] Metrics: {}", THREAD_NAME.toUpperCase(Locale.ROOT), metrics);
                    return new Result(flatFileStore, intermediateFilesCount, sortedFiles.size(), eagerMergeRuns);

                } else {
                    sortedFiles.add(new FileAndSize(sortedIntermediateFile, Files.size(sortedIntermediateFile)));
                    intermediateFilesCount++;
                    LOG.info("Received new intermediate sorted file {}. Size: {}. Total files: {} of size {}",
                            sortedIntermediateFile, FileUtils.byteCountToDisplaySize(Files.size(sortedIntermediateFile)),
                            sortedFiles.size(), FileUtils.byteCountToDisplaySize(sizeOf(sortedFiles)));
                    // No point in doing eager merging if all the remaining intermediate files are already in the queue.
                    // In this case, we do only the final merge.
                    if (stopEagerMerging.get()) {
                        LOG.debug("Skipping eager merging because download from Mongo has finished");
                    } else {
                        tryMergeIntermediateFilesEagerly();
                    }
                }
            }
        } catch (InterruptedException t) {
            LOG.warn("Thread interrupted", t);
            throw t;
        } catch (Throwable t) {
            LOG.warn("Thread terminating with exception", t);
            throw t;
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }

    /**
     * This method is called by the producer thread when it finishes producing the intermediate sorted files.
     * It is used to signal the consumer thread that no more files will be added to the queue, so that whatever is left
     * in the queue, is all that will ever be produced.
     */
    public void stopEagerMerging() {
        stopEagerMerging.set(true);
    }

    private static long sizeOf(PriorityQueue<FileAndSize> sortedFiles) {
        return sortedFiles.stream().mapToLong(f -> f.size).sum();
    }

    private static class FileAndSize implements Comparable<FileAndSize> {
        final Path file;
        final long size;

        public FileAndSize(Path file, long size) {
            this.file = file;
            this.size = size;
        }

        @Override
        public String toString() {
            return "FileAndSize{" +
                    "file=" + file.toString() +
                    ", size=" + FileUtils.byteCountToDisplaySize(size) +
                    '}';
        }

        @Override
        public int compareTo(@NotNull PipelinedMergeSortTask.FileAndSize o) {
            return Long.compare(size, o.size);
        }
    }

    private void tryMergeIntermediateFilesEagerly() throws IOException {
        if (sortedFiles.size() < mergeTriggerThreshold) {
            return;
        }

        ArrayList<FileAndSize> filesAndSizeToSort = new ArrayList<>();
        long sumOfSizesBytes = 0;
        while (!sortedFiles.isEmpty() &&
                filesAndSizeToSort.size() < mergeMaxFilesToMerge &&
                sumOfSizesBytes / FileUtils.ONE_MB < mergeMaxSizeToMergeMB &&
                filesAndSizeToSort.size() < mergeMinFilesToMerge) {
            // Get the next candidate. Do not remove the file from the queue because it may be too large for merging eagerly
            FileAndSize fileAndSize = sortedFiles.peek();
            if (fileAndSize.size / FileUtils.ONE_MB > mergeMaxSizeToMergeMB) {
                LOG.debug("File {} is too large to be merged. Size: {}, max allowed: {} MB. Stopping searching for intermediate files to merge because all other files are larger.",
                        fileAndSize.file.toAbsolutePath(),
                        FileUtils.byteCountToDisplaySize(fileAndSize.size),
                        mergeMaxSizeToMergeMB);
                break;
            }
            // Remove the file from the sorted files queue and add it to the list of files to sort
            sortedFiles.poll();
            filesAndSizeToSort.add(fileAndSize);
            sumOfSizesBytes += fileAndSize.size;
        }
        if (sumOfSizesBytes / FileUtils.ONE_MB > mergeMaxSizeToMergeMB) {
            LOG.debug("Reached maximum size for eager merging: {} MB.", FileUtils.byteCountToDisplaySize(sumOfSizesBytes));
        }
        if (filesAndSizeToSort.size() < mergeMinFilesToMerge) {
            sortedFiles.addAll(filesAndSizeToSort);
            LOG.debug("Not enough candidate files to merge. Found {} candidates of size {}, minimum for merging is {}",
                    filesAndSizeToSort.size(), FileUtils.byteCountToDisplaySize(sumOfSizesBytes), mergeMinFilesToMerge);
            return;
        }
        LOG.info("Merge threshold reached: {} > {}. Going to merge the following {} files {}.",
                sortedFiles.size() + filesAndSizeToSort.size(), mergeTriggerThreshold,
                filesAndSizeToSort.size(),
                filesAndSizeToSort.stream()
                        .map(fs -> fs.file.getFileName() + ": " + FileUtils.byteCountToDisplaySize(fs.size))
                        .collect(Collectors.joining(", ", "[", "]")));
        Stopwatch start = Stopwatch.createStarted();
        Path mergedFiled = sortStoreFile(filesAndSizeToSort.stream().map(f -> f.file).collect(Collectors.toList()));
        eagerMergeRuns++;
        Path destFile = mergedFiled.getParent().resolve("merged-" + mergedFilesCounter++);
        Files.move(mergedFiled, destFile);
        FileAndSize mergedFileAndSize = new FileAndSize(destFile, Files.size(destFile));
        sortedFiles.add(mergedFileAndSize);
        LOG.info("{} files merged in {} seconds. New file {}, size: {}",
                filesAndSizeToSort.size(), start.elapsed(TimeUnit.SECONDS),
                mergedFileAndSize.file.getFileName(), FileUtils.byteCountToDisplaySize(mergedFileAndSize.size));

    }

    private Path sortStoreFile(List<Path> sortedFilesBatch) throws IOException {
        Path sortedFile = storeDir.resolve(getSortedStoreFileName(algorithm));
        try (BufferedOutputStream writer = createOutputStream(sortedFile, algorithm)) {
            Function<byte[], NodeStateHolder> byteArrayToType = new NodeStateHolderFactory();
            Function<NodeStateHolder, byte[]> typeToByteArray = holder -> holder == null ? null : holder.getLine();
            ExternalSortByteArray.mergeSortedFilesBinary(sortedFilesBatch,
                    writer,
                    comparator,
                    true, //distinct
                    algorithm,
                    typeToByteArray,
                    byteArrayToType
            );
        }
        return sortedFile;
    }
}
