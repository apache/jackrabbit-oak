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

import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.apache.jackrabbit.oak.plugins.index.MetricsFormatter;
import org.apache.jackrabbit.oak.plugins.index.FormatingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCountBin;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.sizeOf;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.FLATFILESTORE_CHARSET;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_SORTED_FILES_QUEUE;

/**
 * Accumulates the intermediate sorted files and, when all files are generated, merges them into a single sorted file,
 * the flat file store
 */
class PipelinedMergeSortTask implements Callable<PipelinedMergeSortTask.Result> {
    // TODO: start merging small files into larger files to avoid having too many "small" files at the end.
    //  Idea: when there are more than k (for instance, 10) intermediate files whose size is under a certain limit
    //  (for instance, 1GB),  merge them into a  single file. And repeat the test whenever this task receives a new
    //  intermediate file.
    public static class Result {
        private final File flatFileStoreFile;
        private final int filesMerged;

        public Result(File flatFileStoreFile, int filesMerged) {
            this.flatFileStoreFile = flatFileStoreFile;
            this.filesMerged = filesMerged;
        }

        public File getFlatFileStoreFile() {
            return flatFileStoreFile;
        }

        public int getFilesMerged() {
            return filesMerged;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMergeSortTask.class);

    private static final String THREAD_NAME = "mongo-merge-sort-files";

    private final File storeDir;
    private final Comparator<NodeStateHolder> comparator;
    private final Compression algorithm;
    private final BlockingQueue<File> sortedFilesQueue;
    private final ArrayList<File> sortedFiles = new ArrayList<>();

    public PipelinedMergeSortTask(File storeDir,
                                  PathElementComparator pathComparator,
                                  Compression algorithm,
                                  BlockingQueue<File> sortedFilesQueue) {
        this.storeDir = storeDir;
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.algorithm = algorithm;
        this.sortedFilesQueue = sortedFilesQueue;
    }

    @Override
    public Result call() throws Exception {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName(THREAD_NAME);
        try {
            LOG.info("[TASK:{}:START] Starting merge-sort-task.", THREAD_NAME.toUpperCase(Locale.ROOT));
            while (true) {
                LOG.info("Waiting for next intermediate sorted file");
                File sortedIntermediateFile = sortedFilesQueue.take();
                if (sortedIntermediateFile == SENTINEL_SORTED_FILES_QUEUE) {
                    long sortedFilesSizeBytes = sizeOf(sortedFiles);
                    LOG.info("Going to sort {} files, total size {}", sortedFiles.size(), humanReadableByteCountBin(sortedFilesSizeBytes));
                    Stopwatch w = Stopwatch.createStarted();
                    File flatFileStore = sortStoreFile(sortedFiles);
                    LOG.info("Final merge completed in {}. Created file: {}", FormatingUtils.formatToSeconds(w), flatFileStore.getAbsolutePath());
                    long ffsSizeBytes = flatFileStore.length();
                    String metrics = MetricsFormatter.newBuilder()
                            .add("duration", FormatingUtils.formatToSeconds(w))
                            .add("filesMerged", sortedFiles.size())
                            .add("ffsSizeBytes", ffsSizeBytes)
                            .add("ffsSize", humanReadableByteCountBin(ffsSizeBytes))
                            .build();

                    LOG.info("[TASK:{}:END] Metrics: {}", THREAD_NAME.toUpperCase(Locale.ROOT), metrics);
                    return new Result(flatFileStore, sortedFiles.size());
                }
                sortedFiles.add(sortedIntermediateFile);
                LOG.info("Received new intermediate sorted file {}. Size: {}. Total files: {} of size {}",
                        sortedIntermediateFile, humanReadableByteCountBin(sortedIntermediateFile.length()),
                        sortedFiles.size(), humanReadableByteCountBin(sizeOf(sortedFiles)));
            }
        } catch (InterruptedException t) {
            LOG.warn("Thread interrupted", t);
            throw t;
        } catch (Throwable t) {
            LOG.warn("Thread terminating with exception.", t);
            throw t;
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }

    private File sortStoreFile(List<File> sortedFilesBatch) throws IOException {
        File sortedFile = new File(storeDir, getSortedStoreFileName(algorithm));
        try (BufferedWriter writer = createWriter(sortedFile, algorithm)) {
            Function<String, NodeStateHolder> stringToType = (line) -> line == null ? null : new NodeStateHolder(line);
            Function<NodeStateHolder, String> typeToString = holder -> holder == null ? null : holder.getLine();
            ExternalSort.mergeSortedFiles(sortedFilesBatch,
                    writer,
                    comparator,
                    FLATFILESTORE_CHARSET,
                    true, //distinct
                    algorithm,
                    typeToString,
                    stringToType
            );
        }
        return sortedFile;
    }
}
