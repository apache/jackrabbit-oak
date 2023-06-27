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
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.sizeOf;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.FLATFILESTORE_CHARSET;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_SORTED_FILES_QUEUE;

/**
 * Accumulates the intermediate sorted files and, when all files are genereted, merges them into a single sorted file,
 * the flat file store
 */
class PipelinedMergeSortTask implements Callable<PipelinedMergeSortTask.Result> {
    // TODO: start merging small files into larger files to avoid having too many "small" files at the end.
    //  Idea: when there are more than k (for instance, 10) intermediate files whose size is under a certain limit
    //  (for instance, 1GB),  merge them into a  single file. And repeat the test whenever this task receives a new
    //  intermediate file.
    public static class Result {
        private final File flatFileStoreFile;

        public Result(File flatFileStoreFile) {
            this.flatFileStoreFile = flatFileStoreFile;
        }

        public File getFlatFileStoreFile() {
            return flatFileStoreFile;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMergeSortTask.class);

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
        Thread.currentThread().setName("mongo-merge-sort-files");
        try {
            LOG.info("Starting merge sort thread");
            while (true) {
                LOG.info("Waiting for next intermediate sorted file");
                File sortedIntermediateFile = sortedFilesQueue.take();
                if (sortedIntermediateFile == SENTINEL_SORTED_FILES_QUEUE) {
                    LOG.info("Going to sort {} files of total size {}", sortedFiles.size(), FileUtils.byteCountToDisplaySize(sizeOf(sortedFiles)));
                    File flatFileStore = sortStoreFile(sortedFiles);
                    LOG.info("Terminating thread, merged {} files", sortedFiles.size());
                    return new Result(flatFileStore);
                }
                sortedFiles.add(sortedIntermediateFile);
                LOG.info("Received new intermediate sorted file {}. Size: {}. Total files: {} of size {}",
                        sortedIntermediateFile, FileUtils.byteCountToDisplaySize(sortedIntermediateFile.length()),
                        sortedFiles.size(), FileUtils.byteCountToDisplaySize(sizeOf(sortedFiles)));
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
        LOG.info("Proceeding to perform merge of {} sorted files", sortedFilesBatch.size());
        Stopwatch w = Stopwatch.createStarted();
        File sortedFile = new File(storeDir, getSortedStoreFileName(algorithm));
        try (BufferedWriter writer = createWriter(sortedFile, algorithm)) {
            Function<String, NodeStateHolder> func1 = (line) -> line == null ? null : new NodeStateHolder(line);
            Function<NodeStateHolder, String> func2 = holder -> holder == null ? null : holder.getLine();
            ExternalSort.mergeSortedFiles(sortedFilesBatch,
                    writer,
                    comparator,
                    FLATFILESTORE_CHARSET,
                    true, //distinct
                    algorithm,
                    func2,
                    func1
            );
        }
        LOG.info("Merging of sorted files completed in {}", w);
        return sortedFile;
    }
}
