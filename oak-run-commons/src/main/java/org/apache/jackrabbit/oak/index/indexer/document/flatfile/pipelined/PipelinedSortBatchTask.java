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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createOutputStream;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy.SENTINEL_NSE_BUFFER;

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

    private final Comparator<SortKey> pathComparator;
    private final Compression algorithm;
    private final BlockingQueue<NodeStateEntryBatch> emptyBuffersQueue;
    private final BlockingQueue<NodeStateEntryBatch> nonEmptyBuffersQueue;
    private final BlockingQueue<File> sortedFilesQueue;
    private final File sortWorkDir;
    private final byte[] copyBuffer = new byte[4096];
    private long entriesProcessed = 0;

    public PipelinedSortBatchTask(File storeDir,
                                  PathElementComparator pathComparator,
                                  Compression algorithm,
                                  BlockingQueue<NodeStateEntryBatch> emptyBuffersQueue,
                                  BlockingQueue<NodeStateEntryBatch> nonEmptyBuffersQueue,
                                  BlockingQueue<File> sortedFilesQueue) throws IOException {
        this.pathComparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.algorithm = algorithm;
        this.emptyBuffersQueue = emptyBuffersQueue;
        this.nonEmptyBuffersQueue = nonEmptyBuffersQueue;
        this.sortedFilesQueue = sortedFilesQueue;
        this.sortWorkDir = createdSortWorkDir(storeDir);
    }

    @Override
    public Result call() throws Exception {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName("mongo-sort-batch");
        try {
            LOG.info("Starting sort-and-save task");
            while (true) {
                LOG.info("Waiting for next batch");
                NodeStateEntryBatch nseBuffer = nonEmptyBuffersQueue.take();
                if (nseBuffer == SENTINEL_NSE_BUFFER) {
                    LOG.info("Terminating thread, processed {} entries", entriesProcessed);
                    return new Result(entriesProcessed);
                }
                sortAndSaveBatch(nseBuffer);
                nseBuffer.reset();
                emptyBuffersQueue.put(nseBuffer);
            }
        } catch (InterruptedException t) {
            LOG.warn("Thread interrupted");
            throw t;
        } catch (Throwable t) {
            LOG.warn("Thread terminating with exception.", t);
            throw t;
        } finally {
            Thread.currentThread().setName(originalName);
        }
    }

    private void sortAndSaveBatch(NodeStateEntryBatch nseb) throws Exception {
        ArrayList<SortKey> sortBuffer = nseb.getSortBuffer();
        ByteBuffer buffer = nseb.getBuffer();
        LOG.info("Going to sort batch in memory. Entries: {}, Size: {}",
                sortBuffer.size(), FileUtils.byteCountToDisplaySize(buffer.remaining()));
        if (sortBuffer.size() == 0) {
            return;
        }
        Stopwatch sortClock = Stopwatch.createStarted();
        sortBuffer.sort(pathComparator);
        LOG.info("Sorted batch in {}. Saving to disk.", sortClock);
        Stopwatch saveClock = Stopwatch.createStarted();
        File newtmpfile = File.createTempFile("sortInBatch", "flatfile", sortWorkDir);
        long textSize = 0;
        try (BufferedOutputStream writer = createOutputStream(newtmpfile, algorithm)) {
            for (SortKey entry : sortBuffer) {
                entriesProcessed++;
                // Retrieve the entry from the buffer
                int posInBuffer = entry.getBufferPos();
                buffer.position(posInBuffer);
                int entrySize = buffer.getInt();

                // Write the entry to the file without creating intermediate byte[]
                int bytesRemaining = entrySize;
                while (bytesRemaining > 0) {
                    int bytesRead = Math.min(copyBuffer.length, bytesRemaining);
                    buffer.get(copyBuffer, 0, bytesRead);
                    writer.write(copyBuffer, 0, bytesRead);
                    bytesRemaining -= bytesRead;
                }
                writer.write('\n');
                textSize += entrySize + 1;
            }
        }
        LOG.info("Stored batch of size {} (uncompressed {}) with {} entries in {}",
                FileUtils.byteCountToDisplaySize(newtmpfile.length()),
                FileUtils.byteCountToDisplaySize(textSize),
                sortBuffer.size(), saveClock);
        sortedFilesQueue.put(newtmpfile);
    }

    private static File createdSortWorkDir(File storeDir) throws IOException {
        File sortedFileDir = new File(storeDir, "sort-work-dir");
        FileUtils.forceMkdir(sortedFileDir);
        return sortedFileDir;
    }
}
