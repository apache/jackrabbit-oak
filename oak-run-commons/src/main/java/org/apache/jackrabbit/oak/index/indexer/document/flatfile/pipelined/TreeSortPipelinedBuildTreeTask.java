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
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Session;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Store;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.StoreBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.TreeSortPipelinedStrategy.SENTINEL_NODE_STATE_ENTRY_JSON;

/**
 * Receives batches of node state entries, sorts then in memory, and finally writes them to a file.
 */
class TreeSortPipelinedBuildTreeTask implements Callable<TreeSortPipelinedBuildTreeTask.Result> {

    private final Store store;
    private final Session session;

    public static class Result {
        private final File sortedTreeDirectory;
        private final long totalEntries;

        public Result(File sortedTreeDirectory, long totalEntries) {
            this.sortedTreeDirectory = sortedTreeDirectory;
            this.totalEntries = totalEntries;
        }

        public long getTotalEntries() {
            return totalEntries;
        }

        public File getSortedTreeDirectory() {
            return sortedTreeDirectory;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(TreeSortPipelinedBuildTreeTask.class);
    private final BlockingQueue<List<NodeStateEntryJson>> nonEmptyBuffersQueue;
    private final File storeDir;
    private long totalEntriesProcessed = 0;

    public TreeSortPipelinedBuildTreeTask(File storeDir,
                                          Compression algorithm,
                                          BlockingQueue<List<NodeStateEntryJson>> nseQueue) {
        this.nonEmptyBuffersQueue = nseQueue;
        this.storeDir = storeDir;

        String storeConfig = "type=file\n" +
                "cacheSizeMB=2048\n" +
                "maxFileSize=48000000\n" +
                "dir=" + this.storeDir.getAbsolutePath();
        this.store = StoreBuilder.build(storeConfig);
        store.setWriteCompression(algorithm);
        session = new Session(store);
        session.init();
    }

    @Override
    public Result call() throws Exception {
        String originalName = Thread.currentThread().getName();
        Thread.currentThread().setName("mongo-sort-batch");
        try {
            LOG.info("Starting sort-and-save task");
            while (true) {
                LOG.info("Waiting for next batch");
                List<NodeStateEntryJson> nseBuffer = nonEmptyBuffersQueue.take();
                if (nseBuffer == SENTINEL_NODE_STATE_ENTRY_JSON) {
                    LOG.info("Received sentinel. Performing last checkpoint.");
                    session.checkpoint();
                    LOG.info("Merging roots.");
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    session.mergeRoots(Integer.MAX_VALUE);
                    session.flush();
                    LOG.info("Merged roots in {}", stopwatch);
                    store.close();
                    LOG.info("Terminating thread, processed {} entries", totalEntriesProcessed);
                    return new Result(storeDir, totalEntriesProcessed);
                }
                addBatchToTree(nseBuffer);
                nseBuffer.clear();
            }
        } catch (InterruptedException t) {
            LOG.warn("Thread interrupted", t);
            throw t;
        } catch (Throwable t) {
            LOG.warn("Thread terminating with exception.", t);
            throw t;
        } finally {
            LOG.info("Closing store");
            store.close();
            Thread.currentThread().setName(originalName);
        }
    }

    private void addBatchToTree(List<NodeStateEntryJson> nseb) {
        long textSizeInCurrentTree = 0;
        for (NodeStateEntryJson entry : nseb) {
            totalEntriesProcessed++;
            // Retrieve the entry from the buffer
            session.put(entry.path, entry.json);
            textSizeInCurrentTree += entry.path.length() + entry.json.length();
        }
        LOG.info("Checkpointing tree with: {} entries of size {}", nseb.size(), FileUtils.byteCountToDisplaySize(textSizeInCurrentTree));
        session.checkpoint();
        session.mergeRoots(10);
    }
}
