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

import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;

public class PipelinedSortBatchTaskTest {
    static class TestResult {
        final PipelinedSortBatchTask.Result result;
        final BlockingQueue<Path> sortedFilesQueue;

        public TestResult(PipelinedSortBatchTask.Result result, BlockingQueue<Path> sortedFilesQueue) {
            this.result = result;
            this.sortedFilesQueue = sortedFilesQueue;
        }

        public PipelinedSortBatchTask.Result getResult() {
            return result;
        }

        public BlockingQueue<Path> getSortedFilesQueue() {
            return sortedFilesQueue;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMergeSortTaskTest.class);

    @Rule
    public TemporaryFolder sortFolder = new TemporaryFolder();

    private final PathElementComparator pathComparator = new PathElementComparator(Set.of());
    private final Compression algorithm = Compression.NONE;

    @Test
    public void noBatch() throws Exception {

        TestResult testResult = runTest();

        PipelinedSortBatchTask.Result result = testResult.getResult();
        BlockingQueue<Path> sortedFilesQueue = testResult.getSortedFilesQueue();
        assertEquals(0, result.getTotalEntries());
        assertEquals(0, sortedFilesQueue.size());
    }

    @Test
    public void emptyBatch() throws Exception {
        NodeStateEntryBatch batch = NodeStateEntryBatch.createNodeStateEntryBatch(NodeStateEntryBatch.MIN_BUFFER_SIZE, 10);
        batch.flip();

        TestResult testResult = runTest(batch);

        PipelinedSortBatchTask.Result result = testResult.getResult();
        BlockingQueue<Path> sortedFilesQueue = testResult.getSortedFilesQueue();
        assertEquals(0, result.getTotalEntries());
        assertEquals(0, sortedFilesQueue.size());
    }

    @Test
    public void oneBatch() throws Exception {
        NodeStateEntryBatch batch = NodeStateEntryBatch.createNodeStateEntryBatch(NodeStateEntryBatch.MIN_BUFFER_SIZE, 10);
        addEntry(batch, "/a0/b0", "{\"key\":2}");
        addEntry(batch, "/a0", "{\"key\":1}");
        addEntry(batch, "/a1/b0", "{\"key\":6}");
        addEntry(batch, "/a0/b1", "{\"key\":5}");
        addEntry(batch, "/a0/b0/c1", "{\"key\":4}");
        addEntry(batch, "/a0/b0/c0", "{\"key\":3}");
        batch.flip();

        TestResult testResult = runTest(batch);

        PipelinedSortBatchTask.Result result = testResult.getResult();
        BlockingQueue<Path> sortedFilesQueue = testResult.getSortedFilesQueue();
        assertEquals(6, result.getTotalEntries());
        assertEquals(1, sortedFilesQueue.size());

        Path sortedFile = sortedFilesQueue.take();
        LOG.info("Sorted file:\n{}", Files.readString(sortedFile));
        assertEquals("/a0|{\"key\":1}\n" +
                        "/a0/b0|{\"key\":2}\n" +
                        "/a0/b0/c0|{\"key\":3}\n" +
                        "/a0/b0/c1|{\"key\":4}\n" +
                        "/a0/b1|{\"key\":5}\n" +
                        "/a1/b0|{\"key\":6}\n",
                Files.readString(sortedFile)
        );
    }

    @Test
    public void twoBatches() throws Exception {
        NodeStateEntryBatch batch1 = NodeStateEntryBatch.createNodeStateEntryBatch(NodeStateEntryBatch.MIN_BUFFER_SIZE, 10);
        addEntry(batch1, "/a0/b0", "{\"key\":2}");
        addEntry(batch1, "/a0", "{\"key\":1}");
        addEntry(batch1, "/a1/b0", "{\"key\":6}");
        batch1.flip();

        NodeStateEntryBatch batch2 = NodeStateEntryBatch.createNodeStateEntryBatch(NodeStateEntryBatch.MIN_BUFFER_SIZE, 10);
        addEntry(batch2, "/a0/b1", "{\"key\":5}");
        addEntry(batch2, "/a0/b0/c1", "{\"key\":4}");
        addEntry(batch2, "/a0/b0/c0", "{\"key\":3}");
        batch2.flip();

        TestResult testResult = runTest(batch1, batch2);

        PipelinedSortBatchTask.Result result = testResult.getResult();
        BlockingQueue<Path> sortedFilesQueue = testResult.getSortedFilesQueue();
        assertEquals(6, result.getTotalEntries());
        assertEquals(2, sortedFilesQueue.size());

        Path sortedFile1 = sortedFilesQueue.take();
        Path sortedFile2 = sortedFilesQueue.take();

        LOG.info("Sorted file:\n{}", Files.readString(sortedFile1));
        LOG.info("Sorted file:\n{}", Files.readString(sortedFile2));
        assertEquals("/a0|{\"key\":1}\n" +
                        "/a0/b0|{\"key\":2}\n" +
                        "/a1/b0|{\"key\":6}\n",
                Files.readString(sortedFile1));
        assertEquals("/a0/b0/c0|{\"key\":3}\n" +
                        "/a0/b0/c1|{\"key\":4}\n" +
                        "/a0/b1|{\"key\":5}\n",
                Files.readString(sortedFile2)
        );
    }

    private void addEntry(NodeStateEntryBatch batch, String path, String entry) {
        batch.addEntry(path, entry.getBytes(StandardCharsets.UTF_8));
    }

    private TestResult runTest(NodeStateEntryBatch... nodeStateEntryBatches) throws Exception {
        Path sortRoot = sortFolder.getRoot().toPath();
        int numberOfBuffers = nodeStateEntryBatches.length + 1; // +1 for the sentinel
        ArrayBlockingQueue<NodeStateEntryBatch> emptyBatchesQueue = new ArrayBlockingQueue<>(numberOfBuffers);
        ArrayBlockingQueue<NodeStateEntryBatch> nonEmptyBatchesQueue = new ArrayBlockingQueue<>(numberOfBuffers);
        ArrayBlockingQueue<Path> sortedFilesQueue = new ArrayBlockingQueue<>(numberOfBuffers);

        for (NodeStateEntryBatch nodeStateEntryBatch : nodeStateEntryBatches) {
            nonEmptyBatchesQueue.put(nodeStateEntryBatch);
        }
        nonEmptyBatchesQueue.put(PipelinedStrategy.SENTINEL_NSE_BUFFER);

        PipelinedSortBatchTask sortTask = new PipelinedSortBatchTask(
                sortRoot, pathComparator, algorithm, emptyBatchesQueue, nonEmptyBatchesQueue, sortedFilesQueue,
                StatisticsProvider.NOOP);
        PipelinedSortBatchTask.Result taskResult = sortTask.call();
        LOG.info("Result: {}", taskResult.getTotalEntries());
        LOG.info("Empty batches: {}", emptyBatchesQueue.size());
        LOG.info("Sorted files: {}", sortedFilesQueue.size());
        assertEquals(nodeStateEntryBatches.length, emptyBatchesQueue.size());
        return new TestResult(taskResult, sortedFilesQueue);
    }
}