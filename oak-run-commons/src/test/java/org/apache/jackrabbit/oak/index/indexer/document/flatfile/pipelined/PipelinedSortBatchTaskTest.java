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
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static org.junit.Assert.assertEquals;

public class PipelinedSortBatchTaskTest {
    static class TestResult {
        final PipelinedSortBatchTask.Result result;
        final BlockingQueue<File> sortedFilesQueue;

        public TestResult(PipelinedSortBatchTask.Result result, BlockingQueue<File> sortedFilesQueue) {
            this.result = result;
            this.sortedFilesQueue = sortedFilesQueue;
        }

        public PipelinedSortBatchTask.Result getResult() {
            return result;
        }

        public BlockingQueue<File> getSortedFilesQueue() {
            return sortedFilesQueue;
        }
    }
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedMergeSortTaskTest.class);

    @Rule
    public TemporaryFolder sortFolder = new TemporaryFolder();

    private final PathElementComparator pathComparator = new PathElementComparator(Set.of());
    private final Compression algorithm = Compression.NONE;
    private final NodeStateEntryWriter nodeStateEntryWriter = new NodeStateEntryWriter(new MemoryBlobStore());

    @Test
    public void noBatch() throws Exception {

        TestResult testResult = runTest();

        PipelinedSortBatchTask.Result result = testResult.getResult();
        BlockingQueue<File> sortedFilesQueue = testResult.getSortedFilesQueue();
        assertEquals(0, result.getTotalEntries());
        assertEquals(0, sortedFilesQueue.size());
    }

    @Test
    public void emptyBatch() throws Exception {
        NodeStateEntryBatch batch = NodeStateEntryBatch.createNodeStateEntryBatch(1024, 10);

        TestResult testResult = runTest(batch);

        PipelinedSortBatchTask.Result result = testResult.getResult();
        BlockingQueue<File> sortedFilesQueue = testResult.getSortedFilesQueue();
        assertEquals(0, result.getTotalEntries());
        assertEquals(0, sortedFilesQueue.size());
    }

    @Test
    public void oneBatch() throws Exception {
        NodeStateEntryBatch batch = NodeStateEntryBatch.createNodeStateEntryBatch(1024, 10);
        addEntry(batch, "/a0/b0", "{\"key\":2}");
        addEntry(batch, "/a0", "{\"key\":1}");
        addEntry(batch, "/a1/b0", "{\"key\":6}");
        addEntry(batch, "/a0/b1", "{\"key\":5}");
        addEntry(batch, "/a0/b0/c1", "{\"key\":4}");
        addEntry(batch, "/a0/b0/c0", "{\"key\":3}");

        TestResult testResult = runTest(batch);

        PipelinedSortBatchTask.Result result = testResult.getResult();
        BlockingQueue<File> sortedFilesQueue = testResult.getSortedFilesQueue();
        assertEquals(6, result.getTotalEntries());
        assertEquals(1, sortedFilesQueue.size());

        File sortedFile = sortedFilesQueue.take();
        LOG.info("Sorted file:\n{}", Files.readString(sortedFile.toPath()));
        assertEquals("/a0|{\"key\":1}\n" +
                        "/a0/b0|{\"key\":2}\n" +
                        "/a0/b0/c0|{\"key\":3}\n" +
                        "/a0/b0/c1|{\"key\":4}\n" +
                        "/a0/b1|{\"key\":5}\n" +
                        "/a1/b0|{\"key\":6}\n",
                Files.readString(sortedFile.toPath())
        );
    }

    @Test
    public void twoBatches() throws Exception {
        NodeStateEntryBatch batch1 = NodeStateEntryBatch.createNodeStateEntryBatch(1024, 10);
        addEntry(batch1, "/a0/b0", "{\"key\":2}");
        addEntry(batch1, "/a0", "{\"key\":1}");
        addEntry(batch1, "/a1/b0", "{\"key\":6}");

        NodeStateEntryBatch batch2 = NodeStateEntryBatch.createNodeStateEntryBatch(1024, 10);
        addEntry(batch2, "/a0/b1", "{\"key\":5}");
        addEntry(batch2, "/a0/b0/c1", "{\"key\":4}");
        addEntry(batch2, "/a0/b0/c0", "{\"key\":3}");

        TestResult testResult = runTest(batch1, batch2);

        PipelinedSortBatchTask.Result result = testResult.getResult();
        BlockingQueue<File> sortedFilesQueue = testResult.getSortedFilesQueue();
        assertEquals(6, result.getTotalEntries());
        assertEquals(2, sortedFilesQueue.size());

        File sortedFile1 = sortedFilesQueue.take();
        File sortedFile2 = sortedFilesQueue.take();

        LOG.info("Sorted file:\n{}", Files.readString(sortedFile1.toPath()));
        LOG.info("Sorted file:\n{}", Files.readString(sortedFile2.toPath()));
        assertEquals("/a0|{\"key\":1}\n" +
                        "/a0/b0|{\"key\":2}\n" +
                        "/a1/b0|{\"key\":6}\n",
                Files.readString(sortedFile1.toPath()));
        assertEquals("/a0/b0/c0|{\"key\":3}\n" +
                        "/a0/b0/c1|{\"key\":4}\n" +
                        "/a0/b1|{\"key\":5}\n",
                Files.readString(sortedFile2.toPath())
        );
    }

    private void addEntry(NodeStateEntryBatch batch, String path, String entry) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Writer writer = new OutputStreamWriter(baos);
        nodeStateEntryWriter.writeTo(writer, path, entry);
        writer.close();
        batch.addEntry(path, baos.toByteArray());
    }

    private TestResult runTest(NodeStateEntryBatch... nodeStateEntryBatches) throws Exception {
        File sortRoot = sortFolder.getRoot();
        int numberOfBuffers = nodeStateEntryBatches.length + 1; // +1 for the sentinel
        ArrayBlockingQueue<NodeStateEntryBatch> emptyBatchesQueue = new ArrayBlockingQueue<>(numberOfBuffers);
        ArrayBlockingQueue<NodeStateEntryBatch> nonEmptyBatchesQueue = new ArrayBlockingQueue<>(numberOfBuffers);
        ArrayBlockingQueue<File> sortedFilesQueue = new ArrayBlockingQueue<>(numberOfBuffers);

        for (NodeStateEntryBatch nodeStateEntryBatch : nodeStateEntryBatches) {
            nonEmptyBatchesQueue.put(nodeStateEntryBatch);
        }
        nonEmptyBatchesQueue.put(PipelinedStrategy.SENTINEL_NSE_BUFFER);

        PipelinedSortBatchTask sortTask = new PipelinedSortBatchTask(
                sortRoot, pathComparator, algorithm, emptyBatchesQueue, nonEmptyBatchesQueue, sortedFilesQueue
        );
        PipelinedSortBatchTask.Result taskResult = sortTask.call();
        LOG.info("Result: {}", taskResult.getTotalEntries());
        LOG.info("Empty batches: {}", emptyBatchesQueue.size());
        LOG.info("Sorted files: {}", sortedFilesQueue.size());
        assertEquals(nodeStateEntryBatches.length, emptyBatchesQueue.size());
        return new TestResult(taskResult, sortedFilesQueue);
    }
}