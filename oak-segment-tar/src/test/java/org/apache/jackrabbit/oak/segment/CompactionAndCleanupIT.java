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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Integer.getInteger;
import static java.lang.String.valueOf;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreGCMonitor;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompactionAndCleanupIT {

    private static final Logger log = LoggerFactory
            .getLogger(CompactionAndCleanupIT.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    @Test
    public void compactionNoBinaryClone() throws Exception {
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
        FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withGCOptions(defaultGCOptions().setRetainedGenerations(2))
                .withStatisticsProvider(new DefaultStatisticsProvider(executor))
                .withMaxFileSize(1)
                .build();
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

        try {
            // 5MB blob
            int blobSize = 5 * 1024 * 1024;

            // Create ~2MB of data
            NodeBuilder extra = nodeStore.getRoot().builder();
            NodeBuilder content = extra.child("content");
            for (int i = 0; i < 10000; i++) {
                NodeBuilder c = content.child("c" + i);
                for (int j = 0; j < 1000; j++) {
                    c.setProperty("p" + i, "v" + i);
                }
            }
            nodeStore.merge(extra, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            long size1 = fileStore.getStats().getApproximateSize();
            log.debug("File store size {}", byteCountToDisplaySize(size1));

            // Create a property with 5 MB blob
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setProperty("blob1", createBlob(nodeStore, blobSize));
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            long size2 = fileStore.getStats().getApproximateSize();
            assertTrue("the store should grow", size2 > size1);
            assertTrue("the store should grow of at least the size of the blob", size2 - size1 >= blobSize);

            // Now remove the property. No gc yet -> size doesn't shrink
            builder = nodeStore.getRoot().builder();
            builder.removeProperty("blob1");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            long size3 = fileStore.getStats().getApproximateSize();
            assertTrue("the store should grow", size3 > size2);

            // 1st gc cycle -> no reclaimable garbage...
            fileStore.compact();
            fileStore.cleanup();

            long size4 = fileStore.getStats().getApproximateSize();

            // Add another 5MB binary doubling the blob size
            builder = nodeStore.getRoot().builder();
            builder.setProperty("blob2", createBlob(nodeStore, blobSize));
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            long size5 = fileStore.getStats().getApproximateSize();
            assertTrue("the store should grow", size5 > size4);
            assertTrue("the store should grow of at least the size of the blob", size5 - size4 >= blobSize);

            // 2st gc cycle -> 1st blob should get collected
            fileStore.compact();
            fileStore.cleanup();

            long size6 = fileStore.getStats().getApproximateSize();
            assertTrue("the store should shrink", size6 < size5);
            assertTrue("the store should shrink of at least the size of the blob", size5 - size6 >= blobSize);

            // 3rtd gc cycle -> no  significant change
            fileStore.compact();
            fileStore.cleanup();

            long size7 = fileStore.getStats().getApproximateSize();

            // No data loss
            byte[] blob = ByteStreams.toByteArray(nodeStore.getRoot()
                    .getProperty("blob2").getValue(Type.BINARY).getNewStream());
            assertEquals(blobSize, blob.length);
        } finally {
            fileStore.close();
        }
    }

    @Test
    public void offlineCompaction() throws Exception {
        SegmentGCOptions gcOptions = defaultGCOptions().setOffline();
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
        FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withMaxFileSize(1)
                .withGCOptions(gcOptions)
                .withStatisticsProvider(new DefaultStatisticsProvider(executor))
                .build();
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

        try {
            // 5MB blob
            int blobSize = 5 * 1024 * 1024;

            // Create ~2MB of data
            NodeBuilder extra = nodeStore.getRoot().builder();
            NodeBuilder content = extra.child("content");
            for (int i = 0; i < 10000; i++) {
                NodeBuilder c = content.child("c" + i);
                for (int j = 0; j < 1000; j++) {
                    c.setProperty("p" + i, "v" + i);
                }
            }
            nodeStore.merge(extra, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            long size1 = fileStore.getStats().getApproximateSize();
            log.debug("File store size {}", byteCountToDisplaySize(size1));

            // Create a property with 5 MB blob
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setProperty("blob1", createBlob(nodeStore, blobSize));
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            long size2 = fileStore.getStats().getApproximateSize();
            assertTrue("the store should grow", size2 > size1);
            assertTrue("the store should grow of at least the size of the blob", size2 - size1 > blobSize);

            // Now remove the property. No gc yet -> size doesn't shrink
            builder = nodeStore.getRoot().builder();
            builder.removeProperty("blob1");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            long size3 = fileStore.getStats().getApproximateSize();
            assertTrue("the size should grow", size3 > size2);

            // 1st gc cycle -> 1st blob should get collected
            fileStore.compact();
            fileStore.cleanup();

            long size4 = fileStore.getStats().getApproximateSize();
            assertTrue("the store should shrink", size4 < size3);
            assertTrue("the store should shrink of at least the size of the blob", size3 - size4 >= blobSize);

            // Add another 5MB binary
            builder = nodeStore.getRoot().builder();
            builder.setProperty("blob2", createBlob(nodeStore, blobSize));
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            long size5 = fileStore.getStats().getApproximateSize();
            assertTrue("the store should grow", size5 > size4);
            assertTrue("the store should grow of at least the size of the blob", size5 - size4 > blobSize);

            // 2st gc cycle -> 2nd blob should *not* be collected
            fileStore.compact();
            fileStore.cleanup();

            long size6 = fileStore.getStats().getApproximateSize();
            assertTrue("the blob should not be collected", Math.abs(size5 - size6) < blobSize);

            // 3rd gc cycle -> no significant change
            fileStore.compact();
            fileStore.cleanup();

            long size7 = fileStore.getStats().getApproximateSize();
            assertTrue("the blob should not be collected", Math.abs(size6 - size7) < blobSize);

            // No data loss
            byte[] blob = ByteStreams.toByteArray(nodeStore.getRoot()
                    .getProperty("blob2").getValue(Type.BINARY).getNewStream());
            assertEquals(blobSize, blob.length);
        } finally {
            fileStore.close();
        }
    }

    /**
     * Create a lot of data nodes (no binaries) and a few checkpoints, verify
     * that compacting checkpoints will not cause the size to explode
     */
    @Test
    public void offlineCompactionCps() throws Exception {
        SegmentGCOptions gcOptions = defaultGCOptions().setOffline();
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
        FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withMaxFileSize(1)
                .withGCOptions(gcOptions)
                .withStatisticsProvider(new DefaultStatisticsProvider(executor))
                .build();
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        try {
            // Create ~2MB of data
            NodeBuilder extra = nodeStore.getRoot().builder();
            NodeBuilder content = extra.child("content");
            for (int i = 0; i < 10000; i++) {
                NodeBuilder c = content.child("c" + i);
                for (int j = 0; j < 1000; j++) {
                    c.setProperty("p" + i, "v" + i);
                }
            }
            nodeStore.merge(extra, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();
            fileStore.compact();
            fileStore.cleanup();
            // Compacts to 548Kb
            long size0 = fileStore.getStats().getApproximateSize();

            int cpNo = 4;
            Set<String> cps = new HashSet<String>();
            for (int i = 0; i < cpNo; i++) {
                cps.add(nodeStore.checkpoint(60000));
            }
            assertEquals(cpNo, cps.size());
            for (String cp : cps) {
                assertTrue(nodeStore.retrieve(cp) != null);
            }

            long size1 = fileStore.getStats().getApproximateSize();
            assertTrue("the size should grow or stay the same", size1 >= size0);

            // TODO the following assertion doesn't say anything useful. The
            // conveyed message is "the repository can shrink, grow or stay the
            // same, as long as it remains in a 10% margin of the previous size
            // that I took out of thin air". It has to be fixed or removed.

            // fileStore.compact();
            // fileStore.cleanup();
            // long size2 = fileStore.getStats().getApproximateSize();
            // assertSize("with checkpoints compacted", size2, size1 * 9/10, size1 * 11 / 10);
        } finally {
            fileStore.close();
        }
    }

    /**
     * Create 2 binary nodes with same content but not same reference. Verify
     * de-duplication capabilities of compaction.
     */
    @Test
    public void offlineCompactionBinC1() throws Exception {
        SegmentGCOptions gcOptions = defaultGCOptions().setOffline()
                .withBinaryDeduplication();
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
        FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withMaxFileSize(1)
                .withGCOptions(gcOptions)
                .withStatisticsProvider(new DefaultStatisticsProvider(executor))
                .build();
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders
                .builder(fileStore).build();

        try {
            NodeBuilder extra = nodeStore.getRoot().builder();
            NodeBuilder content = extra.child("content");

            int blobSize = 5 * 1024 * 1024;
            byte[] data = new byte[blobSize];
            new Random().nextBytes(data);

            NodeBuilder c1 = content.child("c1");
            Blob b1 = nodeStore.createBlob(new ByteArrayInputStream(data));
            c1.setProperty("blob1", b1);
            NodeBuilder c2 = content.child("c2");
            Blob b2 = nodeStore.createBlob(new ByteArrayInputStream(data));
            c2.setProperty("blob2", b2);
            nodeStore.merge(extra, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            int cpNo = 4;
            Set<String> cps = new HashSet<String>();
            for (int i = 0; i < cpNo; i++) {
                cps.add(nodeStore.checkpoint(60000));
            }
            assertEquals(cpNo, cps.size());
            for (String cp : cps) {
                assertTrue(nodeStore.retrieve(cp) != null);
            }

            long size1 = fileStore.getStats().getApproximateSize();
            fileStore.compact();
            fileStore.cleanup();
            long size2 = fileStore.getStats().getApproximateSize();
            assertSize("with compacted binaries", size2, 0, size1 - blobSize);
        } finally {
            fileStore.close();
        }
    }

    /**
     * Create 2 binary nodes with same content but not same reference. Reduce
     * the max size if de-duplicated binaries under the binary length. Verify
     * de-duplication capabilities of compaction.
     */
    @Test
    public void offlineCompactionBinC2() throws Exception {
        int blobSize = 5 * 1024 * 1024;

        SegmentGCOptions gcOptions = defaultGCOptions().setOffline()
                .withBinaryDeduplication()
                .setBinaryDeduplicationMaxSize(blobSize / 2);
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
        FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withMaxFileSize(1)
                .withGCOptions(gcOptions)
                .withStatisticsProvider(new DefaultStatisticsProvider(executor))
                .build();
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders
                .builder(fileStore).build();

        try {
            NodeBuilder extra = nodeStore.getRoot().builder();
            NodeBuilder content = extra.child("content");

            byte[] data = new byte[blobSize];
            new Random().nextBytes(data);

            NodeBuilder c1 = content.child("c1");
            Blob b1 = nodeStore.createBlob(new ByteArrayInputStream(data));
            c1.setProperty("blob1", b1);
            NodeBuilder c2 = content.child("c2");
            Blob b2 = nodeStore.createBlob(new ByteArrayInputStream(data));
            c2.setProperty("blob2", b2);
            nodeStore.merge(extra, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            int cpNo = 4;
            Set<String> cps = new HashSet<String>();
            for (int i = 0; i < cpNo; i++) {
                cps.add(nodeStore.checkpoint(60000));
            }
            assertEquals(cpNo, cps.size());
            for (String cp : cps) {
                assertTrue(nodeStore.retrieve(cp) != null);
            }

            long size1 = fileStore.getStats().getApproximateSize();
            fileStore.compact();
            fileStore.cleanup();
            long size2 = fileStore.getStats().getApproximateSize();

            // not expected to reduce the size too much, as the binaries are
            // above the threshold
            assertSize("with compacted binaries", size2, size1 * 9 / 10,
                    size1 * 11 / 10);
        } finally {
            fileStore.close();
        }
    }

    /**
     * Create 2 binary nodes with same content and same reference. Verify
     * de-duplication capabilities of compaction
     */
    @Test
    public void offlineCompactionBinR1() throws Exception {
        SegmentGCOptions gcOptions = defaultGCOptions().setOffline();
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
        FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withMaxFileSize(1)
                .withGCOptions(gcOptions)
                .withStatisticsProvider(new DefaultStatisticsProvider(executor))
                .build();
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders
                .builder(fileStore).build();

        try {
            NodeBuilder extra = nodeStore.getRoot().builder();
            NodeBuilder content = extra.child("content");

            int blobSize = 5 * 1024 * 1024;
            byte[] data = new byte[blobSize];
            new Random().nextBytes(data);
            Blob b = nodeStore.createBlob(new ByteArrayInputStream(data));

            NodeBuilder c1 = content.child("c1");
            c1.setProperty("blob1", b);
            NodeBuilder c2 = content.child("c2");
            c2.setProperty("blob2", b);
            nodeStore.merge(extra, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            int cpNo = 4;
            Set<String> cps = new HashSet<String>();
            for (int i = 0; i < cpNo; i++) {
                cps.add(nodeStore.checkpoint(60000));
            }
            assertEquals(cpNo, cps.size());
            for (String cp : cps) {
                assertTrue(nodeStore.retrieve(cp) != null);
            }

            // 5Mb, de-duplication by the SegmentWriter
            long size1 = fileStore.getStats().getApproximateSize();
            fileStore.compact();
            fileStore.cleanup();
            long size2 = fileStore.getStats().getApproximateSize();
            assertSize("with compacted binaries", size2, 0, size1 * 11 / 10);
        } finally {
            fileStore.close();
        }
    }

    private static void assertSize(String info, long size, long lower, long upper) {
        log.debug("File Store {} size {}, expected in interval [{},{}]",
                info, size, lower, upper);
        assertTrue("File Store " + info + " size expected in interval " +
                "[" + (lower) + "," + (upper) + "] but was: " + (size),
                size >= lower && size <= (upper));
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    @Test
    public void testCancelCompaction()
    throws Throwable {
        final FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withGCOptions(defaultGCOptions().setRetainedGenerations(2))
                .withMaxFileSize(1)
                .build();
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

        NodeBuilder builder = nodeStore.getRoot().builder();
        addNodes(builder, 10);
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fileStore.flush();

        FutureTask<Boolean> async = runAsync(new Callable<Boolean>() {
            @Override
            public Boolean call() throws IOException {
                boolean cancelled = false;
                for (int k = 0; !cancelled && k < 1000; k++) {
                    cancelled = !fileStore.compact();
                }
                return cancelled;
            }
        });

        // Give the compaction thread a head start
        sleepUninterruptibly(1, SECONDS);

        fileStore.close();
        try {
            assertTrue(async.get());
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof IllegalStateException)) {
                // Throw cause unless this is an ISE thrown by the
                // store being already closed, which is kinda expected
                throw e.getCause();
            }
        }
    }

    private static void addNodes(NodeBuilder builder, int depth) {
        if (depth > 0) {
            NodeBuilder child1 = builder.setChildNode("1");
            addNodes(child1, depth - 1);
            NodeBuilder child2 = builder.setChildNode("2");
            addNodes(child2, depth - 1);
        }
    }

    /**
     * Regression test for OAK-2192 testing for mixed segments. This test does not
     * cover OAK-3348. I.e. it does not assert the segment graph is free of cross
     * gc generation references.
     */
    @Test
    public void testMixedSegments() throws Exception {
        FileStore store = fileStoreBuilder(getFileStoreFolder())
                .withMaxFileSize(2)
                .withMemoryMapping(true)
                .build();
        final SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(store).build();
        final AtomicBoolean compactionSuccess = new AtomicBoolean(true);

        NodeBuilder root = nodeStore.getRoot().builder();
        createNodes(root.setChildNode("test"), 10, 3);
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final Set<UUID> beforeSegments = new HashSet<UUID>();
        collectSegments(store.getReader(), store.getRevisions(), beforeSegments);

        final AtomicReference<Boolean> run = new AtomicReference<Boolean>(true);
        final List<String> failedCommits = newArrayList();
        Thread[] threads = new Thread[10];
        for (int k = 0; k < threads.length; k++) {
            final int threadId = k;
            threads[k] = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0; run.get(); j++) {
                        String nodeName = "b-" + threadId + "," + j;
                        try {
                            NodeBuilder root = nodeStore.getRoot().builder();
                            root.setChildNode(nodeName);
                            nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                            Thread.sleep(5);
                        } catch (CommitFailedException e) {
                            failedCommits.add(nodeName);
                        } catch (InterruptedException e) {
                            Thread.interrupted();
                            break;
                        }
                    }
                }
            });
            threads[k].start();
        }
        store.compact();
        run.set(false);
        for (Thread t : threads) {
            t.join();
        }
        store.flush();

        assumeTrue("Failed to acquire compaction lock", compactionSuccess.get());
        assertTrue("Failed commits: " + failedCommits, failedCommits.isEmpty());

        Set<UUID> afterSegments = new HashSet<UUID>();
        collectSegments(store.getReader(), store.getRevisions(), afterSegments);
        try {
            for (UUID u : beforeSegments) {
                assertFalse("Mixed segments found: " + u, afterSegments.contains(u));
            }
        } finally {
            store.close();
        }
    }

    /**
     * Set a root node referring to a child node that lives in a different segments. Depending
     * on the order how the SegmentBufferWriters associated with the threads used to create the
     * nodes are flushed, this will introduce a forward reference between the segments.
     * The current cleanup mechanism cannot handle forward references and removes the referenced
     * segment causing a SNFE.
     * This is a regression introduced with OAK-1828.
     */
    @Test
    public void cleanupCyclicGraph() throws Exception {
        FileStore fileStore = fileStoreBuilder(getFileStoreFolder()).build();
        final SegmentWriter writer = fileStore.getWriter();
        final SegmentNodeState oldHead = fileStore.getHead();

        final SegmentNodeState child = run(new Callable<SegmentNodeState>() {
            @Override
            public SegmentNodeState call() throws Exception {
                NodeBuilder builder = EMPTY_NODE.builder();
                return writer.writeNode(EMPTY_NODE);
            }
        });
        SegmentNodeState newHead = run(new Callable<SegmentNodeState>() {
            @Override
            public SegmentNodeState call() throws Exception {
                NodeBuilder builder = oldHead.builder();
                builder.setChildNode("child", child);
                return writer.writeNode(builder.getNodeState());
            }
        });

        writer.flush();
        fileStore.getRevisions().setHead(oldHead.getRecordId(), newHead.getRecordId());
        fileStore.close();

        fileStore = fileStoreBuilder(getFileStoreFolder()).build();

        traverse(fileStore.getHead());
        fileStore.cleanup();

        // Traversal after cleanup might result in an SNFE
        traverse(fileStore.getHead());

        fileStore.close();
    }

    private static void traverse(NodeState node) {
        for (ChildNodeEntry childNodeEntry : node.getChildNodeEntries()) {
            traverse(childNodeEntry.getNodeState());
        }
    }

    private static <T> T run(Callable<T> callable) throws InterruptedException, ExecutionException {
        FutureTask<T> task = new FutureTask<T>(callable);
        new Thread(task).start();
        return task.get();
    }

    private static <T> FutureTask<T> runAsync(Callable<T> callable) {
        FutureTask<T> task = new FutureTask<T>(callable);
        new Thread(task).start();
        return task;
    }

    /**
     * Test asserting OAK-3348: Cross gc sessions might introduce references to pre-compacted segments
     */
    @Test
    public void preCompactionReferences() throws Exception {
        for (String ref : new String[] {"merge-before-compact", "merge-after-compact"}) {
            File repoDir = new File(getFileStoreFolder(), ref);
            FileStore fileStore = fileStoreBuilder(repoDir)
                    .withMaxFileSize(2)
                    .withGCOptions(defaultGCOptions())
                    .build();
            final SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            try {
                // add some content
                NodeBuilder preGCBuilder = nodeStore.getRoot().builder();
                preGCBuilder.setChildNode("test").setProperty("blob", createBlob(nodeStore, 1024 * 1024));
                nodeStore.merge(preGCBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

                // remove it again so we have something to gc
                preGCBuilder = nodeStore.getRoot().builder();
                preGCBuilder.getChildNode("test").remove();
                nodeStore.merge(preGCBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

                // with a new builder simulate exceeding the update limit.
                // This will cause changes to be pre-written to segments
                preGCBuilder = nodeStore.getRoot().builder();
                preGCBuilder.setChildNode("test").setChildNode("a").setChildNode("b").setProperty("foo", "bar");
                for (int k = 0; k < getInteger("update.limit", 10000); k += 2) {
                    preGCBuilder.setChildNode("dummy").remove();
                }

                // case 1: merge above changes before compact
                if ("merge-before-compact".equals(ref)) {
                    NodeBuilder builder = nodeStore.getRoot().builder();
                    builder.setChildNode("n");
                    nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    nodeStore.merge(preGCBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                }

                // Ensure cleanup is efficient by surpassing the number of
                // retained generations
                for (int k = 0; k < defaultGCOptions().getRetainedGenerations(); k++) {
                    fileStore.compact();
                }

                // case 2: merge above changes after compact
                if ("merge-after-compact".equals(ref)) {
                    NodeBuilder builder = nodeStore.getRoot().builder();
                    builder.setChildNode("n");
                    nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    nodeStore.merge(preGCBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                }
            } finally {
                fileStore.close();
            }

            // Re-initialise the file store to simulate off-line gc
            fileStore = fileStoreBuilder(repoDir).withMaxFileSize(2).build();
            try {
                // The 1M blob should get gc-ed
                fileStore.cleanup();
                assertTrue(ref + " repository size " + fileStore.getStats().getApproximateSize() + " < " + 1024 * 1024,
                        fileStore.getStats().getApproximateSize() < 1024 * 1024);
            } finally {
                fileStore.close();
            }
        }
    }

    private static void collectSegments(SegmentReader reader, Revisions revisions,
                                        final Set<UUID> segmentIds) {
        new SegmentParser(reader) {
            @Override
            protected void onNode(RecordId parentId, RecordId nodeId) {
                super.onNode(parentId, nodeId);
                segmentIds.add(nodeId.asUUID());
            }

            @Override
            protected void onTemplate(RecordId parentId, RecordId templateId) {
                super.onTemplate(parentId, templateId);
                segmentIds.add(templateId.asUUID());
            }

            @Override
            protected void onMap(RecordId parentId, RecordId mapId, MapRecord map) {
                super.onMap(parentId, mapId, map);
                segmentIds.add(mapId.asUUID());
            }

            @Override
            protected void onMapDiff(RecordId parentId, RecordId mapId, MapRecord map) {
                super.onMapDiff(parentId, mapId, map);
                segmentIds.add(mapId.asUUID());
            }

            @Override
            protected void onMapLeaf(RecordId parentId, RecordId mapId, MapRecord map) {
                super.onMapLeaf(parentId, mapId, map);
                segmentIds.add(mapId.asUUID());
            }

            @Override
            protected void onMapBranch(RecordId parentId, RecordId mapId, MapRecord map) {
                super.onMapBranch(parentId, mapId, map);
                segmentIds.add(mapId.asUUID());
            }

            @Override
            protected void onProperty(RecordId parentId, RecordId propertyId, PropertyTemplate template) {
                super.onProperty(parentId, propertyId, template);
                segmentIds.add(propertyId.asUUID());
            }

            @Override
            protected void onValue(RecordId parentId, RecordId valueId, Type<?> type) {
                super.onValue(parentId, valueId, type);
                segmentIds.add(valueId.asUUID());
            }

            @Override
            protected void onBlob(RecordId parentId, RecordId blobId) {
                super.onBlob(parentId, blobId);
                segmentIds.add(blobId.asUUID());
            }

            @Override
            protected void onString(RecordId parentId, RecordId stringId) {
                super.onString(parentId, stringId);
                segmentIds.add(stringId.asUUID());
            }

            @Override
            protected void onList(RecordId parentId, RecordId listId, int count) {
                super.onList(parentId, listId, count);
                segmentIds.add(listId.asUUID());
            }

            @Override
            protected void onListBucket(RecordId parentId, RecordId listId, int index, int count, int capacity) {
                super.onListBucket(parentId, listId, index, count, capacity);
                segmentIds.add(listId.asUUID());
            }
        }.parseNode(revisions.getHead());
    }

    private static void createNodes(NodeBuilder builder, int count, int depth) {
        if (depth > 0) {
            for (int k = 0; k < count; k++) {
                NodeBuilder child = builder.setChildNode("node" + k);
                createProperties(child, count);
                createNodes(child, count, depth - 1);
            }
        }
    }

    private static void createProperties(NodeBuilder builder, int count) {
        for (int k = 0; k < count; k++) {
            builder.setProperty("property-" + UUID.randomUUID().toString(), "value-" + UUID.randomUUID().toString());
        }
    }

    @Test
    public void propertyRetention() throws Exception {
        SegmentGCOptions gcOptions = defaultGCOptions();
        FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withMaxFileSize(1)
                .withGCOptions(gcOptions)
                .build();
        try {
            final SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

            // Add a property
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setChildNode("test").setProperty("property", "value");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // Segment id of the current segment
            NodeState test = nodeStore.getRoot().getChildNode("test");
            SegmentId id = ((SegmentNodeState) test).getRecordId().getSegmentId();
            fileStore.flush();
            assertTrue(fileStore.containsSegment(id));

            // Add enough content to fill up the current tar file
            builder = nodeStore.getRoot().builder();
            addContent(builder.setChildNode("dump"));
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // Segment and property still there
            assertTrue(fileStore.containsSegment(id));
            PropertyState property = test.getProperty("property");
            assertEquals("value", property.getValue(STRING));

            // GC should remove the segment
            fileStore.flush();
            // Ensure cleanup is efficient by surpassing the number of
            // retained generations
            for (int k = 0; k < gcOptions.getRetainedGenerations(); k++) {
                fileStore.compact();
            }
            fileStore.cleanup();

            try {
                fileStore.readSegment(id);
                fail("Segment " + id + " should be gc'ed");
            } catch (SegmentNotFoundException ignore) {}
        } finally {
            fileStore.close();
        }
    }

    @Test
    public void checkpointDeduplicationTest() throws Exception {
        FileStore fileStore = fileStoreBuilder(getFileStoreFolder()).build();
        try {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setChildNode("a").setChildNode("aa");
            builder.setChildNode("b").setChildNode("bb");
            builder.setChildNode("c").setChildNode("cc");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            String cpId = nodeStore.checkpoint(Long.MAX_VALUE);

            NodeState uncompacted = nodeStore.getRoot();
            fileStore.compact();
            NodeState compacted = nodeStore.getRoot();

            assertEquals(uncompacted, compacted);
            assertTrue(uncompacted instanceof SegmentNodeState);
            assertTrue(compacted instanceof SegmentNodeState);
            assertEquals(((SegmentNodeState)uncompacted).getStableId(), ((SegmentNodeState)compacted).getStableId());

            NodeState checkpoint = nodeStore.retrieve(cpId);
            assertTrue(checkpoint instanceof SegmentNodeState);
            assertEquals("Checkpoint should get de-duplicated",
                ((Record) compacted).getRecordId(), ((Record) checkpoint).getRecordId());
        } finally {
            fileStore.close();
        }
    }

    /**
     * Test asserting OAK-4669: No new generation of tar should be created when the segments are the same 
     * and when various indices are created. 
     */
    @Test
    public void concurrentWritesCleanupNoNewGen() throws Exception {
        ScheduledExecutorService scheduler = newSingleThreadScheduledExecutor();
        StatisticsProvider statsProvider = new DefaultStatisticsProvider(scheduler);
        final FileStoreGCMonitor fileStoreGCMonitor = new FileStoreGCMonitor(Clock.SIMPLE);
        
        File fileStoreFolder = getFileStoreFolder();

        final FileStore fileStore = fileStoreBuilder(fileStoreFolder)
                .withGCOptions(defaultGCOptions().setRetainedGenerations(2))
                .withGCMonitor(fileStoreGCMonitor)
                .withStatisticsProvider(statsProvider)
                .withMaxFileSize(1)
                .withMemoryMapping(false)
                .build();
        
        final SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        ExecutorService executorService = newFixedThreadPool(5);
        final AtomicInteger counter = new AtomicInteger();

        try {
            Callable<Void> concurrentWriteTask = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    NodeBuilder builder = nodeStore.getRoot().builder();
                    builder.setProperty("blob-" + counter.getAndIncrement(), createBlob(nodeStore, 512 * 512));
                    nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    fileStore.flush();
                    return null;
                }
            };

            List<Future<?>> results = newArrayList();
            for (int i = 0; i < 5; i++) {
                results.add(executorService.submit(concurrentWriteTask));
            }

            for (Future<?> result : results) {
                assertNull(result.get());
            }

            fileStore.cleanup();

            for (String fileName : fileStoreFolder.list()) {
                if (fileName.endsWith(".tar")) {
                    int pos = fileName.length() - "a.tar".length();
                    char generation = fileName.charAt(pos);
                    assertTrue("Expected generation is 'a', but instead was: '" + generation + "' for file " + fileName,
                            generation == 'a');
                }
            }
        } finally {
            new ExecutorCloser(executorService).close();
            fileStore.close();
            new ExecutorCloser(scheduler).close();
        }
    }

    @Test
    public void concurrentWritesCleanupZeroReclaimedSize() throws Exception {
        ScheduledExecutorService scheduler = newSingleThreadScheduledExecutor();
        StatisticsProvider statsProvider = new DefaultStatisticsProvider(scheduler);
        final FileStoreGCMonitor fileStoreGCMonitor = new FileStoreGCMonitor(Clock.SIMPLE);

        final FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withGCOptions(defaultGCOptions().setRetainedGenerations(2))
                .withGCMonitor(fileStoreGCMonitor)
                .withStatisticsProvider(statsProvider)
                .withMaxFileSize(1)
                .withMemoryMapping(false)
                .build();
        
        final SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        ExecutorService executorService = newFixedThreadPool(100);
        final AtomicInteger counter = new AtomicInteger();
        
        try {
            Callable<Void> concurrentWriteTask = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    NodeBuilder builder = nodeStore.getRoot().builder();
                    builder.setProperty("blob-" + counter.getAndIncrement(), createBlob(nodeStore, 25 * 25));
                    nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    fileStore.flush();
                    return null;
                }
            };

            List<Future<?>> results = newArrayList();
            for (int i = 0; i < 100; i++) {
                results.add(executorService.submit(concurrentWriteTask));
            }

            Thread.sleep(100);
            fileStore.cleanup();

            for (Future<?> result : results) {
                assertNull(result.get());
            }

            long reclaimedSize = fileStoreGCMonitor.getLastReclaimedSize();
            assertEquals("Reclaimed size expected is 0, but instead was: " + reclaimedSize,
                    0, reclaimedSize);
        } finally {
            new ExecutorCloser(executorService).close();
            fileStore.close();
            new ExecutorCloser(scheduler).close();
        }
    }
    
    @Test
    public void randomAccessFileConcurrentReadAndLength() throws Exception {
        final FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withGCOptions(defaultGCOptions().setRetainedGenerations(2))
                .withMaxFileSize(1)
                .withMemoryMapping(false)
                .build();
        
        final SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        ExecutorService executorService = newFixedThreadPool(300);
        final AtomicInteger counter = new AtomicInteger();
        final ReferenceCollector dummyCollector = new ReferenceCollector() {
            
            @Override
            public void addReference(String reference, String nodeId) {
                // do nothing
            }
        };
        
        try {
            Callable<Void> concurrentWriteTask = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    NodeBuilder builder = nodeStore.getRoot().builder();
                    builder.setProperty("blob-" + counter.getAndIncrement(), createBlob(nodeStore, 25 * 25));
                    nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    fileStore.flush();
                    return null;
                }
            };
            
            Callable<Void> concurrentCleanupTask = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    fileStore.cleanup();
                    return null;
                }
            };
            
            Callable<Void> concurrentReferenceCollector = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    fileStore.collectBlobReferences(dummyCollector);
                    return null;
                }
            };

            List<Future<?>> results = newArrayList();
            for (int i = 0; i < 100; i++) {
                results.add(executorService.submit(concurrentWriteTask));
                results.add(executorService.submit(concurrentCleanupTask));
                results.add(executorService.submit(concurrentReferenceCollector));
            }

            for (Future<?> result : results) {
                assertNull(result.get());
            }

        } finally {
            new ExecutorCloser(executorService).close();
            fileStore.close();
        }
    }

    /**
     * Test asserting OAK-4700: Concurrent cleanup must not remove segments that are still reachable
     */
    @Test
    public void concurrentCleanup() throws Exception {
        File fileStoreFolder = getFileStoreFolder();

        final FileStore fileStore = fileStoreBuilder(fileStoreFolder)
                .withMaxFileSize(1)
                .build();

        final SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        ExecutorService executorService = newFixedThreadPool(50);
        final AtomicInteger counter = new AtomicInteger();

        try {
            Callable<Void> concurrentWriteTask = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    NodeBuilder builder = nodeStore.getRoot().builder();
                    builder.setProperty("blob-" + counter.getAndIncrement(), createBlob(nodeStore, 512 * 512));
                    nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    fileStore.flush();
                    return null;
                }
            };

            final Callable<Void> concurrentCleanTask = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    fileStore.cleanup();
                    return null;
                }
            };

            List<Future<?>> results = newArrayList();
            for (int i = 0; i < 50; i++) {
                if (i % 2 == 0) {
                    results.add(executorService.submit(concurrentWriteTask));
                } else {
                    results.add(executorService.submit(concurrentCleanTask));
                }
            }

            for (Future<?> result : results) {
                assertNull(result.get());
            }

            for (String fileName : fileStoreFolder.list()) {
                if (fileName.endsWith(".tar")) {
                    int pos = fileName.length() - "a.tar".length();
                    char generation = fileName.charAt(pos);
                    assertEquals("Expected nothing to be cleaned but generation '" + generation +
                        "' for file " + fileName + " indicates otherwise.",
                        "a", valueOf(generation));
                }
            }
        } finally {
            new ExecutorCloser(executorService).close();
            fileStore.close();
        }
    }

    private static void addContent(NodeBuilder builder) {
        for (int k = 0; k < 10000; k++) {
            builder.setProperty(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
    }
}
