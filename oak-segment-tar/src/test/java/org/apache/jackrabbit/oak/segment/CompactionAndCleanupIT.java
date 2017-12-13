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
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.Integer.getInteger;
import static java.lang.String.valueOf;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.Compactor.UPDATE_LIMIT;
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
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreGCMonitor;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.tool.Compact;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.BeforeClass;
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

    @BeforeClass
    public static void init() {
        // Allow running gc without backoff. Needed for testCancelCompactionSNFE.
        // See FileStore.GC_BACKOFF.
        System.setProperty("oak.gc.backoff", "0");
    }

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    @Test
    public void compactPersistsHead() throws Exception {
        FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withGCOptions(defaultGCOptions().setRetainedGenerations(2))
                .withMaxFileSize(1)
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

            fileStore.compactFull();
            assertEquals(fileStore.getRevisions().getHead(), fileStore.getRevisions().getPersistedHead());
        } finally {
            fileStore.close();
        }
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
            fileStore.compactFull();
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
            fileStore.compactFull();
            fileStore.cleanup();

            long size6 = fileStore.getStats().getApproximateSize();
            assertTrue("the store should shrink", size6 < size5);
            assertTrue("the store should shrink of at least the size of the blob", size5 - size6 >= blobSize);

            // 3rtd gc cycle -> no  significant change
            fileStore.compactFull();
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
            fileStore.compactFull();
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
            fileStore.compactFull();
            fileStore.cleanup();

            long size6 = fileStore.getStats().getApproximateSize();
            assertTrue("the blob should not be collected", size6 > blobSize);

            // 3rd gc cycle -> no significant change
            fileStore.compactFull();
            fileStore.cleanup();

            long size7 = fileStore.getStats().getApproximateSize();
            assertTrue("the blob should not be collected", size7 > blobSize);

            // No data loss
            byte[] blob = ByteStreams.toByteArray(nodeStore.getRoot()
                    .getProperty("blob2").getValue(Type.BINARY).getNewStream());
            assertEquals(blobSize, blob.length);
        } finally {
            fileStore.close();
        }
    }

    @Test
    public void cancelOfflineCompaction() throws Exception {
        final AtomicBoolean cancelCompaction = new AtomicBoolean(true);
        try (FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withGCOptions(defaultGCOptions().setOffline())
                .build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
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
            NodeState uncompactedRoot = nodeStore.getRoot();

            // Keep cancelling compaction
            new Thread(() -> {
                while (cancelCompaction.get()) {
                    fileStore.cancelGC();
                }
            }).start();

            fileStore.compactFull();

            // Cancelling compaction must not corrupt the repository. See OAK-7050.
            NodeState compactedRoot = nodeStore.getRoot();
            assertTrue(compactedRoot.exists());
            assertEquals(uncompactedRoot, compactedRoot);
        } finally {
            cancelCompaction.set(false);
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
            fileStore.compactFull();
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

    @Test
    public void equalContentAfterOC() throws Exception {
        SegmentGCOptions gcOptions = defaultGCOptions().setOffline();
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor();

        try (FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withGCOptions(gcOptions)
                .build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

            // Add initial content
            NodeBuilder rootBuilder = nodeStore.getRoot().builder();
            addNodes(rootBuilder, 8, "p");
            addProperties(rootBuilder, 3);
            nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            NodeState initialRoot = nodeStore.getRoot();
            assertTrue(fileStore.compactFull());
            NodeState compactedRoot = nodeStore.getRoot();

            assertTrue(initialRoot != compactedRoot);
            assertEquals(initialRoot, compactedRoot);
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
            fileStore.compactFull();
            fileStore.cleanup();
            long size2 = fileStore.getStats().getApproximateSize();
            assertSize("with compacted binaries", size2, 0, size1 * 11 / 10);
        } finally {
            fileStore.close();
        }
    }

    /**
     * Test for the Offline compaction tool (OAK-5971)
     */
    @Test
    public void offlineCompactionTool() throws Exception {
        SegmentGCOptions gcOptions = defaultGCOptions().setOffline();
        ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
        FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withMaxFileSize(1)
                .withGCOptions(gcOptions)
                .withStatisticsProvider(new DefaultStatisticsProvider(executor))
                .build();
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        try {
            NodeBuilder root = nodeStore.getRoot().builder();
            root.child("content");
            nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();
        } finally {
            fileStore.close();
        }

        Compact.builder().withPath(getFileStoreFolder()).build().run();

        fileStore = fileStoreBuilder(getFileStoreFolder())
                .withMaxFileSize(1)
                .withGCOptions(gcOptions)
                .withStatisticsProvider(new DefaultStatisticsProvider(executor))
                .build();
        nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        try {
            assertTrue(nodeStore.getRoot().hasChildNode("content"));
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
        addNodes(builder, 10, "");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fileStore.flush();

        FutureTask<Boolean> async = runAsync(new Callable<Boolean>() {
            @Override
            public Boolean call() throws IOException {
                boolean cancelled = false;
                for (int k = 0; !cancelled && k < 1000; k++) {
                    cancelled = !fileStore.compactFull();
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

    /**
     * See OAK-5517: SNFE when running compaction after a cancelled gc
     */
    @Test
    public void testCancelCompactionSNFE()
    throws Throwable {
        final FileStore fileStore = fileStoreBuilder(getFileStoreFolder())
                .withGCOptions(defaultGCOptions()
                        .setRetainedGenerations(2)
                        .setEstimationDisabled(true))
                .withMaxFileSize(1)
                .build();
        try {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

            final Callable<Void> cancel = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    // Give the compaction thread a head start
                    sleepUninterruptibly(1000, MILLISECONDS);
                    fileStore.cancelGC();
                    return null;
                }
            };

            for (int k = 0; k < 100; k++) {
                NodeBuilder builder = nodeStore.getRoot().builder();
                addNodes(builder, 10, k + "-");
                nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                fileStore.flush();

                // Cancelling gc should not cause a SNFE on subsequent gc runs
                runAsync(cancel);
                fileStore.fullGC();
            }
        } finally {
            fileStore.close();
        }
    }

    private static void addNodes(NodeBuilder builder, int depth, String prefix) {
        if (depth > 0) {
            NodeBuilder child1 = builder.setChildNode(prefix + "1");
            addNodes(child1, depth - 1, prefix);
            NodeBuilder child2 = builder.setChildNode(prefix + "2");
            addNodes(child2, depth - 1, prefix);
        }
    }

    private static void addProperties(NodeBuilder builder, int count) {
        for (int c = 0; c < count; c++) {
            builder.setProperty("p-" + c, "v-" + c);
        }
        for (String child : builder.getChildNodeNames()) {
            addProperties(builder.getChildNode(child), count);
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
        store.compactFull();
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
        final SegmentReader reader = fileStore.getReader();
        final SegmentWriter writer = fileStore.getWriter();
        final BlobStore blobStore = fileStore.getBlobStore();
        final SegmentNodeState oldHead = fileStore.getHead();

        final SegmentNodeState child = run(new Callable<SegmentNodeState>() {
            @Override
            public SegmentNodeState call() throws Exception {
                NodeBuilder builder = EMPTY_NODE.builder();
                return new SegmentNodeState(reader, writer, blobStore, writer.writeNode(EMPTY_NODE));
            }
        });
        SegmentNodeState newHead = run(new Callable<SegmentNodeState>() {
            @Override
            public SegmentNodeState call() throws Exception {
                NodeBuilder builder = oldHead.builder();
                builder.setChildNode("child", child);
                return new SegmentNodeState(reader, writer, blobStore, writer.writeNode(builder.getNodeState()));
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
                    fileStore.compactFull();
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
                fileStore.compactFull();
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
        class CP {
            String id;
            NodeState uncompacted;
            NodeState compacted;
        }
        CP[] cps = {new CP(), new CP(), new CP(), new CP()};

        try (FileStore fileStore = fileStoreBuilder(getFileStoreFolder()).build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

            // Initial content and checkpoint
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setChildNode("a").setChildNode("aa");
            builder.setChildNode("b").setChildNode("bb");
            builder.setChildNode("c").setChildNode("cc");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            cps[0].id = nodeStore.checkpoint(Long.MAX_VALUE);

            // Add content and another checkpoint
            builder = nodeStore.getRoot().builder();
            builder.setChildNode("1").setChildNode("11");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            cps[1].id = nodeStore.checkpoint(Long.MAX_VALUE);

            // Modify content and another checkpoint
            builder = nodeStore.getRoot().builder();
            builder.getChildNode("a").getChildNode("aa").setChildNode("aaa");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            cps[2].id = nodeStore.checkpoint(Long.MAX_VALUE);

            // Remove content and another checkpoint
            builder = nodeStore.getRoot().builder();
            builder.getChildNode("a").remove();
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            cps[3].id = nodeStore.checkpoint(Long.MAX_VALUE);

            // A final bit of content
            builder = nodeStore.getRoot().builder();
            builder.setChildNode("d").setChildNode("dd");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            NodeState uncompactedSuperRoot = fileStore.getHead();
            NodeState uncompactedRoot = nodeStore.getRoot();
            for (CP cp : cps) {
                cp.uncompacted = nodeStore.retrieve(cp.id);
            }

            fileStore.compactFull();

            NodeState compactedSuperRoot = fileStore.getHead();
            NodeState compactedRoot = nodeStore.getRoot();
            for (CP cp : cps) {
                cp.compacted = nodeStore.retrieve(cp.id);
            }

            assertEquals(uncompactedSuperRoot, compactedSuperRoot);

            assertEquals(uncompactedRoot, compactedRoot);
            assertStableIds(uncompactedRoot, compactedRoot, "/root");

            for (CP cp : cps) {
                assertEquals(cp.uncompacted, cp.compacted);
                assertStableIds(cp.uncompacted, cp.compacted, concat("/root/checkpoints", cp.id));
            }
        }
    }

    @Test
    public void keepStableIdOnFlush() throws Exception {
        try (FileStore fileStore = fileStoreBuilder(getFileStoreFolder()).build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

            // Initial content and checkpoint
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setChildNode("a");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            nodeStore.checkpoint(Long.MAX_VALUE);

            // A final bit of content
            builder = nodeStore.getRoot().builder();
            for (int k = 0; k < UPDATE_LIMIT; k++) {
                builder.setChildNode("b-" + k);
            }
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            NodeState uncompactedSuperRoot = fileStore.getHead();
            NodeState uncompactedRoot = nodeStore.getRoot();

            fileStore.compactFull();

            NodeState compactedSuperRoot = fileStore.getHead();
            NodeState compactedRoot = nodeStore.getRoot();

            assertEquals(uncompactedSuperRoot, compactedSuperRoot);

            assertEquals(uncompactedRoot, compactedRoot);
            assertStableIds(uncompactedRoot, compactedRoot, "/root");
        }
    }

    @Test
    public void crossGCDeduplicationTest() throws Exception {
        try (FileStore fileStore = fileStoreBuilder(getFileStoreFolder()).build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setChildNode("a").setChildNode("aa");
            builder.setChildNode("b").setChildNode("bb");
            builder.setChildNode("c").setChildNode("cc");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            NodeState a = nodeStore.getRoot().getChildNode("a");

            builder = nodeStore.getRoot().builder();
            builder.setChildNode("x").setChildNode("xx");

            SegmentNodeState uncompacted = (SegmentNodeState) nodeStore.getRoot();
            fileStore.compactFull();
            NodeState compacted = nodeStore.getRoot();

            assertEquals(uncompacted, compacted);
            assertStableIds(uncompacted, compacted, "/root");

            builder.setChildNode("y").setChildNode("yy");
            builder.getChildNode("a").remove();
            NodeState deferCompacted = nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            assertEquals(
                    uncompacted.getSegment().getGcGeneration().nextFull().nonGC(),
                    ((SegmentNodeState)deferCompacted).getSegment().getGcGeneration());
        }
    }

    private static void assertStableIds(NodeState node1, NodeState node2, String path) {
        assertFalse("Nodes should be equal: " + path, node1 == node2);
        assertTrue("Node should be a SegmentNodeState " + path, node1 instanceof SegmentNodeState);
        assertTrue("Node should be a SegmentNodeState " + path, node2 instanceof SegmentNodeState);
        SegmentNodeState sns1 = (SegmentNodeState) node1;
        SegmentNodeState sns2 = (SegmentNodeState) node2;
        assertEquals("GC generation should be bumped by one " + path,
                sns1.getSegment().getGcGeneration().nextFull(), sns2.getSegment().getGcGeneration());
        assertEquals("Nodes should have same stable id: " + path,
                sns1.getStableId(), sns2.getStableId());

        for (ChildNodeEntry cne : node1.getChildNodeEntries()) {
            assertStableIds(
                    cne.getNodeState(), node2.getChildNode(cne.getName()),
                    concat(path, cne.getName()));
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
                    fileStore.collectBlobReferences(s -> {
                        // Do nothing.
                    });
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

    private static BlobStore newBlobStore(File directory) {
        OakFileDataStore delegate = new OakFileDataStore();
        delegate.setPath(directory.getAbsolutePath());
        delegate.init(null);
        return new DataStoreBlobStore(delegate);
    }


    @Test
    public void binaryRetentionWithDS()
    throws IOException, InvalidFileStoreVersionException, CommitFailedException {
        try (FileStore fileStore = fileStoreBuilder(new File(getFileStoreFolder(), "segmentstore"))
                .withBlobStore(newBlobStore(new File(getFileStoreFolder(), "blobstore")))
                .withGCOptions(defaultGCOptions().setGcSizeDeltaEstimation(0))
                .build())
        {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setProperty("bin", createBlob(nodeStore, 1000000));
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            Set<String> expectedReferences = newHashSet();
            fileStore.collectBlobReferences(expectedReferences::add);

            for(int k = 1; k <= 3; k++) {
                fileStore.fullGC();
                Set<String> actualReferences = newHashSet();
                fileStore.collectBlobReferences(actualReferences::add);
                assertEquals("Binary should be retained after " + k + "-th gc cycle",
                        expectedReferences, actualReferences);
            }
        }
    }

    @Test
    public void latestFullCompactedStateShouldNotBeDeleted() throws Exception {
        SegmentGCOptions gcOptions = defaultGCOptions()
                .setEstimationDisabled(true)
                .setRetainedGenerations(2);

        try (FileStore fileStore = fileStoreBuilder(getFileStoreFolder()).withGCOptions(gcOptions).build()) {

            // Create a full, self consistent head state. This state will be the
            // base for the following tail compactions. This increments the
            // full generation.

            fileStore.fullGC();
            traverse(fileStore.getHead());

            // Create a tail head state on top of the previous full state. This
            // increments the generation, but leaves the full generation
            // untouched.

            fileStore.tailGC();
            traverse(fileStore.getHead());

            // Create a tail state on top of the previous tail state. This
            // increments the generation, but leaves the full generation
            // untouched. This brings this generations two generations away from
            // the latest full head state. Still, the full head state will not
            // be deleted because doing so would generate an invalid repository
            // at risk of SegmentNotFoundException.

            fileStore.tailGC();
            traverse(fileStore.getHead());
        }
    }

}
