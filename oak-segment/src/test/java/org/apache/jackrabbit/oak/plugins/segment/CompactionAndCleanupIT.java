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

package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.Integer.getInteger;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture.SEGMENT_MK;
import static org.apache.jackrabbit.oak.commons.FixturesHelper.getFixtures;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore.builder;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType.CLEAN_ALL;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType.CLEAN_NONE;
import static org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy.CleanupType.CLEAN_OLD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Ignore;
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

    public static void assumptions() {
        assumeTrue(getFixtures().contains(SEGMENT_MK));
    }
    
    @Test
    public void compactionNoBinaryClone() throws Exception {
        // 2MB data, 5MB blob
        final int blobSize = 5 * 1024 * 1024;
        final int dataNodes = 10000;

        // really long time span, no binary cloning

        FileStore fileStore = FileStore.builder(getFileStoreFolder())
                .withMaxFileSize(1)
                .build();
        final SegmentNodeStore nodeStore = SegmentNodeStore.builder(fileStore).build();
        CompactionStrategy custom = new CompactionStrategy(false, false,
                CLEAN_OLD, TimeUnit.HOURS.toMillis(1), (byte) 0) {
            @Override
            public boolean compacted(@Nonnull Callable<Boolean> setHead)
                    throws Exception {
                return nodeStore.locked(setHead);
            }
        };
        // Use in memory compaction map as gains asserted later on
        // do not take additional space of the compaction map into consideration
        custom.setPersistCompactionMap(false);
        fileStore.setCompactionStrategy(custom);

        // 1a. Create a bunch of data
        NodeBuilder extra = nodeStore.getRoot().builder();
        NodeBuilder content = extra.child("content");
        for (int i = 0; i < dataNodes; i++) {
            NodeBuilder c = content.child("c" + i);
            for (int j = 0; j < 1000; j++) {
                c.setProperty("p" + i, "v" + i);
            }
        }
        nodeStore.merge(extra, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // ----

        final long dataSize = fileStore.size();
        log.debug("File store dataSize {}", byteCountToDisplaySize(dataSize));

        try {
            // 1. Create a property with 5 MB blob
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setProperty("a1", createBlob(nodeStore, blobSize));
            builder.setProperty("b", "foo");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            log.debug("File store pre removal {}, expecting {}",
                    byteCountToDisplaySize(fileStore.size()),
                    byteCountToDisplaySize(blobSize + dataSize));
            assertEquals(mb(blobSize + dataSize), mb(fileStore.size()));

            // 2. Now remove the property
            builder = nodeStore.getRoot().builder();
            builder.removeProperty("a1");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // Size remains same, no cleanup happened yet
            log.debug("File store pre compaction {}, expecting {}",
                    byteCountToDisplaySize(fileStore.size()),
                    byteCountToDisplaySize(blobSize + dataSize));
            assertEquals(mb(blobSize + dataSize), mb(fileStore.size()));

            // 3. Compact
            assertTrue(fileStore.maybeCompact(false));

            // Size doesn't shrink: ran compaction with a '1 Hour' cleanup
            // strategy
            assertSize("post compaction", fileStore.size(),
                    blobSize + dataSize, blobSize + 2 * dataSize);

            // 4. Add some more property to flush the current TarWriter
            builder = nodeStore.getRoot().builder();
            builder.setProperty("a2", createBlob(nodeStore, blobSize));
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // Size is double
            assertSize("pre cleanup", fileStore.size(), 2 * blobSize
                    + dataSize, 2 * blobSize + 2 * dataSize);

            // 5. Cleanup, expecting store size:
            // no data content =>
            // fileStore.size() == blobSize
            // some data content =>
            // fileStore.size() in [blobSize + dataSize, blobSize + 2 x dataSize]
            assertTrue(fileStore.maybeCompact(false));
            fileStore.cleanup();
            assertSize("post cleanup", fileStore.size(), 0, blobSize + 2 * dataSize);

            // refresh the ts ref, to simulate a long wait time
            custom.setOlderThan(0);
            TimeUnit.MILLISECONDS.sleep(5);

            boolean needsCompaction = true;
            for (int i = 0; i < 3 && needsCompaction; i++) {
                needsCompaction = fileStore.maybeCompact(false);
                fileStore.cleanup();
            }

            // gain is finally 0%
            assertFalse(fileStore.maybeCompact(false));

            // no data loss happened
            byte[] blob = ByteStreams.toByteArray(nodeStore.getRoot()
                    .getProperty("a2").getValue(Type.BINARY).getNewStream());
            assertEquals(blobSize, blob.length);
        } finally {
            fileStore.close();
        }
    }

    @Test
    public void noCleanupOnCompactionMap() throws Exception {
        // 2MB data, 5MB blob
        final int blobSize = 5 * 1024 * 1024;
        final int dataNodes = 10000;

        FileStore fileStore = FileStore.builder(getFileStoreFolder()).withMaxFileSize(1).build();
        final SegmentNodeStore nodeStore = SegmentNodeStore.builder(fileStore).build();
        CompactionStrategy custom = new CompactionStrategy(false, false,
                CLEAN_OLD, TimeUnit.HOURS.toMillis(1), (byte) 0) {
            @Override
            public boolean compacted(@Nonnull Callable<Boolean> setHead)
                    throws Exception {
                return nodeStore.locked(setHead);
            }
        };
        fileStore.setCompactionStrategy(custom);

        // 1a. Create a bunch of data
        NodeBuilder extra = nodeStore.getRoot().builder();
        NodeBuilder content = extra.child("content");
        for (int i = 0; i < dataNodes; i++) {
            NodeBuilder c = content.child("c" + i);
            for (int j = 0; j < 1000; j++) {
                c.setProperty("p" + i, "v" + i);
            }
        }
        nodeStore.merge(extra, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final long dataSize = fileStore.size();
        log.debug("File store dataSize {}", byteCountToDisplaySize(dataSize));

        try {
            // 1. Create a property with 5 MB blob
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setProperty("a1", createBlob(nodeStore, blobSize));
            builder.setProperty("b", "foo");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // 2. Now remove the property
            builder = nodeStore.getRoot().builder();
            builder.removeProperty("a1");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // 3. Compact
            fileStore.maybeCompact(false);

            // 4. Add some more property to flush the current TarWriter
            builder = nodeStore.getRoot().builder();
            builder.setProperty("a2", createBlob(nodeStore, blobSize));
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // There should be no SNFE when running cleanup as compaction map segments
            // should be pinned and thus not collected
            fileStore.maybeCompact(false);
            fileStore.cleanup();

            // refresh the ts ref, to simulate a long wait time
            custom.setOlderThan(0);
            TimeUnit.MILLISECONDS.sleep(5);

            boolean needsCompaction = true;
            for (int i = 0; i < 3 && needsCompaction; i++) {
                needsCompaction = fileStore.maybeCompact(false);
                fileStore.cleanup();
            }
        } finally {
            fileStore.close();
        }
    }

    private static void assertSize(String info, long size, long lower,
            long upper) {
        log.debug("File Store {} size {}, expected in interval [{},{}]", info,
                byteCountToDisplaySize(size), byteCountToDisplaySize(lower),
                byteCountToDisplaySize(upper));
        assertTrue("File Store " + log + " size expected in interval ["
                        + mb(lower) + "," + mb(upper) + "] but was: " + mb(size),
                mb(size) >= mb(lower) && mb(size) <= mb(upper));
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    private static long mb(long size){
        return size / (1024 * 1024);
    }

    /**
     * Regression test for OAK-2192 testing for mixed segments. This test does not
     * cover OAK-3348. I.e. it does not assert the segment graph is free of cross
     * gc generation references.
     */
    @Test
    public void testMixedSegments() throws Exception {
        FileStore store = FileStore.builder(getFileStoreFolder()).withMaxFileSize(2).withMemoryMapping(true).build();
        final SegmentNodeStore nodeStore = SegmentNodeStore.builder(store).build();
        final AtomicBoolean compactionSuccess = new AtomicBoolean(true);
        CompactionStrategy strategy = new CompactionStrategy(true, false, CLEAN_NONE, 0, (byte) 5) {
            @Override
            public boolean compacted(Callable<Boolean> setHead) throws Exception {
                compactionSuccess.set(nodeStore.locked(setHead, 1, MINUTES));
                return compactionSuccess.get();
            }
        };
        strategy.setForceAfterFail(true);
        store.setCompactionStrategy(strategy);

        NodeBuilder root = nodeStore.getRoot().builder();
        createNodes(root.setChildNode("test"), 10, 3);
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        final Set<UUID> beforeSegments = new HashSet<UUID>();
        collectSegments(store.getHead(), beforeSegments);

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
        collectSegments(store.getHead(), afterSegments);
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
    public void cleanupCyclicGraph() throws IOException, ExecutionException, InterruptedException {
        FileStore fileStore = FileStore.builder(getFileStoreFolder()).build();
        final SegmentWriter writer = fileStore.getTracker().getWriter();
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
        fileStore.setHead(oldHead, newHead);
        fileStore.close();

        fileStore = FileStore.builder(getFileStoreFolder()).build();

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

    /**
     * Test asserting OAK-3348: Cross gc sessions might introduce references to pre-compacted segments
     */
    @Test
    @Ignore("OAK-3348")  // FIXME OAK-3348
    public void preCompactionReferences() throws IOException, CommitFailedException, InterruptedException {
        for (String ref : new String[] {"merge-before-compact", "merge-after-compact"}) {
            File repoDir = new File(getFileStoreFolder(), ref);
            FileStore fileStore = FileStore.builder(repoDir).withMaxFileSize(2).build();
            final SegmentNodeStore nodeStore = builder(fileStore).build();
            fileStore.setCompactionStrategy(new CompactionStrategy(true, false, CLEAN_NONE, 0, (byte) 5) {
                @Override
                public boolean compacted(Callable<Boolean> setHead) throws Exception {
                    return nodeStore.locked(setHead);
                }
            });

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

                fileStore.compact();

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
            fileStore = FileStore.builder(repoDir).withMaxFileSize(2).build();
            try {
                // The 1M blob should get gc-ed. This works for case 1.
                // However it doesn't for case 2 as merging after compaction
                // apparently creates references from the current segment
                // to the pre-compacted segment to which above changes have
                // been pre-written.
                fileStore.cleanup();
                assertTrue(ref + " repository size " + fileStore.size() + " < " + 1024 * 1024,
                        fileStore.size() < 1024 * 1024);
            } finally {
                fileStore.close();
            }
        }
    }

    private static void collectSegments(SegmentNodeState s, final Set<UUID> segmentIds) {
        new SegmentParser() {
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
        }.parseNode(s.getRecordId());
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
    public void propertyRetention() throws IOException, CommitFailedException {
        FileStore fileStore = FileStore.builder(getFileStoreFolder()).withMaxFileSize(1).build();
        try {
            final SegmentNodeStore nodeStore = SegmentNodeStore.builder(fileStore).build();
            CompactionStrategy strategy = new CompactionStrategy(false, false, CLEAN_ALL, 0, (byte) 0) {
                @Override
                public boolean compacted(@Nonnull Callable<Boolean> setHead)
                        throws Exception {
                    return nodeStore.locked(setHead);
                }
            };
            // CLEAN_ALL and persisted compaction map results in SNFE in compaction map segments
            strategy.setPersistCompactionMap(false);
            fileStore.setCompactionStrategy(strategy);

            // Add a property
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.setChildNode("test").setProperty("property", "value");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            // Segment id of the current segment
            NodeState test = nodeStore.getRoot().getChildNode("test");
            SegmentId id = ((SegmentNodeState) test).getRecordId().getSegmentId();
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
            fileStore.compact();
            fileStore.cleanup();

            try {
                fileStore.readSegment(id);
                fail("Segment " + id + "should be gc'ed");
            } catch (SegmentNotFoundException ignore) {}
        } finally {
            fileStore.close();
        }
    }

    private static void addContent(NodeBuilder builder) {
        for (int k = 0; k < 10000; k++) {
            builder.setProperty(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }
    }
}
