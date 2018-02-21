/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState.binaryPropertyFromBlob;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import javax.annotation.Nonnull;

import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CheckpointCompactorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore fileStore;

    private SegmentNodeStore nodeStore;

    private CheckpointCompactor compactor;

    private GCGeneration compactedGeneration;

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException {
        fileStore = fileStoreBuilder(folder.getRoot()).build();
        nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        compactedGeneration = newGCGeneration(1,1, true);
        compactor = createCompactor(fileStore, compactedGeneration);
    }

    @After
    public void tearDown() {
        fileStore.close();
    }

    @Test
    public void testCompact() throws Exception {
        addTestContent("cp1", nodeStore);
        String cp1 = nodeStore.checkpoint(DAYS.toMillis(1));
        addTestContent("cp2", nodeStore);
        String cp2 = nodeStore.checkpoint(DAYS.toMillis(1));

        SegmentNodeState uncompacted1 = fileStore.getHead();
        SegmentNodeState compacted1 = compactor.compact(EMPTY_NODE, uncompacted1, EMPTY_NODE);
        assertNotNull(compacted1);
        assertFalse(uncompacted1 == compacted1);
        checkGeneration(compacted1, compactedGeneration);

        assertSameStableId(uncompacted1, compacted1);
        assertSameStableId(getCheckpoint(uncompacted1, cp1), getCheckpoint(compacted1, cp1));
        assertSameStableId(getCheckpoint(uncompacted1, cp2), getCheckpoint(compacted1, cp2));
        assertSameRecord(getCheckpoint(compacted1, cp2), compacted1.getChildNode("root"));

        // Simulate a 2nd compaction cycle
        addTestContent("cp3", nodeStore);
        String cp3 = nodeStore.checkpoint(DAYS.toMillis(1));
        addTestContent("cp4", nodeStore);
        String cp4 = nodeStore.checkpoint(DAYS.toMillis(1));

        SegmentNodeState uncompacted2 = fileStore.getHead();
        SegmentNodeState compacted2 = compactor.compact(uncompacted1, uncompacted2, compacted1);
        assertNotNull(compacted2);
        assertFalse(uncompacted2 == compacted2);
        checkGeneration(compacted2, compactedGeneration);

        assertTrue(fileStore.getRevisions().setHead(uncompacted2.getRecordId(), compacted2.getRecordId()));

        assertEquals(uncompacted2, compacted2);
        assertSameStableId(uncompacted2, compacted2);
        assertSameStableId(getCheckpoint(uncompacted2, cp1), getCheckpoint(compacted2, cp1));
        assertSameStableId(getCheckpoint(uncompacted2, cp2), getCheckpoint(compacted2, cp2));
        assertSameStableId(getCheckpoint(uncompacted2, cp3), getCheckpoint(compacted2, cp3));
        assertSameStableId(getCheckpoint(uncompacted2, cp4), getCheckpoint(compacted2, cp4));
        assertSameRecord(getCheckpoint(compacted1, cp1), getCheckpoint(compacted2, cp1));
        assertSameRecord(getCheckpoint(compacted1, cp2), getCheckpoint(compacted2, cp2));
        assertSameRecord(getCheckpoint(compacted2, cp4), compacted2.getChildNode("root"));
    }

    private static void checkGeneration(NodeState node, GCGeneration gcGeneration) {
        assertTrue(node instanceof SegmentNodeState);
        assertEquals(gcGeneration, ((SegmentNodeState) node).getRecordId().getSegmentId().getGcGeneration());

        for (ChildNodeEntry cne : node.getChildNodeEntries()) {
            checkGeneration(cne.getNodeState(), gcGeneration);
        }
    }

    private static NodeState getCheckpoint(NodeState superRoot, String name) {
        NodeState checkpoint = superRoot
                .getChildNode("checkpoints")
                .getChildNode(name)
                .getChildNode("root");
        assertTrue(checkpoint.exists());
        return checkpoint;
    }

    private static void assertSameStableId(NodeState node1, NodeState node2) {
        assertTrue(node1 instanceof SegmentNodeState);
        assertTrue(node2 instanceof SegmentNodeState);

        assertEquals("Nodes should have the same stable ids",
                ((SegmentNodeState) node1).getStableId(),
                ((SegmentNodeState) node2).getStableId());
    }

    private static void assertSameRecord(NodeState node1, NodeState node2) {
        assertTrue(node1 instanceof SegmentNodeState);
        assertTrue(node2 instanceof SegmentNodeState);

        assertEquals("Nodes should have been deduplicated",
                ((SegmentNodeState) node1).getRecordId(),
                ((SegmentNodeState) node2).getRecordId());
    }

    @Nonnull
    private static CheckpointCompactor createCompactor(@Nonnull FileStore fileStore, @Nonnull GCGeneration generation) {
        SegmentWriter writer = defaultSegmentWriterBuilder("c")
                .withGeneration(generation)
                .build(fileStore);

        return new CheckpointCompactor(
                GCMonitor.EMPTY,
                fileStore.getReader(),
                writer,
                fileStore.getBlobStore(),
                Suppliers.ofInstance(false),
                GCNodeWriteMonitor.EMPTY);
    }

    private static void addTestContent(@Nonnull String parent, @Nonnull NodeStore nodeStore)
    throws CommitFailedException, IOException {
        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        NodeBuilder parentBuilder = rootBuilder.child(parent);
        parentBuilder.setChildNode("a").setChildNode("aa").setProperty("p", 42);
        parentBuilder.getChildNode("a").setChildNode("bb").setChildNode("bbb");
        parentBuilder.setChildNode("b").setProperty("bin", createBlob(nodeStore, 42));
        parentBuilder.setChildNode("c").setProperty(binaryPropertyFromBlob("bins", createBlobs(nodeStore, 42, 43, 44)));
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    private static List<Blob> createBlobs(NodeStore nodeStore, int... sizes) throws IOException {
        List<Blob> blobs = newArrayList();
        for (int size : sizes) {
            blobs.add(createBlob(nodeStore, size));
        }
        return blobs;
    }

}
