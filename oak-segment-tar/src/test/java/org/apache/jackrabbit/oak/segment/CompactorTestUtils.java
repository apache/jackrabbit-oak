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

import static org.apache.jackrabbit.oak.plugins.memory.MultiBinaryPropertyState.binaryPropertyFromBlob;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.segment.file.CompactedNodeState;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CompactorTestUtils {

    private CompactorTestUtils() {}

    public interface SimpleCompactor {
        CompactedNodeState compact(NodeState nodeState, Canceller canceller) throws IOException;
    }

    public interface SimpleCompactorFactory {
        SimpleCompactor newSimpleCompactor(Compactor compactor);
    }

    public static void checkGeneration(NodeState node, GCGeneration gcGeneration) {
        assertTrue(node instanceof SegmentNodeState);
        assertEquals(gcGeneration, ((SegmentNodeState) node).getRecordId().getSegmentId().getGcGeneration());

        for (ChildNodeEntry cne : node.getChildNodeEntries()) {
            checkGeneration(cne.getNodeState(), gcGeneration);
        }
    }

    public static NodeState getCheckpoint(NodeState superRoot, String name) {
        NodeState checkpoint = superRoot
                .getChildNode("checkpoints")
                .getChildNode(name)
                .getChildNode("root");
        assertTrue(checkpoint.exists());
        return checkpoint;
    }

    public static void assertSameStableId(NodeState node1, NodeState node2) {
        assertTrue(node1 instanceof SegmentNodeState);
        assertTrue(node2 instanceof SegmentNodeState);

        assertEquals("Nodes should have the same stable ids",
                ((SegmentNodeState) node1).getStableId(),
                ((SegmentNodeState) node2).getStableId());
    }

    public static void assertSameRecord(NodeState node1, NodeState node2) {
        assertTrue(node1 instanceof SegmentNodeState);
        assertTrue(node2 instanceof SegmentNodeState);

        assertEquals("Nodes should have been deduplicated",
                ((SegmentNodeState) node1).getRecordId(),
                ((SegmentNodeState) node2).getRecordId());
    }

    public static void addTestContent(@NotNull String parent, @NotNull NodeStore nodeStore, int binPropertySize)
            throws CommitFailedException, IOException {
        NodeBuilder rootBuilder = nodeStore.getRoot().builder();
        NodeBuilder parentBuilder = rootBuilder.child(parent);
        parentBuilder.setChildNode("a").setChildNode("aa").setProperty("p", 42);
        parentBuilder.getChildNode("a").setChildNode("bb").setChildNode("bbb");
        parentBuilder.setChildNode("b").setProperty("bin", createBlob(nodeStore, binPropertySize));
        parentBuilder.setChildNode("c").setProperty(binaryPropertyFromBlob("bins", createBlobs(nodeStore, 42, 43, 44)));
        nodeStore.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    public static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    public static List<Blob> createBlobs(NodeStore nodeStore, int... sizes) throws IOException {
        List<Blob> blobs = new ArrayList<>();
        for (int size : sizes) {
            blobs.add(createBlob(nodeStore, size));
        }
        return blobs;
    }
}
