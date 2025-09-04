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
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.CompactionWriter;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ClassicCompactorTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore fileStore;

    private SegmentNodeStore nodeStore;

    @Before
    public void setup() throws IOException, InvalidFileStoreVersionException {
        fileStore = fileStoreBuilder(folder.getRoot()).build();
        nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
    }

    @After
    public void tearDown() {
        fileStore.close();
    }

    @Test
    public void testCompact() throws Exception {
        ClassicCompactor compactor = createCompactor(fileStore, null);
        addTestContent(nodeStore);

        SegmentNodeState uncompacted = (SegmentNodeState) nodeStore.getRoot();
        SegmentNodeState compacted = compactor.compactUp(uncompacted, Canceller.newCanceller());
        assertNotNull(compacted);
        assertFalse(uncompacted == compacted);
        assertEquals(uncompacted, compacted);
        assertEquals(uncompacted.getSegment().getGcGeneration().nextFull(), compacted.getSegment().getGcGeneration());

        modifyTestContent(nodeStore);
        NodeState modified = nodeStore.getRoot();
        compacted = compactor.compact(uncompacted, modified, compacted, Canceller.newCanceller());
        assertNotNull(compacted);
        assertFalse(modified == compacted);
        assertEquals(modified, compacted);
        assertEquals(uncompacted.getSegment().getGcGeneration().nextFull(), compacted.getSegment().getGcGeneration());
    }

    @Test
    public void testExceedUpdateLimit() throws Exception {
        ClassicCompactor compactor = createCompactor(fileStore, null);
        addNodes(nodeStore, ClassicCompactor.UPDATE_LIMIT * 2 + 1);

        SegmentNodeState uncompacted = (SegmentNodeState) nodeStore.getRoot();
        SegmentNodeState compacted = compactor.compactUp(uncompacted, Canceller.newCanceller());
        assertNotNull(compacted);
        assertFalse(uncompacted == compacted);
        assertEquals(uncompacted, compacted);
        assertEquals(uncompacted.getSegment().getGcGeneration().nextFull(), compacted.getSegment().getGcGeneration());
    }

    @Test
    public void testCancel() throws IOException, CommitFailedException {
        ClassicCompactor compactor = createCompactor(fileStore, null);
        addTestContent(nodeStore);
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setChildNode("cancel").setProperty("cancel", "cancel");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertNull(compactor.compactUp(nodeStore.getRoot(), Canceller.newCanceller().withCondition("reason", () -> true)));
    }

    @Test(expected = IOException.class)
    public void testIOException() throws IOException, CommitFailedException {
        ClassicCompactor compactor = createCompactor(fileStore, "IOException");
        addTestContent(nodeStore);
        compactor.compactUp(nodeStore.getRoot(), Canceller.newCanceller());
    }

    @NotNull
    private static ClassicCompactor createCompactor(FileStore fileStore, String failOnName) {
        GCGeneration generation = newGCGeneration(1, 1, true);
        SegmentWriter writer = defaultSegmentWriterBuilder("c")
                .withGeneration(generation)
                .build(fileStore);
        if (failOnName != null) {
            writer = new FailingSegmentWriter(writer, failOnName);
        }
        CompactionWriter compactionWriter = new CompactionWriter(fileStore.getReader(), fileStore.getBlobStore(), generation, writer);
        return new ClassicCompactor(compactionWriter, GCNodeWriteMonitor.EMPTY);
    }

    private static void addNodes(SegmentNodeStore nodeStore, int count)
    throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        for (int k = 0; k < count; k++) {
            builder.setChildNode("n-" + k);
        }
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static void addTestContent(NodeStore nodeStore) throws CommitFailedException, IOException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setChildNode("a").setChildNode("aa").setProperty("p", 42);
        builder.getChildNode("a").setChildNode("error").setChildNode("IOException");
        builder.setChildNode("b").setProperty("bin", createBlob(nodeStore, 42));
        builder.setChildNode("c").setProperty(binaryPropertyFromBlob("bins", createBlobs(nodeStore, 42, 43, 44)));
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static void modifyTestContent(NodeStore nodeStore) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.getChildNode("a").getChildNode("aa").remove();
        builder.getChildNode("b").setProperty("bin", "changed");
        builder.getChildNode("c").removeProperty("bins");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    private static List<Blob> createBlobs(NodeStore nodeStore, int... sizes) throws IOException {
        List<Blob> blobs = new ArrayList<>();
        for (int size : sizes) {
            blobs.add(createBlob(nodeStore, size));
        }
        return blobs;
    }

    private static class FailingSegmentWriter implements SegmentWriter {

        @NotNull
        private final SegmentWriter delegate;

        @NotNull
        private final String failOnName;

        public FailingSegmentWriter(@NotNull SegmentWriter delegate, @NotNull String failOnName) {
            this.delegate = delegate;
            this.failOnName = failOnName;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @NotNull
        @Override
        public RecordId writeBlob(@NotNull Blob blob) throws IOException {
            return delegate.writeBlob(blob);
        }

        @NotNull
        @Override
        public RecordId writeStream(@NotNull InputStream stream) throws IOException {
            return delegate.writeStream(stream);
        }

        @NotNull
        @Override
        public RecordId writeNode(@NotNull NodeState state, @Nullable Buffer stableIdBytes) throws IOException {
            if (state.hasChildNode(failOnName)) {
                throw new IOException("Encountered node with name " + failOnName);
            }
            return delegate.writeNode(state, stableIdBytes);
        }

    }
}
