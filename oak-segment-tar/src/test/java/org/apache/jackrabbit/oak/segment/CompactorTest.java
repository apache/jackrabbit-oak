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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CompactorTest {
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
        Compactor compactor = createCompactor(fileStore, Suppliers.ofInstance(false), null);
        addTestContent(nodeStore);

        SegmentNodeState uncompacted = (SegmentNodeState) nodeStore.getRoot();
        SegmentNodeState compacted = compactor.compact(uncompacted);
        assertNotNull(compacted);
        assertFalse(uncompacted == compacted);
        assertEquals(uncompacted, compacted);
        assertEquals(uncompacted.getSegment().getGcGeneration().nextFull(), compacted.getSegment().getGcGeneration());

        modifyTestContent(nodeStore);
        NodeState modified = nodeStore.getRoot();
        compacted = compactor.compact(uncompacted, modified, compacted);
        assertNotNull(compacted);
        assertFalse(modified == compacted);
        assertEquals(modified, compacted);
        assertEquals(uncompacted.getSegment().getGcGeneration().nextFull(), compacted.getSegment().getGcGeneration());
    }

    @Test
    public void testExceedUpdateLimit() throws Exception {
        Compactor compactor = createCompactor(fileStore, Suppliers.ofInstance(false), null);
        addNodes(nodeStore, Compactor.UPDATE_LIMIT * 2 + 1);

        SegmentNodeState uncompacted = (SegmentNodeState) nodeStore.getRoot();
        SegmentNodeState compacted = compactor.compact(uncompacted);
        assertNotNull(compacted);
        assertFalse(uncompacted == compacted);
        assertEquals(uncompacted, compacted);
        assertEquals(uncompacted.getSegment().getGcGeneration().nextFull(), compacted.getSegment().getGcGeneration());
    }

    @Test
    public void testCancel() throws IOException, CommitFailedException {
        Compactor compactor = createCompactor(fileStore, Suppliers.ofInstance(true), null);
        addTestContent(nodeStore);
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setChildNode("cancel").setProperty("cancel", "cancel");
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertNull(compactor.compact(nodeStore.getRoot()));
    }

    @Test(expected = IOException.class)
    public void testIOException() throws IOException, CommitFailedException {
        Compactor compactor = createCompactor(fileStore, Suppliers.ofInstance(false), "IOException");
        addTestContent(nodeStore);
        compactor.compact(nodeStore.getRoot());
    }

    @Nonnull
    private static Compactor createCompactor(FileStore fileStore, Supplier<Boolean> cancel, String failOnName) {
        SegmentWriter writer = defaultSegmentWriterBuilder("c")
                .withGeneration(newGCGeneration(1, 1, true))
                .build(fileStore);
        if (failOnName != null) {
            writer = new FailingSegmentWriter(writer, failOnName);
        }
        return new Compactor(fileStore.getReader(), writer, fileStore.getBlobStore(), cancel, GCNodeWriteMonitor.EMPTY);
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
        List<Blob> blobs = newArrayList();
        for (int size : sizes) {
            blobs.add(createBlob(nodeStore, size));
        }
        return blobs;
    }

    private static class FailingSegmentWriter implements SegmentWriter {
        @Nonnull
        private final SegmentWriter delegate;

        @Nonnull
        private final String failOnName;

        public FailingSegmentWriter(@Nonnull SegmentWriter delegate, @Nonnull String failOnName) {
            this.delegate = delegate;
            this.failOnName = failOnName;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Nonnull
        @Override
        public RecordId writeMap(
                @Nullable MapRecord base, @Nonnull Map<String, RecordId> changes)
        throws IOException {
            return delegate.writeMap(base, changes);
        }

        @Nonnull
        @Override
        public RecordId writeList(@Nonnull List<RecordId> list) throws IOException {
            return delegate.writeList(list);
        }

        @Nonnull
        @Override
        public RecordId writeString(@Nonnull String string) throws IOException {
            return delegate.writeString(string);
        }

        @Nonnull
        @Override
        public RecordId writeBlob(@Nonnull Blob blob) throws IOException {
            return delegate.writeBlob(blob);
        }

        @Nonnull
        @Override
        public RecordId writeBlock(@Nonnull byte[] bytes, int offset, int length)
        throws IOException {
            return delegate.writeBlock(bytes, offset, length);
        }

        @Nonnull
        @Override
        public RecordId writeStream(@Nonnull InputStream stream) throws IOException {
            return delegate.writeStream(stream);
        }

        @Nonnull
        @Override
        public RecordId writeProperty(@Nonnull PropertyState state) throws IOException {
            return delegate.writeProperty(state);
        }

        @Nonnull
        @Override
        public RecordId writeNode(@Nonnull NodeState state, @Nullable ByteBuffer stableIdBytes)
        throws IOException {
            if (state.hasChildNode(failOnName)) {
                throw new IOException("Encountered node with name " + failOnName);
            }

            return delegate.writeNode(state, stableIdBytes);
        }
    }
}
