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
 */

package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NodeRecordTest {

    private static class Generation implements Supplier<GCGeneration> {

        private GCGeneration generation;

        public void set(GCGeneration generation) {
            this.generation = generation;
        }

        @Override
        public GCGeneration get() {
            return generation;
        }

    }

    @Rule
    public TemporaryFolder root = new TemporaryFolder();

    private FileStore newFileStore() throws Exception {
        return FileStoreBuilder.fileStoreBuilder(root.getRoot()).build();
    }

    @Test
    public void stableIdShouldPersistAcrossGenerations() throws Exception {
        try (FileStore store = newFileStore()) {
            SegmentWriter writer;

            writer = defaultSegmentWriterBuilder("1")
                    .withGeneration(newGCGeneration(1, 0, false))
                    .build(store);
            SegmentNodeState one = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(EmptyNodeState.EMPTY_NODE));
            writer.flush();

            writer = defaultSegmentWriterBuilder("2")
                    .withGeneration(newGCGeneration(2, 0, false))
                    .build(store);
            SegmentNodeState two = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(one));
            writer.flush();

            writer = defaultSegmentWriterBuilder("3")
                    .withGeneration(newGCGeneration(3, 0, false))
                    .build(store);
            SegmentNodeState three = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(two));
            writer.flush();

            assertArrayEquals(asByteArray(three.getStableIdBytes()), asByteArray(two.getStableIdBytes()));
            assertArrayEquals(asByteArray(two.getStableIdBytes()), asByteArray(one.getStableIdBytes()));
        }
    }

    private static final byte[] asByteArray(ByteBuffer bytes) {
        byte[] buffer = new byte[RecordId.SERIALIZED_RECORD_ID_BYTES];
        bytes.get(buffer);
        return buffer;
    }

    @Test
    public void baseNodeStateShouldBeReusedAcrossGenerations() throws Exception {
        try (FileStore store = newFileStore()) {
            Generation generation = new Generation();

            // Create a new SegmentWriter. It's necessary not to have any cache,
            // otherwise the write of some records (in this case, template
            // records) will be cached and prevent this test to fail.

            SegmentWriter writer = defaultSegmentWriterBuilder("test")
                    .withGeneration(generation)
                    .withWriterPool()
                    .with(nodesOnlyCache())
                    .build(store);

            generation.set(newGCGeneration(1, 0, false));

            // Write a new node with a non trivial template. This record will
            // belong to generation 1.

            RecordId baseId = writer.writeNode(EmptyNodeState.EMPTY_NODE.builder()
                    .setProperty("a", "a")
                    .setProperty("k", "v1")
                    .getNodeState()
            );
            SegmentNodeState base = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), baseId);
            writer.flush();

            generation.set(newGCGeneration(2, 0, false));

            // Compact that same record to generation 2.

            SegmentNodeState compacted = new SegmentNodeState(store.getReader(), writer, store.getBlobStore(), writer.writeNode(base));
            writer.flush();

            // Assert that even if the two records have the same stable ID,
            // their physical ID and the ID of their templates are different.

            assertEquals(base.getStableId(), compacted.getStableId());
            assertNotEquals(base.getRecordId(), compacted.getRecordId());
            assertNotEquals(base.getTemplateId(), compacted.getTemplateId());

            // Create a new builder from the base, pre-compaction node state.
            // The base node state is from generation 1, but this builder will
            // be from generation 2 because every builder in the pool is
            // affected by the change of generation. Writing a node state from
            // this builder should perform a partial compaction.

            SegmentNodeState modified = (SegmentNodeState) base.builder().setProperty("k", "v2").getNodeState();

            // Assert that the stable ID of this node state is different from
            // the one in the base state. This is expected, since we have
            // modified the value of a property.

            assertNotEquals(modified.getStableId(), base.getStableId());
            assertNotEquals(modified.getStableId(), compacted.getStableId());

            // The node state should have reused the template from the compacted
            // node state, since this template didn't change and the code should
            // have detected that the base state of this builder was compacted
            // to a new generation.

            assertEquals(modified.getTemplateId(), compacted.getTemplateId());

            // Similarly the node state should have reused the property from
            // the compacted node state, since this property didn't change.

            Record modifiedProperty = (Record) modified.getProperty("a");
            Record compactedProperty = (Record) compacted.getProperty("a");

            assertNotNull(modifiedProperty);
            assertNotNull(compactedProperty);
            assertEquals(modifiedProperty.getRecordId(), compactedProperty.getRecordId());
        }
    }

    private static WriterCacheManager nodesOnlyCache() {
        return new WriterCacheManager() {

            WriterCacheManager defaultCache = new WriterCacheManager.Default();

            @Nonnull
            @Override
            public Cache<String, RecordId> getStringCache(int generation) {
                return Empty.INSTANCE.getStringCache(generation);
            }

            @Nonnull
            @Override
            public Cache<Template, RecordId> getTemplateCache(int generation) {
                return Empty.INSTANCE.getTemplateCache(generation);
            }

            @Nonnull
            @Override
            public Cache<String, RecordId> getNodeCache(int generation) {
                return defaultCache.getNodeCache(generation);
            }
        };
    }

}
