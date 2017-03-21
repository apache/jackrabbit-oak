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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class NodeRecordTest {

    private static class Generation implements Supplier<Integer> {

        private int generation;

        public void set(int generation) {
            this.generation = generation;
        }

        @Override
        public Integer get() {
            return generation;
        }

    }

    @Rule
    public TemporaryFolder root = new TemporaryFolder();

    private FileStore newFileStore() throws Exception {
        return FileStoreBuilder.fileStoreBuilder(root.getRoot()).build();
    }

    @Test
    public void unreferencedNodeRecordShouldBeRoot() throws Exception {
        try (FileStore store = newFileStore()) {
            SegmentWriter writer = SegmentWriterBuilder.segmentWriterBuilder("test").build(store);
            SegmentNodeState state = writer.writeNode(EmptyNodeState.EMPTY_NODE);
            writer.flush();
        }
    }

    @Test
    public void stableIdShouldPersistAcrossGenerations() throws Exception {
        try (FileStore store = newFileStore()) {
            SegmentWriter writer;

            writer = SegmentWriterBuilder.segmentWriterBuilder("1").withGeneration(1).build(store);
            SegmentNodeState one = writer.writeNode(EmptyNodeState.EMPTY_NODE);
            writer.flush();

            writer = SegmentWriterBuilder.segmentWriterBuilder("2").withGeneration(2).build(store);
            SegmentNodeState two = writer.writeNode(one);
            writer.flush();

            writer = SegmentWriterBuilder.segmentWriterBuilder("3").withGeneration(3).build(store);
            SegmentNodeState three = writer.writeNode(two);
            writer.flush();

            assertArrayEquals(three.getStableIdBytes(), two.getStableIdBytes());
            assertArrayEquals(two.getStableIdBytes(), one.getStableIdBytes());
        }
    }

    @Test
    public void baseNodeStateShouldBeReusedAcrossGenerations() throws Exception {
        try (FileStore store = newFileStore()) {
            Generation generation = new Generation();

            // Create a new SegmentWriter. It's necessary not to have any cache,
            // otherwise the write of some records (in this case, template
            // records) will be cached and prevent this test to fail.

            SegmentWriter writer = SegmentWriterBuilder.segmentWriterBuilder("test")
                    .withGeneration(generation)
                    .withWriterPool()
                    .with(nodesOnlyCache())
                    .build(store);

            generation.set(1);

            // Write a new node with a non trivial template. This record will
            // belong to generation 1.

            SegmentNodeState base = writer.writeNode(EmptyNodeState.EMPTY_NODE.builder()
                    .setProperty("a", "a")
                    .setProperty("k", "v1")
                    .getNodeState()
            );
            writer.flush();

            generation.set(2);

            // Compact that same record to generation 2.

            SegmentNodeState compacted = writer.writeNode(base);
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
