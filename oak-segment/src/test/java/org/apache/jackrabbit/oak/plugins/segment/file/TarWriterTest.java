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

package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.nio.ByteBuffer.allocate;
import static java.util.Collections.singleton;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentVersion.V_11;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.segment.RecordId;
import org.apache.jackrabbit.oak.plugins.segment.Segment;
import org.apache.jackrabbit.oak.plugins.segment.SegmentId;
import org.apache.jackrabbit.oak.plugins.segment.SegmentStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentWriter;
import org.apache.jackrabbit.oak.plugins.segment.file.TarWriterTest.SegmentGraphBuilder.Node;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TarWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    /**
     * Regression test for OAK-2800
     */
    @Test
    public void collectReferences() throws IOException {
        SegmentGraphBuilder graphBuilder = new SegmentGraphBuilder();

        // a -> b -> c
        Node c = graphBuilder.createNode("c");
        Node b = graphBuilder.createNode("b", c);
        Node a = graphBuilder.createNode("a", b);

        Node n = graphBuilder.createNode("n");

        // y -> z
        Node z = graphBuilder.createNode("z");
        Node y = graphBuilder.createNode("y", z);

        assertEquals(singleton(b), a.getReferences());
        assertEquals(singleton(c), b.getReferences());
        assertTrue(c.getReferences().isEmpty());
        assertEquals(singleton(z), y.getReferences());
        assertTrue(z.getReferences().isEmpty());

        File tar = folder.newFile(getClass().getName() + ".tar");
        TarWriter tarWriter = new TarWriter(tar);
        try {
            y.write(tarWriter);
            b.write(tarWriter);
            a.write(tarWriter);
            n.write(tarWriter);

            Set<UUID> references = newHashSet();
            references.add(a.getUUID());
            tarWriter.collectReferences(references);
            assertEquals(
                c + " must be in references as " + a + " has an transitive reference to " + c + " through " + b + ", " +
                a + " must not be in references as " + a + " is in the TarWriter, " +
                "no other elements must be in references.",
                singleton(c), toNodes(graphBuilder, references));

            references.clear();
            references.add(b.getUUID());
            tarWriter.collectReferences(references);
            assertEquals(
                b + " must be in references as " + a + " has a direct reference to " + b + ", " +
                a + " must not be in references as " + a + " is in the TarWriter, " +
                "no other elements must be in references.",
                singleton(c), toNodes(graphBuilder, references));

            references.clear();
            references.add(y.getUUID());
            tarWriter.collectReferences(references);
            assertEquals(
                z + " must be in references as " + y + " has a direct reference to " + z + ", " +
                y + " must not be in references as " + y + " is in the TarWriter, " +
                "no other elements must be in references.",
                singleton(z), toNodes(graphBuilder, references));

            references.clear();
            references.add(c.getUUID());
            tarWriter.collectReferences(references);
            assertEquals(
                c + " must be in references as " + c + " is not in the TarWriter, " +
                "no other elements must be in references.",
                singleton(c), toNodes(graphBuilder, references));

            references.clear();
            references.add(z.getUUID());
            tarWriter.collectReferences(references);
            assertEquals(
                z + " must be in references as " + z + " is not in the TarWriter " +
                    "no other elements must be in references.",
                singleton(z), toNodes(graphBuilder, references));

            references.clear();
            references.add(n.getUUID());
            tarWriter.collectReferences(references);
            assertTrue(
                "references must be empty as " + n + " has no references " +
                "and " + n + " is in the TarWriter",
                references.isEmpty());
        } finally {
            tarWriter.close();
        }
    }

    private static Set<Node> toNodes(SegmentGraphBuilder graphBuilder, Set<UUID> uuids) {
        Set<Node> nodes = newHashSet();
        for (UUID uuid : uuids) {
            nodes.add(graphBuilder.getNode(uuid));
        }
        return nodes;
    }

    public static class SegmentGraphBuilder {
        private final Map<SegmentId, ByteBuffer> segments = newHashMap();
        private final Map<UUID, Node> nodes = newHashMap();

        private final SegmentStore store;
        private final SegmentWriter writer;

        private int nextNodeNo;

        public SegmentGraphBuilder() throws IOException {
            store = new MemoryStore() {
                @Override
                public void writeSegment(SegmentId id, byte[] data, int offset, int length) throws IOException {
                    super.writeSegment(id, data, offset, length);
                    ByteBuffer buffer = allocate(length);
                    buffer.put(data, offset, length);
                    buffer.rewind();
                    segments.put(id, buffer);
                }
            };
            writer = new SegmentWriter(store, V_11, "");
        }

        public class Node {
            final String name;
            final RecordId selfId;
            final byte[] data;
            final Segment segment;

            Node(String name, RecordId selfId, ByteBuffer data) {
                this.name = name;
                this.selfId = selfId;
                this.data = data.array();
                segment = new Segment(store.getTracker(), selfId.getSegmentId(), data);
            }

            public void write(TarWriter tarWriter) throws IOException {
                long msb = getSegmentId().getMostSignificantBits();
                long lsb = getSegmentId().getLeastSignificantBits();
                tarWriter.writeEntry(msb, lsb, data, 0, data.length);
            }

            public UUID getUUID() {
                return newUUID(getSegmentId());
            }

            private SegmentId getSegmentId() {
                return selfId.getSegmentId();
            }

            public Set<Node> getReferences() {
                Set<Node> references = newHashSet();
                for (SegmentId segmentId : segment.getReferencedIds()) {
                    references.add(nodes.get(newUUID(segmentId)));
                }
                references.remove(this);
                return references;
            }

            @Override
            public String toString() {
                return name;
            }

            void addReference(SegmentWriter writer) throws IOException {
                // Need to write a proper list as singleton lists are optimised
                // to just returning the recordId of its single element
                writer.writeList(ImmutableList.of(selfId, selfId));
            }
        }

        public Node createNode(String name, Node... refs) throws IOException {
            RecordId selfId = writer.writeString("id-" + nextNodeNo++);
            for (Node ref : refs) {
                ref.addReference(writer);
            }
            writer.flush();
            SegmentId segmentId = selfId.getSegmentId();
            Node node = new Node(name, selfId, segments.get(segmentId));
            nodes.put(newUUID(segmentId), node);
            return node;
        }

        public Node getNode(UUID uuid) {
            return nodes.get(uuid);
        }

        private static UUID newUUID(SegmentId segmentId) {
            return new UUID(segmentId.getMostSignificantBits(), segmentId.getLeastSignificantBits());
        }
    }

}
