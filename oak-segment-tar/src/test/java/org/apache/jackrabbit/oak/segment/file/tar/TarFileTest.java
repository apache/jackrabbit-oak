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
package org.apache.jackrabbit.oak.segment.file.tar;

import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.internal.util.collections.Sets.newSet;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TarFileTest {

    protected static GCGeneration generation(int full) {
        return newGCGeneration(full, 0, false);
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    protected SegmentArchiveManager archiveManager;

    @Before
    public void setUp() throws IOException {
        archiveManager = new SegmentTarManager(folder.newFolder(), new FileStoreMonitorAdapter(), new IOMonitorAdapter(), false, false);
    }

    protected long getWriteAndReadExpectedSize() {
        return 5120;
    }

    @Test
    public void testWriteAndRead() throws IOException {
        UUID id = UUID.randomUUID();
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits() & (-1 >>> 4); // OAK-1672
        byte[] data = "Hello, World!".getBytes(StandardCharsets.UTF_8);

        try (TarWriter writer = new TarWriter(archiveManager, "data00000a.tar")) {
            writer.writeEntry(msb, lsb, data, 0, data.length, generation(0));
            assertEquals(Buffer.wrap(data), writer.readEntry(msb, lsb));
        }

        try (TarReader reader = TarReader.open("data00000a.tar", archiveManager)) {
            assertEquals(getWriteAndReadExpectedSize(), reader.size());
            assertEquals(Buffer.wrap(data), reader.readEntry(msb, lsb));
        }
    }

    @Test
    public void testGCGeneration() throws Exception {
        UUID id = UUID.randomUUID();
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits();
        String data = "test";
        byte[] buffer = data.getBytes(StandardCharsets.UTF_8);

        try (TarWriter writer = new TarWriter(archiveManager, "data00000a.tar")) {
            writer.writeEntry(msb, lsb, buffer, 0, buffer.length, newGCGeneration(1, 2, false));
        }

        try (TarReader reader = TarReader.open("data00000a.tar", archiveManager)) {
            SegmentArchiveEntry[] entries = reader.getEntries();
            assertEquals(newGCGeneration(1, 2, false), newGCGeneration(entries[0]));
        }
    }

    @Test
    public void testGCGenerationIsCompactedFlagNotErased() throws Exception {
        UUID id = UUID.randomUUID();
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits();
        String data = "test";
        byte[] buffer = data.getBytes(StandardCharsets.UTF_8);

        try (TarWriter writer = new TarWriter(archiveManager, "data00000a.tar")) {
            writer.writeEntry(msb, lsb, buffer, 0, buffer.length, newGCGeneration(1, 2, true));
        }

        try (TarReader reader = TarReader.open("data00000a.tar", archiveManager)) {
            SegmentArchiveEntry[] entries = reader.getEntries();
            assertEquals(newGCGeneration(1, 2, true), newGCGeneration(entries[0]));
        }
    }

    @Test
    public void testWriteAndReadBinaryReferences() throws Exception {
        try (TarWriter writer = new TarWriter(archiveManager, "data00000a.tar")) {
            writer.writeEntry(0x00, 0x00, new byte[] {0x01, 0x02, 0x3}, 0, 3, generation(0));

            writer.addBinaryReference(generation(1), new UUID(1, 0), "r0");
            writer.addBinaryReference(generation(1), new UUID(1, 1), "r1");
            writer.addBinaryReference(generation(1), new UUID(1, 2), "r2");
            writer.addBinaryReference(generation(1), new UUID(1, 3), "r3");

            writer.addBinaryReference(generation(2), new UUID(2, 0), "r4");
            writer.addBinaryReference(generation(2), new UUID(2, 1), "r5");
            writer.addBinaryReference(generation(2), new UUID(2, 2), "r6");

            writer.addBinaryReference(generation(3), new UUID(3, 0), "r7");
            writer.addBinaryReference(generation(3), new UUID(3, 1), "r8");
        }

        Map<UUID, Set<String>> one = new HashMap<>();

        one.put(new UUID(1, 0), newSet("r0"));
        one.put(new UUID(1, 1), newSet("r1"));
        one.put(new UUID(1, 2), newSet("r2"));
        one.put(new UUID(1, 3), newSet("r3"));

        Map<UUID, Set<String>> two = new HashMap<>();

        two.put(new UUID(2, 0), newSet("r4"));
        two.put(new UUID(2, 1), newSet("r5"));
        two.put(new UUID(2, 2), newSet("r6"));

        Map<UUID, Set<String>> three = new HashMap<>();

        three.put(new UUID(3, 0), newSet("r7"));
        three.put(new UUID(3, 1), newSet("r8"));

        Map<GCGeneration, Map<UUID, Set<String>>> expected = new HashMap<>();

        expected.put(generation(1), one);
        expected.put(generation(2), two);
        expected.put(generation(3), three);

        try (TarReader reader = TarReader.open("data00000a.tar", archiveManager)) {
            Map<GCGeneration, Map<UUID, Set<String>>> actual = new HashMap<>();

            reader.getBinaryReferences().forEach((generation, full, compacted, id, reference) -> {
                actual
                    .computeIfAbsent(newGCGeneration(generation, full, compacted), x -> new HashMap<>())
                    .computeIfAbsent(id, x -> new HashSet<>())
                    .add(reference);
            });

            assertEquals(expected, actual);
        }
    }

    @Test
    public void binaryReferencesIndexShouldBeTrimmedDownOnSweep() throws Exception {
        try (TarWriter writer = new TarWriter(archiveManager, "data00000a.tar")) {
            writer.writeEntry(1, 1, new byte[] {1}, 0, 1, generation(1));
            writer.writeEntry(1, 2, new byte[] {1}, 0, 1, generation(1));
            writer.writeEntry(2, 1, new byte[] {1}, 0, 1, generation(2));
            writer.writeEntry(2, 2, new byte[] {1}, 0, 1, generation(2));

            writer.addBinaryReference(generation(1), new UUID(1, 1), "a");
            writer.addBinaryReference(generation(1), new UUID(1, 2), "b");

            writer.addBinaryReference(generation(2), new UUID(2, 1), "c");
            writer.addBinaryReference(generation(2), new UUID(2, 2), "d");
        }

        Set<UUID> sweep = newSet(new UUID(1, 1), new UUID(2, 2));

        try (TarReader reader = TarReader.open("data00000a.tar", archiveManager)) {
            try (TarReader swept = reader.sweep(sweep, new HashSet<>())) {
                assertNotNull(swept);

                Map<UUID, Set<String>> one = new HashMap<>();
                one.put(new UUID(1, 2), newSet("b"));

                Map<UUID, Set<String>> two = new HashMap<>();
                two.put(new UUID(2, 1), newSet("c"));

                Map<GCGeneration, Map<UUID, Set<String>>> references = new HashMap<>();
                references.put(generation(1), one);
                references.put(generation(2), two);

                Map<GCGeneration, Map<UUID, Set<String>>> actual = new HashMap<>();
                swept.getBinaryReferences().forEach((generation, full, compacted, uuid, reference) -> {
                    actual
                        .computeIfAbsent(newGCGeneration(generation, full, compacted), x -> new HashMap<>())
                        .computeIfAbsent(uuid, x -> new HashSet<>())
                        .add(reference);
                });

                assertEquals(references, actual);
            }
        }
    }

    @Test
    public void binaryReferencesIndexShouldContainCompleteGCGeneration() throws Exception {
        try (TarWriter writer = new TarWriter(archiveManager, "data00000a.tar")) {
            writer.writeEntry(0x00, 0x00, new byte[] {0x01, 0x02, 0x3}, 0, 3, generation(0));
            writer.addBinaryReference(newGCGeneration(1, 2, false), new UUID(1, 2), "r1");
            writer.addBinaryReference(newGCGeneration(3, 4, true), new UUID(3, 4), "r2");
        }
        try (TarReader reader = TarReader.open("data00000a.tar", archiveManager)) {
            Set<GCGeneration> expected = new HashSet<>();
            expected.add(newGCGeneration(1, 2, false));
            expected.add(newGCGeneration(3, 4, true));
            Set<GCGeneration> actual = new HashSet<>();
            reader.getBinaryReferences().forEach((generation, full, compacted, segment, reference) -> {
                actual.add(newGCGeneration(generation, full, compacted));
            });
            assertEquals(expected, actual);
        }
    }

    @Test
    public void graphShouldBeTrimmedDownOnSweep() throws Exception {
        try (TarWriter writer = new TarWriter(archiveManager, "data00000a.tar")) {
            writer.writeEntry(1, 1, new byte[] {1}, 0, 1, generation(1));
            writer.writeEntry(1, 2, new byte[] {1}, 0, 1, generation(1));
            writer.writeEntry(1, 3, new byte[] {1}, 0, 1, generation(1));
            writer.writeEntry(2, 1, new byte[] {1}, 0, 1, generation(2));
            writer.writeEntry(2, 2, new byte[] {1}, 0, 1, generation(2));
            writer.writeEntry(2, 3, new byte[] {1}, 0, 1, generation(2));

            writer.addGraphEdge(new UUID(1, 1), new UUID(1, 2));
            writer.addGraphEdge(new UUID(1, 2), new UUID(1, 3));
            writer.addGraphEdge(new UUID(2, 1), new UUID(2, 2));
            writer.addGraphEdge(new UUID(2, 2), new UUID(2, 3));
        }

        Set<UUID> sweep = newSet(new UUID(1, 2), new UUID(2, 3));

        try (TarReader reader = TarReader.open("data00000a.tar", archiveManager)) {
            try (TarReader swept = reader.sweep(sweep, new HashSet<UUID>())) {
                assertNotNull(swept);

                Map<UUID, List<UUID>> graph = new HashMap<>();
                graph.put(new UUID(2, 1), List.of(new UUID(2, 2)));

                assertEquals(graph, swept.getGraph());
            }
        }
    }

}
