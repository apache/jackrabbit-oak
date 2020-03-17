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

package org.apache.jackrabbit.oak.segment.file.tar.binaries;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.zip.CRC32;

import com.google.common.base.Charsets;
import org.junit.Test;

public class BinaryReferencesIndexLoaderTest {

    private static int length(String s) {
        return bytes(s).length;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(Charsets.UTF_8);
    }

    private static int checksum(ByteBuffer buffer) {
        CRC32 checksum = new CRC32();
        int position = buffer.position();
        checksum.update(buffer);
        buffer.position(position);
        return (int) checksum.getValue();
    }

    private static BinaryReferencesIndex loadIndex(ByteBuffer buffer) throws Exception {
        return BinaryReferencesIndexLoader.loadBinaryReferencesIndex((whence, length) -> {
            ByteBuffer slice = buffer.duplicate();
            slice.position(slice.limit() - whence);
            slice.limit(slice.position() + length);
            return slice.slice();
        });
    }

    @Test(expected = InvalidBinaryReferencesIndexException.class)
    public void testUnrecognizedMagicNumber() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        try {
            loadIndex(buffer);
        } catch (InvalidBinaryReferencesIndexException e) {
            assertEquals("Unrecognized magic number", e.getMessage());
            throw e;
        }
    }

    @Test
    public void testLoadV1() throws Exception {
        ByteBuffer entries = ByteBuffer.allocate(512)
            // First generation
            .putInt(1)
            .putInt(2)
            // First generation, first segment
            .putLong(1).putLong(1)
            .putInt(2)
            .putInt(length("1.1.1")).put(bytes("1.1.1"))
            .putInt(length("1.1.2")).put(bytes("1.1.2"))
            // First generation, second segment
            .putLong(1).putLong(2)
            .putInt(2)
            .putInt(length("1.2.1")).put(bytes("1.2.1"))
            .putInt(length("1.2.2")).put(bytes("1.2.2"))
            // Second generation
            .putInt(2)
            .putInt(2)
            // Second generation, second segment
            .putLong(2).putLong(1)
            .putInt(2)
            .putInt(length("2.1.1")).put(bytes("2.1.1"))
            .putInt(length("2.1.2")).put(bytes("2.1.2"))
            // Second generation, second segment
            .putLong(2).putLong(2)
            .putInt(2)
            .putInt(length("2.2.1")).put(bytes("2.2.1"))
            .putInt(length("2.2.2")).put(bytes("2.2.2"));
        entries.flip();

        ByteBuffer buffer = ByteBuffer.allocate(entries.remaining() + BinaryReferencesIndexLoaderV1.FOOTER_SIZE);
        buffer.duplicate()
            .put(entries.duplicate())
            .putInt(checksum(entries))
            .putInt(2)
            .putInt(entries.remaining() + BinaryReferencesIndexLoaderV1.FOOTER_SIZE)
            .putInt(BinaryReferencesIndexLoaderV1.MAGIC);

        BinaryReferencesIndex index = loadIndex(buffer);

        Generation g1 = new Generation(1, 1, true);
        Generation g2 = new Generation(2, 2, true);

        UUID s1 = new UUID(1, 1);
        UUID s2 = new UUID(1, 2);
        UUID s3 = new UUID(2, 1);
        UUID s4 = new UUID(2, 2);

        Map<Generation, Map<UUID, Set<String>>> expected = new HashMap<>();
        expected.put(g1, new HashMap<>());
        expected.put(g2, new HashMap<>());
        expected.get(g1).put(s1, new HashSet<>());
        expected.get(g1).put(s2, new HashSet<>());
        expected.get(g2).put(s3, new HashSet<>());
        expected.get(g2).put(s4, new HashSet<>());
        expected.get(g1).get(s1).addAll(asList("1.1.1", "1.1.2"));
        expected.get(g1).get(s2).addAll(asList("1.2.1", "1.2.2"));
        expected.get(g2).get(s3).addAll(asList("2.1.1", "2.1.2"));
        expected.get(g2).get(s4).addAll(asList("2.2.1", "2.2.2"));

        Map<Generation, Map<UUID, Set<String>>> actual = new HashMap<>();
        index.forEach((generation, full, compacted, id, reference) -> {
            actual
                .computeIfAbsent(new Generation(generation, full, compacted), k -> new HashMap<>())
                .computeIfAbsent(id, k -> new HashSet<>())
                .add(reference);
        });

        assertEquals(expected, actual);
    }

    @Test
    public void testLoadV2() throws Exception {
        ByteBuffer entries = ByteBuffer.allocate(512)
            // First generation
            .putInt(1).putInt(2).put((byte) 0)
            .putInt(2)
            // First generation, first segment
            .putLong(1).putLong(1)
            .putInt(2)
            .putInt(length("1.1.1")).put(bytes("1.1.1"))
            .putInt(length("1.1.2")).put(bytes("1.1.2"))
            // First generation, second segment
            .putLong(1).putLong(2)
            .putInt(2)
            .putInt(length("1.2.1")).put(bytes("1.2.1"))
            .putInt(length("1.2.2")).put(bytes("1.2.2"))
            // Second generation
            .putInt(3).putInt(4).put((byte) 1)
            .putInt(2)
            // Second generation, second segment
            .putLong(2).putLong(1)
            .putInt(2)
            .putInt(length("2.1.1")).put(bytes("2.1.1"))
            .putInt(length("2.1.2")).put(bytes("2.1.2"))
            // Second generation, second segment
            .putLong(2).putLong(2)
            .putInt(2)
            .putInt(length("2.2.1")).put(bytes("2.2.1"))
            .putInt(length("2.2.2")).put(bytes("2.2.2"));
        entries.flip();

        ByteBuffer buffer = ByteBuffer.allocate(entries.remaining() + BinaryReferencesIndexLoaderV2.FOOTER_SIZE);
        buffer.duplicate()
            .put(entries.duplicate())
            .putInt(checksum(entries))
            .putInt(2)
            .putInt(entries.remaining() + BinaryReferencesIndexLoaderV2.FOOTER_SIZE)
            .putInt(BinaryReferencesIndexLoaderV2.MAGIC);

        BinaryReferencesIndex index = loadIndex(buffer);

        Generation g1 = new Generation(1, 2, false);
        Generation g2 = new Generation(3, 4, true);

        UUID s1 = new UUID(1, 1);
        UUID s2 = new UUID(1, 2);
        UUID s3 = new UUID(2, 1);
        UUID s4 = new UUID(2, 2);

        Map<Generation, Map<UUID, Set<String>>> expected = new HashMap<>();
        expected.put(g1, new HashMap<>());
        expected.put(g2, new HashMap<>());
        expected.get(g1).put(s1, new HashSet<>());
        expected.get(g1).put(s2, new HashSet<>());
        expected.get(g2).put(s3, new HashSet<>());
        expected.get(g2).put(s4, new HashSet<>());
        expected.get(g1).get(s1).addAll(asList("1.1.1", "1.1.2"));
        expected.get(g1).get(s2).addAll(asList("1.2.1", "1.2.2"));
        expected.get(g2).get(s3).addAll(asList("2.1.1", "2.1.2"));
        expected.get(g2).get(s4).addAll(asList("2.2.1", "2.2.2"));

        Map<Generation, Map<UUID, Set<String>>> actual = new HashMap<>();
        index.forEach((generation, full, compacted, id, reference) -> {
            actual
                .computeIfAbsent(new Generation(generation, full, compacted), k -> new HashMap<>())
                .computeIfAbsent(id, k -> new HashSet<>())
                .add(reference);
        });

        assertEquals(expected, actual);
    }

}
