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
import static org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoader.loadBinaryReferencesIndex;
import static org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexWriter.newBinaryReferencesIndexWriter;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;

public class BinaryReferencesIndexWriterTest {

    @Test
    public void testWrite() throws Exception {
        UUID s1 = new UUID(1, 1);
        UUID s2 = new UUID(1, 2);
        UUID s3 = new UUID(2, 1);
        UUID s4 = new UUID(2, 2);

        BinaryReferencesIndexWriter writer = newBinaryReferencesIndexWriter();
        writer.addEntry(1, 2, false, s1, "1.1.1");
        writer.addEntry(1, 2, false, s1, "1.1.2");
        writer.addEntry(1, 2, false, s2, "1.2.1");
        writer.addEntry(1, 2, false, s2, "1.2.2");
        writer.addEntry(3, 4, true, s3, "2.1.1");
        writer.addEntry(3, 4, true, s3, "2.1.2");
        writer.addEntry(3, 4, true, s4, "2.2.1");
        writer.addEntry(3, 4, true, s4, "2.2.2");

        byte[] data = writer.write();

        BinaryReferencesIndex index = loadBinaryReferencesIndex((whence, length) -> ByteBuffer.wrap(data, data.length - whence, length));

        Generation g1 = new Generation(1, 2, false);
        Generation g2 = new Generation(3, 4, true);

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
