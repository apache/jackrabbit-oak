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
package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TarFileTest {

    private File file;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Before
    public void setUp() throws IOException {
        file = folder.newFile();
    }

    @Test
    public void testWriteAndRead() throws IOException {
        UUID id = UUID.randomUUID();
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits() & (-1 >>> 4); // OAK-1672
        byte[] data = "Hello, World!".getBytes(UTF_8);

        TarWriter writer = new TarWriter(file);
        try {
            writer.writeEntry(msb, lsb, data, 0, data.length, 0);
            assertEquals(ByteBuffer.wrap(data), writer.readEntry(msb, lsb));
        } finally {
            writer.close();
        }

        assertEquals(5120, file.length());

        TarReader reader = TarReader.open(file, false);
        try {
            assertEquals(ByteBuffer.wrap(data), reader.readEntry(msb, lsb));
        } finally {
            reader.close();
        }

        reader = TarReader.open(file, false);
        try {
            assertEquals(ByteBuffer.wrap(data), reader.readEntry(msb, lsb));
        } finally {
            reader.close();
        }
    }

    @Test
    public void testWriteAndReadBinaryReferences() throws Exception {
        try (TarWriter writer = new TarWriter(file)) {
            writer.writeEntry(0x00, 0x00, new byte[] {0x01, 0x02, 0x3}, 0, 3, 0);

            writer.addBinaryReference(1, "r0");
            writer.addBinaryReference(1, "r1");
            writer.addBinaryReference(1, "r2");
            writer.addBinaryReference(1, "r3");

            writer.addBinaryReference(2, "r4");
            writer.addBinaryReference(2, "r5");
            writer.addBinaryReference(2, "r6");

            writer.addBinaryReference(3, "r7");
            writer.addBinaryReference(3, "r8");
        }

        try (TarReader reader = TarReader.open(file, false)) {
            Map<Integer, Set<String>> brf = reader.getBinaryReferences();

            assertNotNull(brf);

            assertEquals(newHashSet(1, 2, 3), brf.keySet());

            assertEquals(newHashSet("r0", "r1", "r2", "r3"), brf.get(1));
            assertEquals(newHashSet("r4", "r5", "r6"), brf.get(2));
            assertEquals(newHashSet("r7", "r8"), brf.get(3));
        }
    }

}
