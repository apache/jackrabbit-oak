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
package org.apache.jackrabbit.oak.plugins.segment;

import static org.apache.jackrabbit.oak.plugins.segment.ListRecord.LEVEL_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

public class RecordTest {

    private String hello = "Hello, World!";

    private byte[] bytes = hello.getBytes(Charsets.UTF_8);

    private SegmentStore store = new MemoryStore();

    private SegmentWriter writer = new SegmentWriter(store);

    private SegmentReader reader = new SegmentReader(store);

    private final Random random = new Random(0xcafefaceL);

    @Test
    public void testBlockRecord() {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);
        writer.flush();
        BlockRecord block = new BlockRecord(blockId, bytes.length);

        // Check reading with all valid positions and lengths
        for (int n = 1; n < bytes.length; n++) {
            for (int i = 0; i + n <= bytes.length; i++) {
                Arrays.fill(bytes, i, i + n, (byte) '.');
                assertEquals(n, block.read(reader, i, bytes, i, n));
                assertEquals(hello, new String(bytes, Charsets.UTF_8));
            }
        }

        // Check reading with a too long length
        byte[] large = new byte[bytes.length * 2];
        assertEquals(bytes.length, block.read(reader, 0, large, 0, large.length));
        assertEquals(hello, new String(large, 0, bytes.length, Charsets.UTF_8));
    }

    @Test
    public void testListRecord() {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);

        ListRecord zero = writeList(0, blockId);
        ListRecord one = writeList(1, blockId);
        ListRecord level1 = writeList(LEVEL_SIZE, blockId);
        ListRecord level1p = writeList(LEVEL_SIZE + 1, blockId);
        ListRecord level2 = writeList(LEVEL_SIZE * LEVEL_SIZE, blockId);
        ListRecord level2p = writeList(LEVEL_SIZE * LEVEL_SIZE + 1, blockId);
        writer.flush();

        assertEquals(0, zero.size());
        assertEquals(1, one.size());
        assertEquals(blockId, one.getEntry(reader, 0));
        assertEquals(LEVEL_SIZE, level1.size());
        assertEquals(blockId, level1.getEntry(reader, 0));
        assertEquals(blockId, level1.getEntry(reader, LEVEL_SIZE - 1));
        assertEquals(LEVEL_SIZE + 1, level1p.size());
        assertEquals(blockId, level1p.getEntry(reader, 0));
        assertEquals(blockId, level1p.getEntry(reader, LEVEL_SIZE));
        assertEquals(LEVEL_SIZE * LEVEL_SIZE, level2.size());
        assertEquals(blockId, level2.getEntry(reader, 0));
        assertEquals(blockId, level2.getEntry(reader, LEVEL_SIZE * LEVEL_SIZE - 1));
        assertEquals(LEVEL_SIZE * LEVEL_SIZE + 1, level2p.size());
        assertEquals(blockId, level2p.getEntry(reader, 0));
        assertEquals(blockId, level2p.getEntry(reader, LEVEL_SIZE * LEVEL_SIZE));
    }

    private ListRecord writeList(int size, RecordId id) {
        List<RecordId> list = Collections.nCopies(size, id);
        return new ListRecord(writer.writeList(list), size);
    }

    @Test
    public void testStreamRecord() throws IOException {
        checkRandomStreamRecord(0);
        checkRandomStreamRecord(1);
        checkRandomStreamRecord(SegmentWriter.BLOCK_SIZE);
        checkRandomStreamRecord(SegmentWriter.BLOCK_SIZE + 1);
        checkRandomStreamRecord(SegmentWriter.INLINE_SIZE);
        checkRandomStreamRecord(SegmentWriter.INLINE_SIZE + 1);
        checkRandomStreamRecord(store.getMaxSegmentSize());
        checkRandomStreamRecord(store.getMaxSegmentSize() + 1);
        checkRandomStreamRecord(store.getMaxSegmentSize() * 2);
        checkRandomStreamRecord(store.getMaxSegmentSize() * 2 + 1);
    }

    private void checkRandomStreamRecord(int size) throws IOException {
        byte[] source = new byte[size];
        random.nextBytes(source);

        RecordId valueId = writer.writeStream(new ByteArrayInputStream(source));
        writer.flush();

        InputStream stream = reader.readStream(valueId);
        try {
            byte[] b = new byte[349]; // prime number
            int offset = 0;
            for (int n = stream.read(b); n != -1; n = stream.read(b)) {
                for (int i = 0; i < n; i++) {
                    assertEquals(source[offset + i], b[i]);
                }
                offset += n;
            }
            assertEquals(offset, size);
            assertEquals(-1, stream.read());
        } finally {
            stream.close();
        }
    }

    @Test
    public void testStringRecord() {
        RecordId empty = writer.writeString("");
        RecordId space = writer.writeString(" ");
        RecordId hello = writer.writeString("Hello, World!");

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            builder.append((char) ('0' + i % 10));
        }
        RecordId large = writer.writeString(builder.toString());

        writer.flush();

        assertEquals("", reader.readString(empty));
        assertEquals(" ", reader.readString(space));
        assertEquals("Hello, World!", reader.readString(hello));
        assertEquals(builder.toString(), reader.readString(large));
    }

    @Test
    public void testMapRecord() {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);

        MapRecord zero = new MapRecord(writer.writeMap(
                ImmutableMap.<String, RecordId>of()));
        MapRecord one = new MapRecord(writer.writeMap(
                ImmutableMap.of("one", blockId)));
        MapRecord two = new MapRecord(writer.writeMap(
                ImmutableMap.of("one", blockId, "two", blockId)));
        Map<String, RecordId> map = Maps.newHashMap();
        for (int i = 0; i < 1000; i++) {
            map.put("key" + i, blockId);
        }
        MapRecord many = new MapRecord(writer.writeMap(map));

        writer.flush();
        Iterator<MapRecord.Entry> iterator;

        assertEquals(0, zero.size(reader));
        assertNull(zero.getEntry(reader, "one"));
        iterator = zero.getEntries(reader).iterator();
        assertFalse(iterator.hasNext());

        assertEquals(1, one.size(reader));
        assertEquals(blockId, one.getEntry(reader, "one"));
        assertNull(one.getEntry(reader, "two"));
        iterator = one.getEntries(reader).iterator();
        assertTrue(iterator.hasNext());
        assertEquals("one", iterator.next().getKey());
        assertFalse(iterator.hasNext());

        assertEquals(2, two.size(reader));
        assertEquals(blockId, two.getEntry(reader, "one"));
        assertEquals(blockId, two.getEntry(reader, "two"));
        assertNull(two.getEntry(reader, "three"));
        iterator = two.getEntries(reader).iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());

        assertEquals(1000, many.size(reader));
        iterator = many.getEntries(reader).iterator();
        for (int i = 0; i < 1000; i++) {
            assertTrue(iterator.hasNext());
            iterator.next();
            assertEquals(blockId, many.getEntry(reader, "key" + i));
        }
        assertFalse(iterator.hasNext());
        assertNull(many.getEntry(reader, "foo"));
    }

}
