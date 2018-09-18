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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.ListRecord.LEVEL_SIZE;
import static org.apache.jackrabbit.oak.segment.ListRecord.MAX_ELEMENTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.segment.test.TemporaryFileStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class DefaultSegmentWriterTest {

    private static final String HELLO_WORLD = "Hello, World!";

    private final byte[] bytes = HELLO_WORLD.getBytes(Charsets.UTF_8);

    private TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private TemporaryFileStore store = new TemporaryFileStore(folder, false);

    @Rule
    public RuleChain rules = RuleChain.outerRule(folder).around(store);

    private DefaultSegmentWriter writer;

    @Before
    public void setUp() throws Exception {
        writer = defaultSegmentWriterBuilder("test").build(store.fileStore());
    }

    @Test
    public void testBlockRecord() throws IOException {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);
        BlockRecord block = new BlockRecord(blockId, bytes.length);

        // Check reading with all valid positions and lengths
        for (int n = 1; n < bytes.length; n++) {
            for (int i = 0; i + n <= bytes.length; i++) {
                Arrays.fill(bytes, i, i + n, (byte) '.');
                assertEquals(n, block.read(i, bytes, i, n));
                assertEquals(HELLO_WORLD, new String(bytes, Charsets.UTF_8));
            }
        }

        // Check reading with a too long length
        byte[] large = new byte[bytes.length * 2];
        assertEquals(bytes.length, block.read(0, large, 0, large.length));
        assertEquals(HELLO_WORLD, new String(large, 0, bytes.length, Charsets.UTF_8));
    }

    @Test
    public void testListRecord() throws IOException {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);

        ListRecord one = writeList(1, blockId);
        ListRecord level1 = writeList(LEVEL_SIZE, blockId);
        ListRecord level1p = writeList(LEVEL_SIZE + 1, blockId);
        ListRecord level2 = writeList(LEVEL_SIZE * LEVEL_SIZE, blockId);
        ListRecord level2p = writeList(LEVEL_SIZE * LEVEL_SIZE + 1, blockId);

        assertEquals(1, one.size());
        assertEquals(blockId, one.getEntry(0));
        assertEquals(LEVEL_SIZE, level1.size());
        assertEquals(blockId, level1.getEntry(0));
        assertEquals(blockId, level1.getEntry(LEVEL_SIZE - 1));
        assertEquals(LEVEL_SIZE + 1, level1p.size());
        assertEquals(blockId, level1p.getEntry(0));
        assertEquals(blockId, level1p.getEntry(LEVEL_SIZE));
        assertEquals(LEVEL_SIZE * LEVEL_SIZE, level2.size());
        assertEquals(blockId, level2.getEntry(0));
        assertEquals(blockId, level2.getEntry(LEVEL_SIZE * LEVEL_SIZE - 1));
        assertEquals(LEVEL_SIZE * LEVEL_SIZE + 1, level2p.size());
        assertEquals(blockId, level2p.getEntry(0));
        assertEquals(blockId, level2p.getEntry(LEVEL_SIZE * LEVEL_SIZE));

        int count = 0;
        for (RecordId entry : level2p.getEntries()) {
            assertEquals(blockId, entry);
            assertEquals(blockId, level2p.getEntry(count));
            count++;
        }
        assertEquals(LEVEL_SIZE * LEVEL_SIZE + 1, count);
    }

    @Test
    public void testLargeListRecord() throws IOException {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);

        ListRecord one = writeList(MAX_ELEMENTS, blockId);
        one.getEntry(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooLargeListRecord() throws IOException {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);

        ListRecord one = writeList(MAX_ELEMENTS + 1, blockId);
    }

    private ListRecord writeList(int size, RecordId id) throws IOException {
        List<RecordId> list = Collections.nCopies(size, id);
        return new ListRecord(writer.writeList(list), size);
    }

    @Test
    public void testListWithLotsOfReferences() throws IOException { // OAK-1184
        List<RecordId> list = newArrayList();
        for (int i = 0; i < 1000; i++) {
            list.add(new RecordId(store.fileStore().getSegmentIdProvider().newBulkSegmentId(), 0));
        }
        writer.writeList(list);
    }

    @Test
    public void testStringRecord() throws IOException {
        RecordId empty = writer.writeString("");
        RecordId space = writer.writeString(" ");
        RecordId hello = writer.writeString("Hello, World!");

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 2 * Segment.MAX_SEGMENT_SIZE + 1000; i++) {
            builder.append((char) ('0' + i % 10));
        }
        RecordId large = writer.writeString(builder.toString());

        Segment segment = large.getSegmentId().getSegment();

        assertEquals("", store.fileStore().getReader().readString(empty));
        assertEquals(" ", store.fileStore().getReader().readString(space));
        assertEquals("Hello, World!", store.fileStore().getReader().readString(hello));
        assertEquals(builder.toString(), store.fileStore().getReader().readString(large));
    }

    @Test
    public void testMapRecord() throws IOException {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);

        MapRecord zero = new MapRecord(store.fileStore().getReader(), writer.writeMap(null, ImmutableMap.<String, RecordId>of()));
        MapRecord one = new MapRecord(store.fileStore().getReader(), writer.writeMap(null, ImmutableMap.of("one", blockId)));
        MapRecord two = new MapRecord(store.fileStore().getReader(), writer.writeMap(null, ImmutableMap.of("one", blockId, "two", blockId)));
        Map<String, RecordId> map = newHashMap();
        for (int i = 0; i < 1000; i++) {
            map.put("key" + i, blockId);
        }
        MapRecord many = new MapRecord(store.fileStore().getReader(), writer.writeMap(null, map));

        Iterator<MapEntry> iterator;

        assertEquals(0, zero.size());
        assertNull(zero.getEntry("one"));
        iterator = zero.getEntries().iterator();
        assertFalse(iterator.hasNext());

        assertEquals(1, one.size());
        assertEquals(blockId, one.getEntry("one").getValue());
        assertNull(one.getEntry("two"));
        iterator = one.getEntries().iterator();
        assertTrue(iterator.hasNext());
        assertEquals("one", iterator.next().getName());
        assertFalse(iterator.hasNext());

        assertEquals(2, two.size());
        assertEquals(blockId, two.getEntry("one").getValue());
        assertEquals(blockId, two.getEntry("two").getValue());
        assertNull(two.getEntry("three"));
        iterator = two.getEntries().iterator();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertTrue(iterator.hasNext());
        iterator.next();
        assertFalse(iterator.hasNext());

        assertEquals(1000, many.size());
        iterator = many.getEntries().iterator();
        for (int i = 0; i < 1000; i++) {
            assertTrue(iterator.hasNext());
            assertEquals(blockId, iterator.next().getValue());
            assertEquals(blockId, many.getEntry("key" + i).getValue());
        }
        assertFalse(iterator.hasNext());
        assertNull(many.getEntry("foo"));

        Map<String, RecordId> changes = newHashMap();
        changes.put("key0", null);
        changes.put("key1000", blockId);
        MapRecord modified = new MapRecord(store.fileStore().getReader(), writer.writeMap(many, changes));
        assertEquals(1000, modified.size());
        iterator = modified.getEntries().iterator();
        for (int i = 1; i <= 1000; i++) {
            assertTrue(iterator.hasNext());
            assertEquals(blockId, iterator.next().getValue());
            assertEquals(blockId, modified.getEntry("key" + i).getValue());
        }
        assertFalse(iterator.hasNext());
        assertNull(many.getEntry("foo"));
    }

    @Test
    public void testMapRemoveNonExisting() throws IOException {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);

        Map<String, RecordId> changes = newHashMap();
        changes.put("one", null);
        MapRecord zero = new MapRecord(store.fileStore().getReader(), writer.writeMap(null, changes));
        assertEquals(0, zero.size());
    }

    @Test
    public void testWorstCaseMap() throws IOException {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);
        Map<String, RecordId> map = newHashMap();
        char[] key = new char[2];
        for (int i = 0; i <= MapRecord.BUCKETS_PER_LEVEL; i++) {
            key[0] = (char) ('A' + i);
            key[1] = (char) ('\u1000' - key[0] * 31);
            map.put(new String(key), blockId);
        }

        MapRecord bad = new MapRecord(store.fileStore().getReader(), writer.writeMap(null, map));

        assertEquals(map.size(), bad.size());
        Iterator<MapEntry> iterator = bad.getEntries().iterator();
        for (int i = 0; i < map.size(); i++) {
            assertTrue(iterator.hasNext());
            assertEquals('\u1000', iterator.next().getName().hashCode());
        }
        assertFalse(iterator.hasNext());
    }

    @Test  // See OAK-2049
    public void segmentOverflow() throws Exception {
        for (int n = 1; n < 255; n++) {  // 255 = ListRecord.LEVEL_SIZE
            DefaultSegmentWriter writer = defaultSegmentWriterBuilder("test").build(store.fileStore());
            // writer.length == 32  (from the root node)

            // adding 15 strings with 16516 bytes each
            for (int k = 0; k < 15; k++) {
                // 16516 = (Segment.MEDIUM_LIMIT - 1 + 2 + 3)
                // 1 byte per char, 2 byte to store the length and 3 bytes for the
                // alignment to the integer boundary
                writer.writeString(Strings.repeat("abcdefghijklmno".substring(k, k + 1),
                    SegmentTestConstants.MEDIUM_LIMIT - 1));
            }

            // adding 14280 bytes. 1 byte per char, and 2 bytes to store the length
            RecordId x = writer.writeString(Strings.repeat("x", 14278));
            // writer.length == 262052

            // Adding 765 bytes (255 recordIds)
            // This should cause the current segment to flush
            List<RecordId> list = Collections.nCopies(n, x);
            writer.writeList(list);

            writer.flush();
        }
    }

}
