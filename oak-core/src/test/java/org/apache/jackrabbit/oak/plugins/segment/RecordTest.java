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

import static com.google.common.collect.Lists.newArrayList;
import static junit.framework.Assert.fail;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
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

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class RecordTest {

    private String hello = "Hello, World!";

    private byte[] bytes = hello.getBytes(Charsets.UTF_8);

    private SegmentStore store = new MemoryStore();

    private SegmentWriter writer = store.getTracker().getWriter();

    private final Random random = new Random(0xcafefaceL);

    @Test
    public void testBlockRecord() {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);
        BlockRecord block = new BlockRecord(blockId, bytes.length);

        // Check reading with all valid positions and lengths
        for (int n = 1; n < bytes.length; n++) {
            for (int i = 0; i + n <= bytes.length; i++) {
                Arrays.fill(bytes, i, i + n, (byte) '.');
                assertEquals(n, block.read(i, bytes, i, n));
                assertEquals(hello, new String(bytes, Charsets.UTF_8));
            }
        }

        // Check reading with a too long length
        byte[] large = new byte[bytes.length * 2];
        assertEquals(bytes.length, block.read(0, large, 0, large.length));
        assertEquals(hello, new String(large, 0, bytes.length, Charsets.UTF_8));
    }

    @Test
    public void testListRecord() {
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

        try {
            int count = 0;
            for (RecordId entry : level2p.getEntries()) {
                assertEquals(blockId, entry);
                assertEquals(blockId, level2p.getEntry(count));
                count++;
            }
            assertEquals(LEVEL_SIZE * LEVEL_SIZE + 1, count);
        } catch (IllegalArgumentException e) {
            fail("OAK-1287");
        }
    }

    private ListRecord writeList(int size, RecordId id) {
        List<RecordId> list = Collections.nCopies(size, id);
        return new ListRecord(writer.writeList(list), size);
    }

    @Test
    public void testListWithLotsOfReferences() { // OAK-1184
        SegmentTracker factory = store.getTracker();
        List<RecordId> list = newArrayList();
        for (int i = 0; i < 1000; i++) {
            list.add(new RecordId(factory.newBulkSegmentId(), 0));
        }
        writer.writeList(list);
    }

    @Test
    public void testStreamRecord() throws IOException {
        checkRandomStreamRecord(0);
        checkRandomStreamRecord(1);
        checkRandomStreamRecord(0x79);
        checkRandomStreamRecord(0x80);
        checkRandomStreamRecord(0x4079);
        checkRandomStreamRecord(0x4080);
        checkRandomStreamRecord(SegmentWriter.BLOCK_SIZE);
        checkRandomStreamRecord(SegmentWriter.BLOCK_SIZE + 1);
        checkRandomStreamRecord(Segment.MAX_SEGMENT_SIZE);
        checkRandomStreamRecord(Segment.MAX_SEGMENT_SIZE + 1);
        checkRandomStreamRecord(Segment.MAX_SEGMENT_SIZE * 2);
        checkRandomStreamRecord(Segment.MAX_SEGMENT_SIZE * 2 + 1);
    }

    private void checkRandomStreamRecord(int size) throws IOException {
        byte[] source = new byte[size];
        random.nextBytes(source);

        Blob value = writer.writeStream(new ByteArrayInputStream(source));
        InputStream stream = value.getNewStream();
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
        for (int i = 0; i < 2 * Segment.MAX_SEGMENT_SIZE + 1000; i++) {
            builder.append((char) ('0' + i % 10));
        }
        RecordId large = writer.writeString(builder.toString());

        Segment segment = large.getSegmentId().getSegment();

        assertEquals("", segment.readString(empty));
        assertEquals(" ", segment.readString(space));
        assertEquals("Hello, World!", segment.readString(hello));
        assertEquals(builder.toString(), segment.readString(large));
    }

    @Test
    public void testMapRecord() {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);

        MapRecord zero = writer.writeMap(
                null, ImmutableMap.<String, RecordId>of());
        MapRecord one = writer.writeMap(
                null, ImmutableMap.of("one", blockId));
        MapRecord two = writer.writeMap(
                null, ImmutableMap.of("one", blockId, "two", blockId));
        Map<String, RecordId> map = Maps.newHashMap();
        for (int i = 0; i < 1000; i++) {
            map.put("key" + i, blockId);
        }
        MapRecord many = writer.writeMap(null, map);

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

        Map<String, RecordId> changes = Maps.newHashMap();
        changes.put("key0", null);
        changes.put("key1000", blockId);
        MapRecord modified = writer.writeMap(many, changes);
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
    public void testWorstCaseMap() {
        RecordId blockId = writer.writeBlock(bytes, 0, bytes.length);
        Map<String, RecordId> map = Maps.newHashMap();
        char[] key = new char[2];
        for (int i = 0; i <= MapRecord.BUCKETS_PER_LEVEL; i++) {
            key[0] = (char) ('A' + i);
            key[1] = (char) ('\u1000' - key[0] * 31);
            map.put(new String(key), blockId);
        }

        MapRecord bad = writer.writeMap(null, map);

        assertEquals(map.size(), bad.size());
        Iterator<MapEntry> iterator = bad.getEntries().iterator();
        for (int i = 0; i < map.size(); i++) {
            assertTrue(iterator.hasNext());
            assertEquals('\u1000', iterator.next().getName().hashCode());
        }
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testEmptyNode() {
        NodeState before = EMPTY_NODE;
        NodeState after = writer.writeNode(before);
        assertEquals(before, after);
    }

    @Test
    public void testSimpleNode() {
        NodeState before = EMPTY_NODE.builder()
                .setProperty("foo", "abc")
                .setProperty("bar", 123)
                .setProperty("baz", Math.PI)
                .getNodeState();
        NodeState after = writer.writeNode(before);
        assertEquals(before, after);
    }

    @Test
    public void testDeepNode() {
        NodeBuilder root = EMPTY_NODE.builder();
        NodeBuilder builder = root;
        for (int i = 0; i < 1000; i++) {
            builder = builder.child("test");
        }
        NodeState before = builder.getNodeState();
        NodeState after = writer.writeNode(before);
        assertEquals(before, after);
    }

    @Test
    public void testManyMapDeletes() {
        NodeBuilder builder = EMPTY_NODE.builder();
        for (int i = 0; i < 1000; i++) {
            builder.child("test" + i);
        }
        NodeState before = writer.writeNode(builder.getNodeState());
        assertEquals(builder.getNodeState(), before);

        builder = before.builder();
        for (int i = 0; i < 900; i++) {
            builder.getChildNode("test" + i).remove();
        }
        NodeState after = writer.writeNode(builder.getNodeState());
        assertEquals(builder.getNodeState(), after);
    }

    @Test
    public void testMultiValuedBinaryPropertyAcrossSegments()
            throws IOException {
        // biggest possible inlined value record
        byte[] data = new byte[Segment.MEDIUM_LIMIT - 1];
        random.nextBytes(data);

        // create enough copies of the value to fill a full segment
        List<Blob> blobs = newArrayList();
        while (blobs.size() * data.length < Segment.MAX_SEGMENT_SIZE) {
            blobs.add(writer.writeStream(new ByteArrayInputStream(data)));
        }

        // write a simple node that'll now be stored in a separate segment
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("test", blobs, BINARIES);
        NodeState state = writer.writeNode(builder.getNodeState());

        // all the blobs should still be accessible, even if they're
        // referenced from another segment
        for (Blob blob : state.getProperty("test").getValue(BINARIES)) {
            try {
                blob.getNewStream().close();
            } catch (IllegalStateException e) {
                fail("OAK-1374");
            }
        }
    }

}
