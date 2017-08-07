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

package org.apache.jackrabbit.oak.segment.file.tar.index;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;

public class IndexV2Test {

    @Test
    public void testGetUUIDs() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV2.SIZE);
        buffer.duplicate()
                .putLong(1).putLong(2).putInt(3).putInt(4).putInt(5).putInt(6).put((byte) 0)
                .putLong(7).putLong(8).putInt(9).putInt(10).putInt(11).putInt(12).put((byte) 1);
        Set<UUID> expected = new HashSet<>();
        expected.add(new UUID(1, 2));
        expected.add(new UUID(7, 8));
        assertEquals(expected, new IndexV2(buffer).getUUIDs());
    }

    @Test
    public void testFindEntry() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(4 * IndexEntryV2.SIZE);
        buffer.duplicate()
                .putLong(1).putLong(1).putInt(0).putInt(0).putInt(0).putInt(0).put((byte) 0)
                .putLong(1).putLong(3).putInt(0).putInt(0).putInt(0).putInt(0).put((byte) 0)
                .putLong(3).putLong(1).putInt(0).putInt(0).putInt(0).putInt(0).put((byte) 0)
                .putLong(3).putLong(3).putInt(0).putInt(0).putInt(0).putInt(0).put((byte) 0);
        IndexV2 index = new IndexV2(buffer);
        assertEquals(-1, index.findEntry(1, 0));
        assertEquals(0, index.findEntry(1, 1));
        assertEquals(-1, index.findEntry(1, 2));
        assertEquals(1, index.findEntry(1, 3));
        assertEquals(-1, index.findEntry(3, 0));
        assertEquals(2, index.findEntry(3, 1));
        assertEquals(-1, index.findEntry(3, 2));
        assertEquals(3, index.findEntry(3, 3));
    }

    @Test
    public void testSize() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexEntryV2.SIZE);
        buffer.duplicate()
                .putLong(1).putLong(2).putInt(3).putInt(4).putInt(5).putInt(6).put((byte) 0);
        assertEquals(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE, new IndexV2(buffer).size());
    }

    @Test
    public void testCount() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV2.SIZE);
        buffer.duplicate()
                .putLong(1).putLong(2).putInt(3).putInt(4).putInt(5).putInt(6).put((byte) 0)
                .putLong(7).putLong(8).putInt(9).putInt(10).putInt(11).putInt(12).put((byte) 1);
        assertEquals(2, new IndexV2(buffer).count());
    }

    @Test
    public void testEntry() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexEntryV2.SIZE);
        buffer.duplicate()
                .putLong(1).putLong(2).putInt(3).putInt(4).putInt(5).putInt(6).put((byte) 1);
        IndexEntryV2 entry = new IndexV2(buffer).entry(0);
        assertEquals(1, entry.getMsb());
        assertEquals(2, entry.getLsb());
        assertEquals(3, entry.getPosition());
        assertEquals(4, entry.getLength());
        assertEquals(5, entry.getGeneration());
        assertEquals(6, entry.getFullGeneration());
        assertEquals(true, entry.isCompacted());
    }

}
