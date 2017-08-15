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

import static org.apache.jackrabbit.oak.segment.file.tar.index.IndexLoader.newIndexLoader;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;

import org.junit.Test;

public class IndexLoaderTest {

    private static Index loadIndex(ByteBuffer buffer) throws Exception {
        return newIndexLoader(1).loadIndex((whence, length) -> {
            ByteBuffer slice = buffer.duplicate();
            slice.position(slice.limit() - whence);
            slice.limit(slice.position() + length);
            return slice.slice();
        });
    }

    private static void assertEntry(IndexEntry entry, long msb, long lsb, int position, int length, int full, int tail, boolean isTail) {
        assertEquals(msb, entry.getMsb());
        assertEquals(lsb, entry.getLsb());
        assertEquals(position, entry.getPosition());
        assertEquals(length, entry.getLength());
        assertEquals(full, entry.getGeneration());
        assertEquals(tail, entry.getFullGeneration());
        assertEquals(isTail, entry.isCompacted());
    }

    @Test(expected = InvalidIndexException.class)
    public void testUnrecognizedMagicNumber() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE);
        buffer.duplicate().putInt(0xDEADBEEF);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Unrecognized magic number", e.getMessage());
            throw e;
        }
    }

    @Test
    public void testLoadIndexV1() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(1).putLong(2).putInt(3).putInt(4).putInt(5)
                .putLong(6).putLong(7).putInt(8).putInt(9).putInt(10)
                .putInt(0x3F5F40E9)
                .putInt(2)
                .putInt(2 * IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE)
                .putInt(IndexLoaderV1.MAGIC);
        Index index = loadIndex(buffer);
        assertNotNull(index);
        assertEquals(2, index.count());
        assertEntry(index.entry(0), 1, 2, 3, 4, 5, 5, true);
        assertEntry(index.entry(1), 6, 7, 8, 9, 10, 10, true);
    }

    @Test
    public void testLoadIndexV2() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(1).putLong(2).putInt(3).putInt(4).putInt(5).putInt(6).put((byte) 0)
                .putLong(7).putLong(8).putInt(9).putInt(10).putInt(11).putInt(12).put((byte) 1)
                .putInt(0xE2138EB4)
                .putInt(2)
                .putInt(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
                .putInt(IndexLoaderV2.MAGIC);
        Index index = loadIndex(buffer);
        assertNotNull(index);
        assertEquals(2, index.count());
        assertEntry(index.entry(0), 1, 2, 3, 4, 5, 6, false);
        assertEntry(index.entry(1), 7, 8, 9, 10, 11, 12, true);
    }

}
