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
import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.junit.Test;

public class IndexLoaderV2Test {

    private static IndexV2 loadIndex(ByteBuffer buffer) throws Exception {
        return loadIndex(1, buffer);
    }

    private static IndexV2 loadIndex(int blockSize, ByteBuffer buffer) throws Exception {
        return new IndexLoaderV2(blockSize).loadIndex((whence, length) -> {
            ByteBuffer slice = buffer.duplicate();
            slice.position(slice.limit() - whence);
            slice.limit(slice.position() + length);
            return slice.slice();
        });
    }

    private static void assertInvalidIndexException(ByteBuffer buffer, String message) throws Exception {
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals(message, e.getMessage());
            throw e;
        }
    }

    private static void assertInvalidIndexException(int blockSize, ByteBuffer buffer, String message) throws Exception {
        try {
            loadIndex(blockSize, buffer);
        } catch (InvalidIndexException e) {
            assertEquals(message, e.getMessage());
            throw e;
        }
    }

    private static int checksum(ByteBuffer buffer) {
        CRC32 checksum = new CRC32();
        int position = buffer.position();
        checksum.update(buffer);
        buffer.position(position);
        return (int) checksum.getValue();
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidMagic() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexV2.FOOTER_SIZE);
        assertInvalidIndexException(buffer, "Magic number mismatch");
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidCount() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexV2.FOOTER_SIZE);
        buffer.duplicate()
            .putInt(0)
            .putInt(0)
            .putInt(0)
            .putInt(IndexLoaderV2.MAGIC);
        assertInvalidIndexException(buffer, "Invalid entry count");
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidSize() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexV2.FOOTER_SIZE);
        buffer.duplicate()
            .putInt(0)
            .putInt(1)
            .putInt(0)
            .putInt(IndexLoaderV2.MAGIC);
        assertInvalidIndexException(buffer, "Invalid size");
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidSizeAlignment() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexV2.FOOTER_SIZE);
        buffer.duplicate()
            .putInt(0)
            .putInt(1)
            .putInt(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
            .putInt(IndexLoaderV2.MAGIC);
        assertInvalidIndexException(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE + 1, buffer, "Invalid size alignment");
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidChecksum() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
            .putLong(1).putLong(2).putInt(3).putInt(4).putInt(5).putInt(6).put((byte) 0)
            .putInt(0)
            .putInt(1)
            .putInt(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
            .putInt(IndexLoaderV2.MAGIC);
        assertInvalidIndexException(buffer, "Invalid checksum");
    }

    @Test(expected = InvalidIndexException.class)
    public void testIncorrectEntryOrderingByMsb() throws Exception {
        ByteBuffer entries = ByteBuffer.allocate(2 * IndexEntryV2.SIZE);
        entries.duplicate()
            .putLong(1).putLong(0).putInt(0).putInt(1).putInt(0).putInt(0).put((byte) 0)
            .putLong(0).putLong(0).putInt(1).putInt(1).putInt(0).putInt(0).put((byte) 0);

        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
            .put(entries.duplicate())
            .putInt(checksum(entries))
            .putInt(2)
            .putInt(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
            .putInt(IndexLoaderV2.MAGIC);

        assertInvalidIndexException(buffer, "Incorrect entry ordering");
    }

    @Test(expected = InvalidIndexException.class)
    public void testIncorrectEntryOrderingByLsb() throws Exception {
        ByteBuffer entries = ByteBuffer.allocate(2 * IndexEntryV2.SIZE);
        entries.duplicate()
            .putLong(0).putLong(1).putInt(0).putInt(1).putInt(0).putInt(0).put((byte) 0)
            .putLong(0).putLong(0).putInt(1).putInt(1).putInt(0).putInt(0).put((byte) 0);

        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
            .put(entries.duplicate())
            .putInt(checksum(entries))
            .putInt(2)
            .putInt(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
            .putInt(IndexLoaderV2.MAGIC);

        assertInvalidIndexException(buffer, "Incorrect entry ordering");
    }

    @Test(expected = InvalidIndexException.class)
    public void testDuplicateEntry() throws Exception {
        ByteBuffer entries = ByteBuffer.allocate(2 * IndexEntryV2.SIZE);
        entries.duplicate()
            .putLong(0).putLong(0).putInt(0).putInt(1).putInt(0).putInt(0).put((byte) 0)
            .putLong(0).putLong(0).putInt(1).putInt(1).putInt(0).putInt(0).put((byte) 0);

        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
            .put(entries.duplicate())
            .putInt(checksum(entries))
            .putInt(2)
            .putInt(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
            .putInt(IndexLoaderV2.MAGIC);

        assertInvalidIndexException(buffer, "Duplicate entry");
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidEntryOffset() throws Exception {
        ByteBuffer entries = ByteBuffer.allocate(IndexEntryV2.SIZE);
        entries.duplicate()
            .putLong(0).putLong(0).putInt(-1).putInt(1).putInt(0).putInt(0).put((byte) 0);

        ByteBuffer buffer = ByteBuffer.allocate(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
            .put(entries.duplicate())
            .putInt(checksum(entries))
            .putInt(1)
            .putInt(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
            .putInt(IndexLoaderV2.MAGIC);

        assertInvalidIndexException(buffer, "Invalid entry offset");
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidEntryOffsetAlignment() throws Exception {
        ByteBuffer entries = ByteBuffer.allocate(IndexEntryV2.SIZE);
        entries.duplicate()
            .putLong(0).putLong(0).putInt(1).putInt(1).putInt(0).putInt(0).put((byte) 0);

        ByteBuffer index = ByteBuffer.allocate(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        index.duplicate()
            .put(entries.duplicate())
            .putInt(checksum(entries))
            .putInt(1)
            .putInt(2 * (IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE))
            .putInt(IndexLoaderV2.MAGIC);

        ByteBuffer buffer = ByteBuffer.allocate(2 * (IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE));
        buffer.mark();
        buffer.position(buffer.limit() - IndexEntryV2.SIZE - IndexV2.FOOTER_SIZE);
        buffer.put(index);
        buffer.reset();

        assertInvalidIndexException(2, buffer, "Invalid entry offset alignment");
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidEntrySize() throws Exception {
        ByteBuffer entries = ByteBuffer.allocate(IndexEntryV2.SIZE);
        entries.duplicate()
            .putLong(0).putLong(0).putInt(0).putInt(0).putInt(0).putInt(0).put((byte) 0);

        ByteBuffer buffer = ByteBuffer.allocate(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
            .put(entries.duplicate())
            .putInt(checksum(entries))
            .putInt(1)
            .putInt(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
            .putInt(IndexLoaderV2.MAGIC);

        assertInvalidIndexException(buffer, "Invalid entry size");
    }

    @Test
    public void testLoadIndex() throws Exception {
        ByteBuffer entries = ByteBuffer.allocate(2 * IndexEntryV2.SIZE);
        entries.duplicate()
            .putLong(0).putLong(0).putInt(0).putInt(1).putInt(0).putInt(0).put((byte) 0)
            .putLong(0).putLong(1).putInt(1).putInt(1).putInt(0).putInt(0).put((byte) 0);

        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
            .put(entries.duplicate())
            .putInt(checksum(entries))
            .putInt(2)
            .putInt(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
            .putInt(IndexLoaderV2.MAGIC);

        assertNotNull(loadIndex(buffer));
    }

}
