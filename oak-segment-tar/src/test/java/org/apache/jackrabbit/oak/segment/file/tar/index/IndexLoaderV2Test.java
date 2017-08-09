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

    @Test(expected = InvalidIndexException.class)
    public void testInvalidMagic() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexV2.FOOTER_SIZE);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Magic number mismatch", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidCount() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexV2.FOOTER_SIZE);
        buffer.duplicate()
                .putInt(0)
                .putInt(0)
                .putInt(0)
                .putInt(IndexLoaderV2.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid entry count", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidSize() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexV2.FOOTER_SIZE);
        buffer.duplicate()
                .putInt(0)
                .putInt(1)
                .putInt(0)
                .putInt(IndexLoaderV2.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid size", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidSizeAlignment() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexV2.FOOTER_SIZE);
        buffer.duplicate()
                .putInt(0)
                .putInt(1)
                .putInt(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
                .putInt(IndexLoaderV2.MAGIC);
        try {
            loadIndex(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE + 1, buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid size alignment", e.getMessage());
            throw e;
        }
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
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid checksum", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testIncorrectEntryOrderingByMsb() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(1).putLong(0).putInt(0).putInt(1).putInt(0).putInt(0).put((byte) 0)
                .putLong(0).putLong(0).putInt(1).putInt(1).putInt(0).putInt(0).put((byte) 0)
                .putInt(0x40ECBFA7)
                .putInt(2)
                .putInt(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
                .putInt(IndexLoaderV2.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Incorrect entry ordering", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testIncorrectEntryOrderingByLsb() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(0).putLong(1).putInt(0).putInt(1).putInt(0).putInt(0).put((byte) 0)
                .putLong(0).putLong(0).putInt(1).putInt(1).putInt(0).putInt(0).put((byte) 0)
                .putInt(0xE39E49C5)
                .putInt(2)
                .putInt(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
                .putInt(IndexLoaderV2.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Incorrect entry ordering", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testDuplicateEntry() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(0).putLong(0).putInt(0).putInt(1).putInt(0).putInt(0).put((byte) 0)
                .putLong(0).putLong(0).putInt(1).putInt(1).putInt(0).putInt(0).put((byte) 0)
                .putInt(0x29A0BA56)
                .putInt(2)
                .putInt(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
                .putInt(IndexLoaderV2.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Duplicate entry", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidEntryOffset() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(0).putLong(0).putInt(-1).putInt(1).putInt(0).putInt(0).put((byte) 0)
                .putInt(0xA3AE5FF1)
                .putInt(1)
                .putInt(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
                .putInt(IndexLoaderV2.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid entry offset", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidEntryOffsetAlignment() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * (IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE));
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(duplicate.limit() - IndexEntryV2.SIZE - IndexV2.FOOTER_SIZE);
        duplicate
                .putLong(0).putLong(0).putInt(1).putInt(1).putInt(0).putInt(0).put((byte) 0)
            .putInt(0x8B1B0C5)
                .putInt(1)
                .putInt(2 * (IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE))
                .putInt(IndexLoaderV2.MAGIC);
        try {
            loadIndex(2, buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid entry offset alignment", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidEntrySize() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(0).putLong(0).putInt(0).putInt(0).putInt(0).putInt(0).put((byte) 0)
                .putInt(0x7A7C3A8D)
                .putInt(1)
                .putInt(IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
                .putInt(IndexLoaderV2.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid entry size", e.getMessage());
            throw e;
        }
    }

    @Test
    public void testLoadIndex() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(0).putLong(0).putInt(0).putInt(1).putInt(0).putInt(0).put((byte) 0)
                .putLong(0).putLong(1).putInt(1).putInt(1).putInt(0).putInt(0).put((byte) 0)
                .putInt(0xC6F20CB7)
                .putInt(2)
                .putInt(2 * IndexEntryV2.SIZE + IndexV2.FOOTER_SIZE)
                .putInt(IndexLoaderV2.MAGIC);
        assertNotNull(loadIndex(buffer));
    }

}
