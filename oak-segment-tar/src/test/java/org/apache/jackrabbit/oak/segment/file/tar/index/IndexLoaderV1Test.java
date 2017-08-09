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

public class IndexLoaderV1Test {

    private static IndexV1 loadIndex(ByteBuffer buffer) throws Exception {
        return loadIndex(1, buffer);
    }

    private static IndexV1 loadIndex(int blockSize, ByteBuffer buffer) throws Exception {
        return new IndexLoaderV1(blockSize).loadIndex((whence, length) -> {
            ByteBuffer slice = buffer.duplicate();
            slice.position(slice.limit() - whence);
            slice.limit(slice.position() + length);
            return slice.slice();
        });
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidMagic() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexV1.FOOTER_SIZE);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Magic number mismatch", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidCount() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexV1.FOOTER_SIZE);
        buffer.duplicate()
                .putInt(0)
                .putInt(0)
                .putInt(0)
                .putInt(IndexLoaderV1.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid entry count", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidSize() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexV1.FOOTER_SIZE);
        buffer.duplicate()
                .putInt(0)
                .putInt(1)
                .putInt(0)
                .putInt(IndexLoaderV1.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid size", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidSizeAlignment() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexV1.FOOTER_SIZE);
        buffer.duplicate()
                .putInt(0)
                .putInt(1)
                .putInt(IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE)
                .putInt(IndexLoaderV1.MAGIC);
        try {
            loadIndex(IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE + 1, buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid size alignment", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidChecksum() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(1).putLong(2).putInt(3).putInt(4).putInt(5)
                .putInt(0)
                .putInt(1)
                .putInt(IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE)
                .putInt(IndexLoaderV1.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid checksum", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testIncorrectEntryOrderingByMsb() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(1).putLong(0).putInt(0).putInt(1).putInt(0)
                .putLong(0).putLong(0).putInt(1).putInt(1).putInt(0)
                .putInt(0x4079275A)
                .putInt(2)
                .putInt(2 * IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE)
                .putInt(IndexLoaderV1.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Incorrect entry ordering", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testIncorrectEntryOrderingByLsb() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(0).putLong(1).putInt(0).putInt(1).putInt(0)
                .putLong(0).putLong(0).putInt(1).putInt(1).putInt(0)
                .putInt(0x273698E8)
                .putInt(2)
                .putInt(2 * IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE)
                .putInt(IndexLoaderV1.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Incorrect entry ordering", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testDuplicateEntry() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(0).putLong(0).putInt(0).putInt(1).putInt(0)
                .putLong(0).putLong(0).putInt(1).putInt(1).putInt(0)
                .putInt(0xCF210849)
                .putInt(2)
                .putInt(2 * IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE)
                .putInt(IndexLoaderV1.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Duplicate entry", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidEntryOffset() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(0).putLong(0).putInt(-1).putInt(1).putInt(0)
                .putInt(0x393A67C9)
                .putInt(1)
                .putInt(IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE)
                .putInt(IndexLoaderV1.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid entry offset", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidEntryOffsetAlignment() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * (IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE));
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(duplicate.limit() - IndexEntryV1.SIZE - IndexV1.FOOTER_SIZE);
        duplicate
                .putLong(0).putLong(0).putInt(1).putInt(1).putInt(0)
            .putInt(0xAA6B4A1A)
                .putInt(1)
                .putInt(2 * (IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE))
                .putInt(IndexLoaderV1.MAGIC);
        try {
            loadIndex(2, buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid entry offset alignment", e.getMessage());
            throw e;
        }
    }

    @Test(expected = InvalidIndexException.class)
    public void testInvalidEntrySize() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(0).putLong(0).putInt(0).putInt(0).putInt(0)
                .putInt(0x807077E9)
                .putInt(1)
                .putInt(IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE)
                .putInt(IndexLoaderV1.MAGIC);
        try {
            loadIndex(buffer);
        } catch (InvalidIndexException e) {
            assertEquals("Invalid entry size", e.getMessage());
            throw e;
        }
    }

    @Test
    public void testLoadIndex() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(2 * IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE);
        buffer.duplicate()
                .putLong(0).putLong(0).putInt(0).putInt(1).putInt(0)
                .putLong(0).putLong(1).putInt(1).putInt(1).putInt(0)
                .putInt(0x12B7D1CC)
                .putInt(2)
                .putInt(2 * IndexEntryV1.SIZE + IndexV1.FOOTER_SIZE)
                .putInt(IndexLoaderV1.MAGIC);
        assertNotNull(loadIndex(buffer));
    }

}
