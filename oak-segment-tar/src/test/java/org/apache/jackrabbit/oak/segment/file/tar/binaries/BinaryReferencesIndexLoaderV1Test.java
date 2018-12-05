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

import static org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoaderV1.FOOTER_SIZE;
import static org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoaderV1.MAGIC;
import static org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoaderV1.loadBinaryReferencesIndex;
import static org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoaderV1.parseBinaryReferencesIndex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.zip.CRC32;

import com.google.common.base.Charsets;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.junit.Test;

public class BinaryReferencesIndexLoaderV1Test {

    private static int length(String s) {
        return bytes(s).length;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(Charsets.UTF_8);
    }

    private static BinaryReferencesIndex loadIndex(Buffer buffer) throws Exception {
        Buffer data = loadBinaryReferencesIndex((whence, length) -> {
            Buffer slice = buffer.duplicate();
            slice.position(slice.limit() - whence);
            slice.limit(slice.position() + length);
            return slice.slice();
        });
        return parseBinaryReferencesIndex(data);
    }

    private static void assertInvalidBinaryReferencesIndexException(Buffer buffer, String message) throws Exception {
        try {
            loadIndex(buffer);
        } catch (InvalidBinaryReferencesIndexException e) {
            assertEquals(message, e.getMessage());
            throw e;
        }
    }

    private static int checksum(Buffer buffer) {
        CRC32 checksum = new CRC32();
        int position = buffer.position();
        buffer.update(checksum);
        buffer.position(position);
        return (int) checksum.getValue();
    }

    @Test(expected = InvalidBinaryReferencesIndexException.class)
    public void testInvalidMagicNumber() throws Exception {
        Buffer buffer = Buffer.allocate(FOOTER_SIZE);
        assertInvalidBinaryReferencesIndexException(buffer, "Invalid magic number");
    }

    @Test(expected = InvalidBinaryReferencesIndexException.class)
    public void testInvalidCount() throws Exception {
        Buffer buffer = Buffer.allocate(FOOTER_SIZE);
        buffer.duplicate()
            .putInt(0)
            .putInt(-1)
            .putInt(0)
            .putInt(MAGIC);
        assertInvalidBinaryReferencesIndexException(buffer, "Invalid count");
    }

    @Test(expected = InvalidBinaryReferencesIndexException.class)
    public void testInvalidSize() throws Exception {
        Buffer buffer = Buffer.allocate(FOOTER_SIZE);
        buffer.duplicate()
            .putInt(0)
            .putInt(0)
            .putInt(0)
            .putInt(MAGIC);
        assertInvalidBinaryReferencesIndexException(buffer, "Invalid size");
    }

    @Test(expected = InvalidBinaryReferencesIndexException.class)
    public void testInvalidChecksum() throws Exception {
        Buffer entries = Buffer.allocate(512)
            // First generation
            .putInt(1)
            .putInt(2)
            // First generation, first segment
            .putLong(1).putLong(1)
            .putInt(2)
            .putInt(length("1.1.1")).put(bytes("1.1.1"))
            .putInt(length("1.1.2")).put(bytes("1.1.2"))
            // First generation, second segment
            .putLong(1).putLong(2)
            .putInt(2)
            .putInt(length("1.2.1")).put(bytes("1.2.1"))
            .putInt(length("1.2.2")).put(bytes("1.2.2"))
            // Second generation
            .putInt(2)
            .putInt(2)
            // Second generation, second segment
            .putLong(1).putLong(1)
            .putInt(2)
            .putInt(length("2.1.1")).put(bytes("2.1.1"))
            .putInt(length("2.1.2")).put(bytes("2.1.2"))
            // Second generation, second segment
            .putLong(1).putLong(2)
            .putInt(2)
            .putInt(length("2.2.1")).put(bytes("2.2.1"))
            .putInt(length("2.2.2")).put(bytes("2.2.2"));
        entries.flip();

        Buffer buffer = Buffer.allocate(entries.remaining() + FOOTER_SIZE);
        buffer.duplicate()
            .put(entries.duplicate())
            .putInt(checksum(entries) + 1)
            .putInt(2)
            .putInt(entries.remaining() + FOOTER_SIZE)
            .putInt(MAGIC);

        assertInvalidBinaryReferencesIndexException(buffer, "Invalid checksum");
    }

    @Test
    public void testParse() throws Exception {
        Buffer entries = Buffer.allocate(512)
            // First generation
            .putInt(1)
            .putInt(2)
            // First generation, first segment
            .putLong(1).putLong(1)
            .putInt(2)
            .putInt(length("1.1.1")).put(bytes("1.1.1"))
            .putInt(length("1.1.2")).put(bytes("1.1.2"))
            // First generation, second segment
            .putLong(1).putLong(2)
            .putInt(2)
            .putInt(length("1.2.1")).put(bytes("1.2.1"))
            .putInt(length("1.2.2")).put(bytes("1.2.2"))
            // Second generation
            .putInt(2)
            .putInt(2)
            // Second generation, second segment
            .putLong(1).putLong(1)
            .putInt(2)
            .putInt(length("2.1.1")).put(bytes("2.1.1"))
            .putInt(length("2.1.2")).put(bytes("2.1.2"))
            // Second generation, second segment
            .putLong(1).putLong(2)
            .putInt(2)
            .putInt(length("2.2.1")).put(bytes("2.2.1"))
            .putInt(length("2.2.2")).put(bytes("2.2.2"));
        entries.flip();

        Buffer buffer = Buffer.allocate(entries.remaining() + FOOTER_SIZE);
        buffer.duplicate()
            .put(entries.duplicate())
            .putInt(checksum(entries))
            .putInt(2)
            .putInt(entries.remaining() + FOOTER_SIZE)
            .putInt(MAGIC);

        assertNotNull(loadIndex(buffer));
    }
}
