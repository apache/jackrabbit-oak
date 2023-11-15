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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class NodeStateEntryBatchTest {

    @Test
    public void testMaximumNumberOfEntries() {
        NodeStateEntryBatch batch = NodeStateEntryBatch.createNodeStateEntryBatch(NodeStateEntryBatch.MIN_BUFFER_SIZE, 2);
        assertFalse(batch.isAtMaxEntries());
        batch.addEntry("a", new byte[1]);
        assertFalse(batch.isAtMaxEntries());
        batch.addEntry("b", new byte[1]);
        assertEquals(2, batch.numberOfEntries());
        assertTrue(batch.isAtMaxEntries());
        assertThrows(NodeStateEntryBatch.BufferFullException.class, () -> batch.addEntry("c", new byte[1]));
    }

    @Test
    public void testMaximumBufferSize() {
        NodeStateEntryBatch batch = NodeStateEntryBatch.createNodeStateEntryBatch(NodeStateEntryBatch.MIN_BUFFER_SIZE, 10);
        int keySize = "a".getBytes(StandardCharsets.UTF_8).length;
        batch.addEntry("a", new byte[NodeStateEntryBatch.MIN_BUFFER_SIZE - 4 - 4 - keySize]); // Needs 4 bytes for the length of the path and of the entry data
        assertThrows(NodeStateEntryBatch.BufferFullException.class, () -> batch.addEntry("b", new byte[NodeStateEntryBatch.MIN_BUFFER_SIZE]));

        assertEquals(NodeStateEntryBatch.MIN_BUFFER_SIZE, batch.sizeOfEntriesBytes());
        assertEquals(1, batch.numberOfEntries());
        assertThrows(NodeStateEntryBatch.BufferFullException.class, () -> batch.addEntry("b", new byte[1]));
    }

    @Test
    public void flipAndResetBuffer() {
        NodeStateEntryBatch batch = NodeStateEntryBatch.createNodeStateEntryBatch(NodeStateEntryBatch.MIN_BUFFER_SIZE, 10);
        int expectedBytesWrittenToBuffer = NodeStateEntryBatch.MIN_BUFFER_SIZE;

        String key = "a";
        int keyLength = "a".getBytes(StandardCharsets.UTF_8).length;
        byte[] jsonNodeBytes = new byte[batch.capacity()-4-4-keyLength];
        for (int i = 0; i < jsonNodeBytes.length; i++) {
            jsonNodeBytes[i] = (byte) (i % 127);
        }

        int bytesWrittenToBuffer = batch.addEntry(key, jsonNodeBytes);
        assertEquals(bytesWrittenToBuffer, expectedBytesWrittenToBuffer);
        assertEquals(batch.getBuffer().position(), expectedBytesWrittenToBuffer);

        batch.flip();

        ByteBuffer buffer = batch.getBuffer();
        assertEquals(buffer.position(), 0);
        assertEquals(buffer.remaining(), expectedBytesWrittenToBuffer);
        assertEquals(keyLength, buffer.getInt());
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        assertEquals(key, new String(keyBytes, StandardCharsets.UTF_8));

        int jsonLength = buffer.getInt();
        byte[] jsonBytes = new byte[jsonLength];
        buffer.get(jsonBytes);
        assertEquals(buffer.position(), expectedBytesWrittenToBuffer);
        assertArrayEquals(jsonNodeBytes, jsonBytes);

        batch.reset();

        assertEquals(0, batch.numberOfEntries());
        assertEquals(0, batch.getBuffer().position());
        assertEquals(NodeStateEntryBatch.MIN_BUFFER_SIZE, batch.getBuffer().remaining());
    }
}