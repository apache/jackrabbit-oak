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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
        batch.addEntry("a", new byte[NodeStateEntryBatch.MIN_BUFFER_SIZE - 4 - 2]); // Needs 4 bytes for the length and 2 for the key
        assertThrows(NodeStateEntryBatch.BufferFullException.class, () -> batch.addEntry("b", new byte[NodeStateEntryBatch.MIN_BUFFER_SIZE]));

        assertEquals(NodeStateEntryBatch.MIN_BUFFER_SIZE, batch.sizeOfEntries());
        assertEquals(1, batch.numberOfEntries());
        assertThrows(NodeStateEntryBatch.BufferFullException.class, () -> batch.addEntry("b", new byte[1]));
    }

    @Test
    public void flipAndResetBuffer() throws IOException {
        int sizeOfEntry = NodeStateEntryBatch.MIN_BUFFER_SIZE-4;
        String key = "a";
        int jsonNodeLength = sizeOfEntry-2; // minus key and pipe characters

        NodeStateEntryBatch batch = NodeStateEntryBatch.createNodeStateEntryBatch(NodeStateEntryBatch.MIN_BUFFER_SIZE, 10);
        byte[] jsonNodeBytes = new byte[jsonNodeLength];
        for (int i = 0; i < jsonNodeLength; i++) {
            jsonNodeBytes[i] = (byte) (i % 127);
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write("a|".getBytes(StandardCharsets.UTF_8));
        baos.write(jsonNodeBytes);
        byte[] expectedContents = baos.toByteArray();
        int size = batch.addEntry(key, jsonNodeBytes);
        assertEquals(size, sizeOfEntry);
        assertEquals(batch.getBuffer().position(), sizeOfEntry + 4);

        batch.flip();

        ByteBuffer buffer = batch.getBuffer();
        assertEquals(buffer.position(), 0);
        assertEquals(buffer.remaining(), sizeOfEntry + 4);
        assertEquals(sizeOfEntry, buffer.getInt());
        byte[] entryData = new byte[sizeOfEntry];
        buffer.get(entryData);
        assertEquals(buffer.position(), sizeOfEntry + 4);
        assertArrayEquals(expectedContents, entryData);

        batch.reset();

        assertEquals(0, batch.numberOfEntries());
        assertEquals(0, batch.getBuffer().position());
        assertEquals(NodeStateEntryBatch.MIN_BUFFER_SIZE, batch.getBuffer().remaining());
    }
}