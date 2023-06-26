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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeStateEntryBatchTest {

    @Test
    public void testMaximumNumberOfEntries() {
        NodeStateEntryBatch batch = NodeStateEntryBatch.createNodeStateEntryBatch(1024, 2);
        assertFalse(batch.isAtMaxEntries());
        batch.addEntry("a", new byte[1]);
        assertFalse(batch.isAtMaxEntries());
        batch.addEntry("b", new byte[1]);
        assertEquals(2, batch.numberOfEntries());
        assertTrue(batch.isAtMaxEntries());
        try {
            batch.addEntry("c", new byte[1]);
            throw new AssertionError("Expected exception");
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void testMaximumBufferSize() {
        NodeStateEntryBatch batch = NodeStateEntryBatch.createNodeStateEntryBatch(128, 10);
        assertTrue(batch.hasSpaceForEntry(new byte[124])); // Needs 4 bytes for the length
        assertFalse(batch.hasSpaceForEntry(new byte[125]));

        batch.addEntry("a", new byte[124]);
        assertEquals(124 + 4, batch.sizeOfEntries());
        assertEquals(1, batch.numberOfEntries());
        assertFalse(batch.hasSpaceForEntry(new byte[1]));
        try {
            batch.addEntry("b", new byte[1]);
            throw new AssertionError("Expected exception");
        } catch (BufferOverflowException e) {
            // expected
        }
    }

    @Test
    public void flipAndResetBuffer() {
        int sizeOfEntry = 124;
        int bufferSize = 1024;
        NodeStateEntryBatch batch = NodeStateEntryBatch.createNodeStateEntryBatch(bufferSize, 10);
        byte[] testArray = new byte[sizeOfEntry];
        for (int i = 0; i < sizeOfEntry; i++) {
            testArray[i] = (byte) (i % 127);
        }
        batch.addEntry("a", testArray);
        assertEquals(batch.getBuffer().position(), sizeOfEntry+4);

        batch.flip();

        ByteBuffer buffer = batch.getBuffer();
        assertEquals(buffer.position(), 0);
        assertEquals(buffer.remaining(), sizeOfEntry+4);
        assertEquals(sizeOfEntry, buffer.getInt());
        byte[] entryData = new byte[sizeOfEntry];
        buffer.get(entryData);
        assertEquals(buffer.position(), sizeOfEntry+4);
        assertArrayEquals(testArray, entryData);

        batch.reset();

        assertEquals(0, batch.numberOfEntries());
        assertEquals(0, batch.getBuffer().position());
        assertEquals(bufferSize, batch.getBuffer().remaining());
    }
}