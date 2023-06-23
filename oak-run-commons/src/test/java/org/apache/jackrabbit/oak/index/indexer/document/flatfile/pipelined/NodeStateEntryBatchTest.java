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