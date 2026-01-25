package org.apache.jackrabbit.oak.segment.remote;

import org.junit.Test;

import static org.junit.Assert.*;

public class RemoteSegmentArchiveEntryTest {

    private final RemoteSegmentArchiveEntry entry = new RemoteSegmentArchiveEntry(1L, 2L, 0, 128, 3, 4, true);

    @Test
    public void getMsb() {
        assertEquals(1, entry.getMsb());
    }

    @Test
    public void getLsb() {
        assertEquals(2, entry.getLsb());
    }

    @Test
    public void getPosition() {
        assertEquals(0, entry.getPosition());
    }

    @Test
    public void getLength() {
        assertEquals(128, entry.getLength());
    }

    @Test
    public void getGeneration() {
        assertEquals(3, entry.getGeneration());
    }

    @Test
    public void getFullGeneration() {
        assertEquals(4, entry.getFullGeneration());
    }

    @Test
    public void isCompacted() {
        assertTrue(entry.isCompacted());
    }

    @Test
    public void getUuid() {
        assertSame("The same UUID instance must be returned for different calls", entry.getUuid(), entry.getUuid());
        assertEquals(entry.getMsb(), entry.getUuid().getMostSignificantBits());
        assertEquals(entry.getLsb(), entry.getUuid().getLeastSignificantBits());
    }
}
