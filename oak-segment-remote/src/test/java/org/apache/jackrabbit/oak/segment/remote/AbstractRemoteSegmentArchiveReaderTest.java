package org.apache.jackrabbit.oak.segment.remote;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.file.tar.SegmentGraph;
import org.apache.jackrabbit.oak.segment.remote.AbstractRemoteSegmentArchiveReader.ArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AbstractRemoteSegmentArchiveReaderTest {

    private static class TestSegmentArchiveReader extends AbstractRemoteSegmentArchiveReader {

        public TestSegmentArchiveReader(String archiveName, Iterable<ArchiveEntry> entries) {
            super(new IOMonitorAdapter(), archiveName, entries);
        }

        @Override
        protected void doReadSegmentToBuffer(String segmentFileName, Buffer buffer) throws IOException {
            for (int i = 0; i < buffer.limit(); i++) {
                buffer.put((byte) 1);
            }
        }

        @Override
        protected Buffer doReadDataFile(String extension) throws IOException {
            return null;
        }

        @Override
        protected File archivePathAsFile() {
            return new File(getName());
        }

    }

    private static final List<UUID> SEGMENT_UUIDS = List.of(
            new UUID(0L, 0L),
            new UUID(0L, 1L),
            new UUID(0L, 2L),
            new UUID(0L, 3L),
            new UUID(0L, 4L)
    );

    private TestSegmentArchiveReader reader;

    @Before
    public void setup() {

        ArrayList<ArchiveEntry> archiveEntries = new ArrayList<>(SEGMENT_UUIDS.size() + 2);
        archiveEntries.add(new ArchiveEntry(15));
        archiveEntries.add(new ArchiveEntry(35));
        for (int i = 0; i < SEGMENT_UUIDS.size(); i++) {
            UUID uuid = SEGMENT_UUIDS.get(i);
            archiveEntries.add(new ArchiveEntry(new RemoteSegmentArchiveEntry(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), i, 20, 0, 0, true)));
        }

        // sort in random order to make sure the reader sorts them correctly by their position
        Random random = new Random();
        archiveEntries.sort(Comparator.comparing(e -> random.nextInt(2) - 1));

        reader = new TestSegmentArchiveReader("data00000a.tar", archiveEntries);
    }

    @Test
    public void testReadSegment() throws IOException {
        Buffer buffer = reader.readSegment(0L, 1L);
        assertNotNull(buffer);
        assertEquals(20, buffer.limit());
        for (int i = 0; i < buffer.limit(); i++) {
            assertEquals(1, buffer.get(i));
        }
    }

    @Test
    public void testReadNonExistentSegment() throws IOException {
        Buffer buffer = reader.readSegment(1L, 3L);
        assertNull(buffer);
    }

    @Test
    public void testGetArchiveSize() {
        assertEquals(150, reader.length());
    }

    @Test
    public void testIsRemote() {
        assertTrue(reader.isRemote());
    }

    @Test
    public void testGetEntrySize() {
        assertEquals(10, reader.getEntrySize(10));
        assertEquals(20, reader.getEntrySize(20));
    }

    @Test
    public void testGetBinaryReferences() throws IOException {
        assertNull(reader.getBinaryReferences());
    }

    @Test
    public void testGetGraph() throws IOException {
        SegmentGraph graph = reader.getGraph();
        assertNotNull(graph);
    }

    @Test
    public void testGetName() {
        assertEquals("data00000a.tar", reader.getName());
    }

    @Test
    public void testClose() {
        try {
            reader.close();
        } catch (Exception e) {
            fail("Close should not throw an exception");
        }
    }

    @Test
    public void testContainsSegment() {
        SEGMENT_UUIDS.forEach(uuid -> assertTrue(reader.containsSegment(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits())));
        assertFalse(reader.containsSegment(1L, 3L));
    }

    @Test
    public void testListSegments() {
        var segments = reader.listSegments();
        assertEquals(5, segments.size());
        segments.forEach(e -> assertEquals(0L, e.getMsb()));
        for (int i = 0; i < segments.size(); i++) {
            assertEquals(i, segments.get(i).getLsb()); // LSBs are set up to be the same as the position
        }
    }

    @Test
    public void testGetSegmentUUIDs() {
        assertEquals(Set.copyOf(SEGMENT_UUIDS), reader.getSegmentUUIDs());
    }
}
