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
package org.apache.jackrabbit.oak.segment.file.tar;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.UUID.randomUUID;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.api.IllegalRepositoryStateException;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.file.tar.TarFiles.CleanupResult;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TarFilesTest {

    public static final int MAX_FILE_SIZE = 512 * 1024;

    private static final Random random = new Random();

    private static byte[] randomData() {
        byte[] buffer = new byte[512];
        random.nextBytes(buffer);
        return buffer;
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    protected TarFiles tarFiles;

    @Before
    public void setUp() throws Exception {
        tarFiles = TarFiles.builder()
            .withDirectory(folder.getRoot())
            .withTarRecovery((id, data, recovery) -> {
                // Intentionally left blank
            })
            .withIOMonitor(new IOMonitorAdapter())
            .withFileStoreMonitor(new FileStoreMonitorAdapter())
            .withMaxFileSize(MAX_FILE_SIZE)
            .withRemoteStoreMonitor(new RemoteStoreMonitorAdapter())
            .build();
    }

    @After
    public void tearDown() throws Exception {
        if (tarFiles != null) {
            tarFiles.close();
            tarFiles = null;
        }
    }

    private void writeSegment(UUID id) throws IOException {
        writeSegment(id, randomData());
    }

    private void writeSegmentWithReferences(UUID id, UUID... references) throws IOException {
        writeSegmentWithReferences(id, randomData(), references);
    }

    private void writeSegmentWithReferences(UUID id, byte[] buffer, UUID... references) throws IOException {
        tarFiles.writeSegment(id, buffer, 0, buffer.length, newGCGeneration(1, 1, false), new HashSet<>(asList(references)), emptySet());
    }

    private void writeSegmentWithBinaryReferences(UUID id, String... references) throws IOException {
        writeSegmentWithBinaryReferences(id, newGCGeneration(1, 1, false), references);
    }

    private void writeSegmentWithBinaryReferences(UUID id, GCGeneration generation, String... references) throws IOException {
        writeSegmentWithBinaryReferences(id, randomData(), generation, references);
    }

    private void writeSegmentWithBinaryReferences(UUID id, byte[] buffer, GCGeneration generation, String... binaryReferences) throws IOException {
        tarFiles.writeSegment(id, buffer, 0, buffer.length, generation, emptySet(), new HashSet<>(asList(binaryReferences)));
    }

    private void writeSegment(UUID id, byte[] buffer) throws IOException {
        tarFiles.writeSegment(id, buffer, 0, buffer.length, newGCGeneration(1, 1, false), emptySet(), emptySet());
    }

    private boolean containsSegment(UUID id) {
        return tarFiles.containsSegment(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    private byte[] readSegment(UUID id) {
        Buffer buffer = tarFiles.readSegment(id.getMostSignificantBits(), id.getLeastSignificantBits());
        if (buffer == null) {
            return null;
        }
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }

    @Test
    public void testInitialSize() throws Exception {
        assertEquals(0, tarFiles.size());
    }

    @Test
    public void testWriterSize() throws Exception {
        UUID id = randomUUID();
        writeSegment(id);
        assertTrue(tarFiles.size() >= 0);
    }

    @Test
    public void testReaderWriterSize() throws Exception {
        UUID id = randomUUID();
        writeSegment(id);
        tarFiles.newWriter();
        assertTrue(tarFiles.size() >= 0);
    }

    @Test
    public void testInitialReaderCount() throws Exception {
        assertEquals(0, tarFiles.readerCount());
    }

    @Test
    public void testReaderCount() throws Exception {
        UUID id = randomUUID();
        writeSegment(id);
        tarFiles.newWriter();
        assertEquals(1, tarFiles.readerCount());
    }

    @Test
    public void testInitialSegmentCount() {
        assertEquals(0, tarFiles.segmentCount());
    }

    @Test
    public void testSegmentCount() throws IOException {
        UUID id = randomUUID();
        writeSegment(id);
        tarFiles.newWriter();
        assertEquals(1, tarFiles.segmentCount());
    }

    @Test
    public void testInitialContainsSegment() throws Exception {
        UUID id = randomUUID();
        assertFalse(containsSegment(id));
    }

    @Test
    public void testContainsSegmentInWriter() throws Exception {
        UUID id = randomUUID();
        writeSegment(id);
        assertTrue(containsSegment(id));
    }

    @Test
    public void testContainsSegmentInReader() throws Exception {
        UUID id = randomUUID();
        writeSegment(id);
        tarFiles.newWriter();
        assertTrue(containsSegment(id));
    }

    @Test
    public void testInitialReadSegment() throws Exception {
        assertNull(readSegment(randomUUID()));
    }

    @Test
    public void testReadSegmentFromWriter() throws Exception {
        UUID id = randomUUID();
        byte[] data = randomData();
        writeSegment(id, data);
        assertArrayEquals(data, readSegment(id));
    }

    @Test
    public void testReadSegmentFromReader() throws Exception {
        UUID id = randomUUID();
        byte[] data = randomData();
        writeSegment(id, data);
        tarFiles.newWriter();
        assertArrayEquals(data, readSegment(id));
    }

    @Test
    public void testGetIndices() throws Exception {
        UUID a = randomUUID();
        UUID b = randomUUID();
        UUID c = randomUUID();
        UUID d = randomUUID();

        writeSegment(a);
        writeSegment(b);
        tarFiles.newWriter();
        writeSegment(c);
        writeSegment(d);
        tarFiles.newWriter();

        Set<Set<UUID>> expected = new HashSet<>();
        expected.add(new HashSet<>(asList(a, b)));
        expected.add(new HashSet<>(asList(c, d)));
        assertEquals(expected, new HashSet<>(tarFiles.getIndices().values()));
    }

    @Test
    public void testGetGraph() throws Exception {
        UUID a = randomUUID();
        UUID b = randomUUID();
        UUID c = randomUUID();
        UUID d = randomUUID();

        writeSegment(a);
        writeSegmentWithReferences(b, a);
        writeSegmentWithReferences(c, a);
        writeSegmentWithReferences(d, b, c);
        tarFiles.newWriter();

        String file = tarFiles.getIndices().keySet().iterator().next();
        Map<UUID, Set<UUID>> graph = tarFiles.getGraph(file);

        Map<UUID, Set<UUID>> expected = new HashMap<>();
        expected.put(a, emptySet());
        expected.put(b, singleton(a));
        expected.put(c, singleton(a));
        expected.put(d, Set.of(b, c));

        assertEquals(expected, graph);
    }

    @Test
    public void testCollectBlobReferences() throws Exception {
        UUID u1 = randomUUID();
        writeSegmentWithBinaryReferences(u1);
        UUID u2 = randomUUID();
        writeSegmentWithBinaryReferences(u2, "a");
        UUID u3 = randomUUID();
        writeSegmentWithBinaryReferences(u3, "b", "c");

        Set<String> references = new HashSet<>();
        tarFiles.collectBlobReferences(references::add, gen -> false);
        assertEquals("unexpected results for collectBlobReferences, UUIDs were " + u1 + ", " + u2 + " and " + u3,
                new HashSet<>(asList("a", "b", "c")), references);
    }

    @Test
    public void testCollectBlobReferencesWithGenerationFilter() throws Exception {
        GCGeneration ok = newGCGeneration(1, 1, false);
        GCGeneration ko = newGCGeneration(2, 2, false);

        UUID u1 = randomUUID();
        writeSegmentWithBinaryReferences(u1, ok, "ok");
        UUID u2 = randomUUID();
        writeSegmentWithBinaryReferences(u2, ko, "ko");

        Set<String> references = new HashSet<>();
        tarFiles.collectBlobReferences(references::add, ko::equals);
        assertEquals("unexpected results for collectBlobReferences, UUIDs were " + u1 + " and " + u2, singleton("ok"), references);
    }

    @Test
    public void testGetSegmentId() throws Exception {
        UUID a = randomUUID();
        UUID b = randomUUID();
        UUID c = randomUUID();

        writeSegment(a);
        writeSegment(b);
        writeSegment(c);
        tarFiles.newWriter();

        Set<UUID> segmentIds = new HashSet<>();
        tarFiles.getSegmentIds().forEach(segmentIds::add);
        assertEquals(new HashSet<>(asList(a, b, c)), segmentIds);
    }

    @Test
    public void testCleanup() throws Exception {
        UUID a = randomUUID();
        UUID b = randomUUID();
        UUID c = randomUUID();
        UUID d = randomUUID();
        UUID e = randomUUID();

        writeSegment(a);
        writeSegment(b);
        writeSegmentWithReferences(c, a, b);
        writeSegment(d);
        writeSegmentWithReferences(e, a, d);

        // Traverse graph of segments starting with `e`. Mark as reclaimable
        // every segment that are not traversed. The two segments `b` and `c`
        // will be reclaimed.

        CleanupResult result = tarFiles.cleanup(new CleanupContext() {

            @Override
            public Collection<UUID> initialReferences() {
                return singletonList(e);
            }

            @Override
            public boolean shouldReclaim(UUID id, GCGeneration generation, boolean referenced) {
                return !referenced;
            }

            @Override
            public boolean shouldFollow(UUID from, UUID to) {
                return true;
            }

        });

        assertFalse(result.isInterrupted());
        assertFalse(result.getRemovableFiles().isEmpty());
        assertEquals(new HashSet<>(asList(c, b)), result.getReclaimedSegmentIds());
        assertTrue(result.getReclaimedSize() > 0);
    }

    @Test
    public void testCleanupConnectedSegments() throws Exception {
        UUID a = randomUUID();
        UUID b = randomUUID();
        UUID c = randomUUID();
        UUID d = randomUUID();
        UUID e = randomUUID();

        writeSegment(a);
        writeSegment(b);
        writeSegmentWithReferences(c, a, b);
        writeSegment(d);
        writeSegmentWithReferences(e, c, d);

        // Traverse graph of segments starting with `e`. Mark as reclaimable
        // every segment that are not traversed. The segments are all connected,
        // though. No segments will be removed.

        CleanupResult result = tarFiles.cleanup(new CleanupContext() {

            @Override
            public Collection<UUID> initialReferences() {
                return singletonList(e);
            }

            @Override
            public boolean shouldReclaim(UUID id, GCGeneration generation, boolean referenced) {
                return !referenced;
            }

            @Override
            public boolean shouldFollow(UUID from, UUID to) {
                return true;
            }

        });

        assertFalse(result.isInterrupted());
        assertTrue(result.getRemovableFiles().isEmpty());
        assertTrue(result.getReclaimedSegmentIds().isEmpty());
        assertEquals(0, result.getReclaimedSize());
    }

    @Test
    public void testUninitialisedTarFiles() throws IOException {
        tarFiles = TarFiles.builder()
                .withDirectory(folder.getRoot())
                .withTarRecovery((id, data, recovery) -> {
                    // Intentionally left blank
                })
                .withIOMonitor(new IOMonitorAdapter())
                .withFileStoreMonitor(new FileStoreMonitorAdapter())
                .withMaxFileSize(MAX_FILE_SIZE)
                .withRemoteStoreMonitor(new RemoteStoreMonitorAdapter())
                .withInitialisedReadersAndWriters(false)
                .build();

        assertEquals(0, tarFiles.readerCount());
        assertEquals(0, tarFiles.segmentCount());
        assertEquals(0, tarFiles.size());
        assertFalse(tarFiles.containsSegment(0L, 0L));
        assertNull(tarFiles.readSegment(0L, 0L));

        assertThrows(IllegalRepositoryStateException.class, () -> tarFiles.flush());
        assertThrows(IllegalRepositoryStateException.class, () -> writeSegment(randomUUID()));
        assertThrows(IllegalRepositoryStateException.class, () -> tarFiles.cleanup(new CleanupContext() {
            @Override
            public Collection<UUID> initialReferences() {
                return emptySet();
            }

            @Override
            public boolean shouldReclaim(UUID id, GCGeneration generation, boolean referenced) {
                return false;
            }

            @Override
            public boolean shouldFollow(UUID from, UUID to) {
                return false;
            }
        }));

        assertThrows(IllegalRepositoryStateException.class, () -> tarFiles.collectBlobReferences(r -> {}, g -> false));
        assertFalse(tarFiles.getSegmentIds().iterator().hasNext());
        assertTrue(tarFiles.getIndices().isEmpty());
        assertTrue(tarFiles.getGraph("").isEmpty());

        assertThrows(IllegalRepositoryStateException.class, () -> tarFiles.createFileReaper());
    }

    @Test
    public void testWriterRolloverOnExcessiveEntryCount() throws IOException {
        // TOTAL_ENTRIES > WRITER_ENTRY_LIMIT;
        final int WRITER_ENTRY_LIMIT = 8;
        final int TOTAL_ENTRIES = 42;

        IOMonitorAdapter ioMonitor = new IOMonitorAdapter();
        FileStoreMonitor fsMonitor = new FileStoreMonitorAdapter();
        RemoteStoreMonitor remoteStoreMonitor = new RemoteStoreMonitorAdapter();
        File rootDirectory = folder.getRoot();
        File segmentStoreDir = folder.newFolder();

        // create persistence
        SegmentNodeStorePersistence persistence = new TarPersistence(rootDirectory) {
            @Override
            public SegmentArchiveManager createArchiveManager(
                    boolean memoryMapping, boolean offHeapAccess, IOMonitor ioMonitor,
                    FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor
            ) {
                return new SegmentTarManager(
                        segmentStoreDir, fsMonitor, ioMonitor, false, false
                ) {
                    @Override
                    public @NotNull SegmentArchiveWriter create(String archiveName) {
                        return new SegmentTarWriter(new File(segmentStoreDir, archiveName), fsMonitor, ioMonitor) {
                            @Override
                            public int getMaxEntryCount() {
                                return WRITER_ENTRY_LIMIT;
                            }
                        };
                    }
                };
            }
        };
        tarFiles = TarFiles.builder()
                .withDirectory(rootDirectory)
                .withTarRecovery((id, data, recovery) -> {})
                .withIOMonitor(ioMonitor)
                .withFileStoreMonitor(fsMonitor)
                .withMaxFileSize(MAX_FILE_SIZE)
                .withRemoteStoreMonitor(remoteStoreMonitor)
                .withPersistence(persistence)
                .build();

        // write more entries than fit into a single tar file
        for (int i = 0; i < TOTAL_ENTRIES; i++) {
            writeSegment(randomUUID());
        }

        assertEquals(TOTAL_ENTRIES / WRITER_ENTRY_LIMIT, tarFiles.readerCount());
    }
}
