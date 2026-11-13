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
package org.apache.jackrabbit.oak.segment.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.segment.LongIdMappingBlobStore;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoader;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests the migration of the binary references between a remote segment store and TAR, in both
 * directions. Archives without binary references are, e.g., remote archives that are still open
 * or were not closed before the Oak process terminated, and TAR archives written without them.
 */
@RunWith(Parameterized.class)
public abstract class SegmentStoreMigratorTestBase {

    public enum Direction { REMOTE_TO_TAR, TAR_TO_REMOTE }

    private static final int MAX_ARCHIVE_SIZE_MB = 1;

    @Parameters(name = "{0}")
    public static List<Object[]> directions() {
        return List.of(new Object[] {Direction.REMOTE_TO_TAR}, new Object[] {Direction.TAR_TO_REMOTE});
    }

    @Parameter
    public Direction direction;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private SegmentNodeStorePersistence source;

    private SegmentNodeStorePersistence target;

    protected abstract SegmentNodeStorePersistence createRemotePersistence() throws Exception;

    protected abstract void deleteRemoteArchiveFile(String archive, String name) throws Exception;

    protected abstract void migrate(SegmentNodeStorePersistence source, SegmentNodeStorePersistence target) throws Exception;

    @Before
    public void setUpPersistences() throws Exception {
        SegmentNodeStorePersistence tar = new TarPersistence(folder.newFolder("tar"));
        SegmentNodeStorePersistence remote = createRemotePersistence();
        source = direction == Direction.REMOTE_TO_TAR ? remote : tar;
        target = direction == Direction.REMOTE_TO_TAR ? tar : remote;
    }

    @Test
    public void binaryReferencesOfClosedArchiveAreMigrated() throws Exception {
        try (FileStore fileStore = newFileStore()) {
            writeExternalBinary(fileStore);
        }
        Map<String, ByteBuffer> sourceBinaryReferences = binaryReferences(source);

        migrate(source, target);

        assertEquals(sourceBinaryReferences, binaryReferences(target));
    }

    @Test
    public void binaryReferencesOfArchivesWithoutThemAreRecovered() throws Exception {
        try (FileStore fileStore = newFileStore()) {
            writeExternalBinary(fileStore);
        }
        Map<String, Set<String>> sourceEntries = binaryReferenceEntries(source);
        for (String archive : sourceEntries.keySet()) {
            removeBinaryReferences(archive);
        }

        migrate(source, target);

        assertEquals(sourceEntries, binaryReferenceEntries(target));
        assertSourceArchivesWithoutBinaryReferences(sourceEntries.keySet());
    }

    @Test
    public void binaryReferencesWithValueInPreviousArchiveAreRecovered() throws Exception {
        BlobStore blobStore = new LongIdMappingBlobStore(LongIdMappingBlobStore.DEDUPLICATED_ID_LENGTH);
        String blobId;
        try (FileStore fileStore = newFileStoreBuilder(blobStore).withMaxFileSize(MAX_ARCHIVE_SIZE_MB).build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            blobId = writeBinary(nodeStore, "first", newBinary());
            writeContentLargerThanMaxArchiveSize(nodeStore);
            writeBinary(nodeStore, "second", new BlobStoreBlob(blobStore, blobId));
        }
        Map<String, Set<String>> sourceEntries = binaryReferenceEntries(source);
        String lastArchive = lastArchiveReferencing(sourceEntries, blobId);
        removeBinaryReferences(lastArchive);

        migrate(source, target);

        assertEquals(sourceEntries, binaryReferenceEntries(target));
        assertSourceArchivesWithoutBinaryReferences(Set.of(lastArchive));
    }

    @Test
    public void binaryReferencesOfOpenArchiveAreMigrated() throws Exception {
        assumeTrue("only remote archives can be read while they are open", direction == Direction.REMOTE_TO_TAR);
        try (FileStore fileStore = newFileStore()) {
            String blobId = writeExternalBinary(fileStore);
            fileStore.flush();

            migrate(source, target);

            Map<String, Set<String>> migratedEntries = binaryReferenceEntries(target);
            assertEquals(new HashSet<>(archiveManager(source).listArchives()), migratedEntries.keySet());
            assertTrue("binary reference " + blobId + " missing after migration",
                    migratedEntries.values().stream().anyMatch(entries -> containsReference(entries, blobId)));
        }
    }

    /**
     * The migrator opens the source store read-only to recover the binary references. A writable
     * store would recover the source archives and write their binary references.
     */
    private void assertSourceArchivesWithoutBinaryReferences(Set<String> archives) throws IOException {
        for (String archive : archives) {
            try (SegmentArchiveReader reader = archiveManager(source).forceOpen(archive)) {
                assertNull(archive + " of the source store was modified", reader.getBinaryReferences());
            }
        }
    }

    private static String lastArchiveReferencing(Map<String, Set<String>> entries, String blobId) {
        List<String> archives = entries.entrySet().stream()
                .filter(archive -> containsReference(archive.getValue(), blobId))
                .map(Map.Entry::getKey)
                .sorted()
                .collect(Collectors.toList());
        assertEquals("archives referencing " + blobId, 2, archives.size());
        return archives.get(1);
    }

    private static boolean containsReference(Set<String> entries, String blobId) {
        return entries.stream().anyMatch(entry -> entry.endsWith("/" + blobId));
    }

    private static Map<String, ByteBuffer> binaryReferences(SegmentNodeStorePersistence persistence) throws IOException {
        Map<String, ByteBuffer> binaryReferences = new HashMap<>();
        SegmentArchiveManager archiveManager = archiveManager(persistence);
        for (String archive : archiveManager.listArchives()) {
            try (SegmentArchiveReader reader = archiveManager.open(archive)) {
                assertNotNull(archive + " not found", reader);
                ByteBuffer archiveBinaryReferences = reader.getBinaryReferences();
                assertNotNull(archive + " has no binary references", archiveBinaryReferences);
                binaryReferences.put(archive, archiveBinaryReferences);
            }
        }
        return binaryReferences;
    }

    private static Map<String, Set<String>> binaryReferenceEntries(SegmentNodeStorePersistence persistence) throws Exception {
        Map<String, Set<String>> entries = new HashMap<>();
        for (Map.Entry<String, ByteBuffer> archive : binaryReferences(persistence).entrySet()) {
            Set<String> archiveEntries = new HashSet<>();
            BinaryReferencesIndexLoader.parseBinaryReferencesIndex(archive.getValue()).forEach(
                    (generation, full, compacted, segment, reference) ->
                            archiveEntries.add(generation + "/" + full + "/" + compacted + "/" + segment + "/" + reference));
            entries.put(archive.getKey(), archiveEntries);
        }
        return entries;
    }

    private void removeBinaryReferences(String archive) throws Exception {
        if (direction == Direction.REMOTE_TO_TAR) {
            removeRemoteFilesWrittenOnClose(archive);
        } else {
            rewriteTarArchiveWithoutBinaryReferences(archive);
        }
    }

    /**
     * Leaves the remote archive as it is after the Oak process terminated before closing it:
     * segments only.
     */
    private void removeRemoteFilesWrittenOnClose(String archive) throws Exception {
        for (String name : List.of(archive + ".brf", archive + ".gph", "closed")) {
            deleteRemoteArchiveFile(archive, name);
        }
    }

    private void rewriteTarArchiveWithoutBinaryReferences(String archive) throws IOException {
        SegmentArchiveManager archiveManager = archiveManager(source);
        Map<SegmentArchiveEntry, byte[]> segments = new LinkedHashMap<>();
        byte[] graph;
        try (SegmentArchiveReader reader = archiveManager.open(archive)) {
            for (SegmentArchiveEntry entry : reader.listSegments()) {
                ByteBuffer buffer = reader.readSegment(entry.getMsb(), entry.getLsb());
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);
                segments.put(entry, data);
            }
            graph = reader.getGraph().write();
        }
        assertTrue(archive + " not deleted", archiveManager.delete(archive));
        SegmentArchiveWriter writer = archiveManager.create(archive);
        for (Map.Entry<SegmentArchiveEntry, byte[]> segment : segments.entrySet()) {
            SegmentArchiveEntry entry = segment.getKey();
            writer.writeSegment(entry.getMsb(), entry.getLsb(), segment.getValue(), 0, segment.getValue().length,
                    entry.getGeneration(), entry.getFullGeneration(), entry.isCompacted());
        }
        writer.writeGraph(graph);
        writer.close();
    }

    private static SegmentArchiveManager archiveManager(SegmentNodeStorePersistence persistence) throws IOException {
        return persistence.createArchiveManager(false, false, new IOMonitorAdapter(),
                new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
    }

    private FileStore newFileStore() throws Exception {
        return newFileStoreBuilder(new MemoryBlobStore()).build();
    }

    private FileStoreBuilder newFileStoreBuilder(BlobStore blobStore) throws IOException {
        return FileStoreBuilder.fileStoreBuilder(folder.newFolder())
                .withCustomPersistence(source)
                .withBlobStore(blobStore);
    }

    private static String writeExternalBinary(FileStore fileStore) throws Exception {
        return writeBinary(SegmentNodeStoreBuilders.builder(fileStore).build(), "binary", newBinary());
    }

    private static Blob newBinary() {
        byte[] data = new byte[100 * 1024];
        new Random(42).nextBytes(data);
        return new ArrayBasedBlob(data);
    }

    private static String writeBinary(SegmentNodeStore nodeStore, String name, Blob binary) throws Exception {
        NodeBuilder root = nodeStore.getRoot().builder();
        root.child("content").setProperty(name, binary);
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        Blob blob = nodeStore.getRoot().getChildNode("content").getProperty(name).getValue(Type.BINARY);
        String blobId = ((SegmentBlob) blob).getBlobId();
        assertNotNull("binary should be stored in the blob store", blobId);
        return blobId;
    }

    private static void writeContentLargerThanMaxArchiveSize(SegmentNodeStore nodeStore) throws Exception {
        Random random = new Random(42);
        NodeBuilder root = nodeStore.getRoot().builder();
        for (int i = 0; i < 2 * MAX_ARCHIVE_SIZE_MB * 1024; i++) {
            byte[] value = new byte[1024];
            random.nextBytes(value);
            root.child("filler").child("node" + i).setProperty("value", Base64.getEncoder().encodeToString(value));
        }
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
