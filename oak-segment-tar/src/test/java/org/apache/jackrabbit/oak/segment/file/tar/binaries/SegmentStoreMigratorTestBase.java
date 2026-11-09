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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.segment.LongIdMappingBlobStore;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
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

/**
 * Tests the migration of a remote segment store to TAR. The binary references of a remote
 * archive are written only when the archive is closed, so they are missing while the archive is
 * still open and when the Oak process terminated before closing it.
 */
public abstract class SegmentStoreMigratorTestBase {

    private static final int MAX_ARCHIVE_SIZE_MB = 1;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private SegmentNodeStorePersistence source;

    protected abstract SegmentNodeStorePersistence createSourcePersistence() throws Exception;

    protected abstract void migrate(SegmentNodeStorePersistence source, File tarDir) throws Exception;

    protected abstract void deleteArchiveFile(String archive, String name) throws Exception;

    protected abstract boolean archiveFileExists(String archive, String name) throws Exception;

    @Before
    public void setUpSource() throws Exception {
        source = createSourcePersistence();
    }

    @Test
    public void binaryReferencesOfClosedArchiveAreMigrated() throws Exception {
        try (FileStore fileStore = newFileStore()) {
            writeExternalBinary(fileStore);
        }
        Map<String, ByteBuffer> sourceBinaryReferences = binaryReferences(sourceArchiveManager());

        File tarDir = migrateToTar();

        assertEquals(sourceBinaryReferences, binaryReferences(tarArchiveManager(tarDir)));
    }

    @Test
    public void binaryReferencesOfOpenArchiveAreMigrated() throws Exception {
        try (FileStore fileStore = newFileStore()) {
            String blobId = writeExternalBinary(fileStore);
            fileStore.flush();

            File tarDir = migrateToTar();

            Map<String, Set<String>> migratedEntries = binaryReferenceEntries(tarArchiveManager(tarDir));
            assertEquals(new HashSet<>(listArchives()), migratedEntries.keySet());
            assertTrue("binary reference " + blobId + " missing after migration",
                    migratedEntries.values().stream().anyMatch(entries -> containsReference(entries, blobId)));
        }
    }

    @Test
    public void binaryReferencesOfArchiveNotClosedBeforeTerminationAreMigrated() throws Exception {
        try (FileStore fileStore = newFileStore()) {
            writeExternalBinary(fileStore);
        }
        Map<String, Set<String>> sourceEntries = binaryReferenceEntries(sourceArchiveManager());
        for (String archive : listArchives()) {
            removeFilesWrittenOnClose(archive);
        }

        File tarDir = migrateToTar();

        assertEquals(sourceEntries, binaryReferenceEntries(tarArchiveManager(tarDir)));
    }

    @Test
    public void binaryReferencesWithValueInPreviousArchiveAreMigrated() throws Exception {
        BlobStore blobStore = new LongIdMappingBlobStore(LongIdMappingBlobStore.DEDUPLICATED_ID_LENGTH);
        String blobId;
        try (FileStore fileStore = newFileStoreBuilder(blobStore).withMaxFileSize(MAX_ARCHIVE_SIZE_MB).build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            blobId = writeBinary(nodeStore, "first", newBinary());
            writeContentLargerThanMaxArchiveSize(nodeStore);
            writeBinary(nodeStore, "second", new BlobStoreBlob(blobStore, blobId));
        }
        Map<String, Set<String>> sourceEntries = binaryReferenceEntries(sourceArchiveManager());
        String lastArchive = lastArchiveReferencing(sourceEntries, blobId);
        assertThrows("value of the blob id should be in a previous archive", SegmentNotFoundException.class,
                () -> rebuildBinaryReferencesFromArchiveOnly(lastArchive));
        removeFilesWrittenOnClose(lastArchive);

        File tarDir = migrateToTar();

        assertSourceArchiveNotRecovered(lastArchive);
        assertEquals(sourceEntries, binaryReferenceEntries(tarArchiveManager(tarDir)));
    }

    private File migrateToTar() throws Exception {
        File tarDir = folder.newFolder("tar");
        migrate(source, tarDir);
        return tarDir;
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

    private byte[] rebuildBinaryReferencesFromArchiveOnly(String archive) throws IOException {
        try (SegmentArchiveReader reader = sourceArchiveManager().forceOpen(archive)) {
            BinaryReferencesCollector collector = new BinaryReferencesCollector(() -> SegmentStore.EMPTY_STORE);
            for (SegmentArchiveEntry entry : reader.listSegments()) {
                collector.addSegment(entry, reader.readSegment(entry.getMsb(), entry.getLsb()));
            }
            return collector.write();
        }
    }

    private static Map<String, ByteBuffer> binaryReferences(SegmentArchiveManager archiveManager) throws IOException {
        Map<String, ByteBuffer> binaryReferences = new HashMap<>();
        for (String archive : archiveManager.listArchives()) {
            try (SegmentArchiveReader reader = archiveManager.open(archive)) {
                assertNotNull(archive + " not found", reader);
                binaryReferences.put(archive, ByteBuffer.wrap(bytes(reader.getBinaryReferences())));
            }
        }
        return binaryReferences;
    }

    private static byte[] bytes(Buffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static Map<String, Set<String>> binaryReferenceEntries(SegmentArchiveManager archiveManager) throws Exception {
        Map<String, Set<String>> entries = new HashMap<>();
        for (Map.Entry<String, ByteBuffer> archive : binaryReferences(archiveManager).entrySet()) {
            Set<String> archiveEntries = new HashSet<>();
            BinaryReferencesIndexLoader.parseBinaryReferencesIndex(Buffer.wrap(archive.getValue().array())).forEach(
                    (generation, full, compacted, segment, reference) ->
                            archiveEntries.add(generation + "/" + full + "/" + compacted + "/" + segment + "/" + reference));
            entries.put(archive.getKey(), archiveEntries);
        }
        return entries;
    }

    /**
     * Removes the files written when an archive is closed, leaving it as it is after the Oak
     * process terminated before closing it: segments only.
     */
    private void removeFilesWrittenOnClose(String archive) throws Exception {
        for (String name : filesWrittenOnClose(archive)) {
            deleteArchiveFile(archive, name);
        }
    }

    /**
     * The migrator opens the source store read-only to resolve binary references. A writable
     * store would recover the archive and write its binary references, graph and closed marker.
     */
    private void assertSourceArchiveNotRecovered(String archive) throws Exception {
        for (String name : filesWrittenOnClose(archive)) {
            assertFalse(archive + "/" + name + " written: the source store was recovered", archiveFileExists(archive, name));
        }
    }

    private static String[] filesWrittenOnClose(String archive) {
        return new String[] {archive + ".brf", archive + ".gph", "closed"};
    }

    private List<String> listArchives() throws IOException {
        List<String> archives = sourceArchiveManager().listArchives();
        assertFalse("expected at least one archive", archives.isEmpty());
        return archives;
    }

    private SegmentArchiveManager sourceArchiveManager() throws IOException {
        return source.createArchiveManager(false, false, new IOMonitorAdapter(),
                new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
    }

    private static SegmentArchiveManager tarArchiveManager(File tarDir) throws IOException {
        return new TarPersistence(tarDir).createArchiveManager(false, false,
                new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
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
