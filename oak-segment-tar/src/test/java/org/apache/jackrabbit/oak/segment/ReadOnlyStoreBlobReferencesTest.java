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

package org.apache.jackrabbit.oak.segment;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoader;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for ReadOnlyFileStore#collectReferences
 */
public class ReadOnlyStoreBlobReferencesTest {

    private static final int EXTERNAL_BINARY_SIZE = 18000;

    private static final int MAX_ARCHIVE_SIZE_MB = 1;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));


    @Test
    public void collectReferences()
        throws IOException, InvalidFileStoreVersionException, CommitFailedException {
        File fileStoreDir = new File(getFileStoreFolder(), "segmentstore");
        File dataStoreDir = new File(getFileStoreFolder(), "blobstore");
        String blobId = createLoad(fileStoreDir, dataStoreDir).getContentIdentity();

        assertReferences(fileStoreDir, dataStoreDir, 1, blobId);
    }

    @Test
    public void collectReferencesAfterGC()
        throws IOException, InvalidFileStoreVersionException, CommitFailedException {
        File fileStoreDir = new File(getFileStoreFolder(), "segmentstore");
        File dataStoreDir = new File(getFileStoreFolder(), "blobstore");
        String blobId = createLoad(fileStoreDir, dataStoreDir).getContentIdentity();


        try (FileStore fileStore = fileStoreBuilder(fileStoreDir).withBlobStore(newBlobStore(dataStoreDir))
            .withGCOptions(defaultGCOptions().setGcSizeDeltaEstimation(1).setRetainedGenerations(1)).build()) {

            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.removeProperty("bin");
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();

            Set<String> actualReferences = new HashSet<>();
            fileStore.collectBlobReferences(actualReferences::add);
            assertEquals("Binary should be visible before gc cycle", 1, actualReferences.size());
            assertEquals("Binary reference returned should be same", blobId,
                actualReferences.toArray(new String[0])[0]);

            actualReferences = new HashSet<>();
            fileStore.fullGC();
            fileStore.collectBlobReferences(actualReferences::add);
            assertEquals("Binary should be deleted after gc cycle", 0, actualReferences.size());
        }

        assertReferences(fileStoreDir, dataStoreDir, 0, null);
    }

    @Test
    public void collectReferencesOfArchivesWithoutBinaryReferences() throws Exception {
        File fileStoreDir = new File(getFileStoreFolder(), "segmentstore");
        String blobId = writeExternalBinary(fileStoreDir, new MemoryBlobStore());
        File withoutBinaryReferences = copyWithoutBinaryReferences(fileStoreDir);

        assertEquals(Set.of(blobId), collectReferences(withoutBinaryReferences));
    }

    @Test
    public void collectLongReferencesOfArchivesWithoutBinaryReferences() throws Exception {
        File fileStoreDir = new File(getFileStoreFolder(), "segmentstore");
        String blobId = writeExternalBinary(fileStoreDir, new LongIdMappingBlobStore());
        File withoutBinaryReferences = copyWithoutBinaryReferences(fileStoreDir);

        assertEquals(Set.of(blobId), collectReferences(withoutBinaryReferences));
    }

    @Test
    public void collectLongReferencesInBulkSegmentsOfArchivesWithoutBinaryReferences() throws Exception {
        File fileStoreDir = new File(getFileStoreFolder(), "segmentstore");
        String blobId = writeExternalBinary(fileStoreDir, new LongIdMappingBlobStore(Segment.MAX_SEGMENT_SIZE + 1));
        File withoutBinaryReferences = copyWithoutBinaryReferences(fileStoreDir);

        assertEquals(Set.of(blobId), collectReferences(withoutBinaryReferences));
    }

    @Test
    public void collectLongReferencesWithValueInPreviousArchiveWithoutBinaryReferences() throws Exception {
        File fileStoreDir = new File(getFileStoreFolder(), "segmentstore");
        String blobId = writeBlobIdInTwoArchives(fileStoreDir,
                new LongIdMappingBlobStore(LongIdMappingBlobStore.DEDUPLICATED_ID_LENGTH));
        File withoutBinaryReferences = copyWithoutBinaryReferences(fileStoreDir);

        assertEquals(Set.of(blobId), collectReferences(withoutBinaryReferences));
    }

    @Test
    public void cleanupRecoversBinaryReferencesOfRewrittenArchive() throws Exception {
        File fileStoreDir = new File(getFileStoreFolder(), "segmentstore");
        BlobStore blobStore = new MemoryBlobStore();
        writeArchiveWithCompactedAndOldSegments(fileStoreDir, blobStore);
        Map<UUID, Set<String>> writtenReferences = binaryReferencesBySegment(fileStoreDir);
        File withoutBinaryReferences = copyWithoutBinaryReferences(fileStoreDir);
        Set<String> archivesBeforeCleanup = Set.of(withoutBinaryReferences.list());

        try (FileStore fileStore = fileStoreBuilder(withoutBinaryReferences).withBlobStore(blobStore)
                .withMaxFileSize(MAX_ARCHIVE_SIZE_MB)
                .withGCOptions(defaultGCOptions().setGcSizeDeltaEstimation(0).setRetainedGenerations(1)).build()) {
            fileStore.fullGC();

            String rewritten = rewrittenArchive(withoutBinaryReferences, archivesBeforeCleanup);
            Map<UUID, Set<String>> keptReferences = new HashMap<>(writtenReferences);
            keptReferences.keySet().retainAll(segmentsOf(withoutBinaryReferences, rewritten));
            assertFalse("rewritten archive should keep segments with binary references", keptReferences.isEmpty());
            assertEquals(keptReferences, binaryReferencesBySegment(withoutBinaryReferences, rewritten));
        }
    }

    private File getFileStoreFolder() {
        return folder.getRoot();
    }

    private static Blob createBlob(NodeStore nodeStore, int size) throws IOException {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return nodeStore.createBlob(new ByteArrayInputStream(data));
    }

    private static BlobStore newBlobStore(File directory) {
        OakFileDataStore delegate = new OakFileDataStore();
        delegate.setPath(directory.getAbsolutePath());
        delegate.init(null);
        return new DataStoreBlobStore(delegate);
    }

    private Blob createLoad(File fileStoreDir, File dataStoreDir)
        throws IOException, CommitFailedException, InvalidFileStoreVersionException {
        try (FileStore fileStore = fileStoreBuilder(fileStoreDir).withBlobStore(newBlobStore(dataStoreDir))
            .withGCOptions(defaultGCOptions().setGcSizeDeltaEstimation(0)).build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

            NodeBuilder builder = nodeStore.getRoot().builder();
            Blob blob = createBlob(nodeStore, EXTERNAL_BINARY_SIZE);
            builder.setProperty("bin", blob);
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.flush();
            return blob;
        }
    }

    /**
     * Writes a binary and content that is removed again, then compacts while retaining two
     * generations. The current archive then contains both compacted segments and old ones, so the
     * next cleanup with one retained generation rewrites it instead of removing it.
     */
    private static void writeArchiveWithCompactedAndOldSegments(File fileStoreDir, BlobStore blobStore) throws Exception {
        try (FileStore fileStore = fileStoreBuilder(fileStoreDir).withBlobStore(blobStore)
                .withMaxFileSize(MAX_ARCHIVE_SIZE_MB)
                .withGCOptions(defaultGCOptions().setGcSizeDeltaEstimation(0).setRetainedGenerations(2)).build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            writeBinary(nodeStore, "bin", newBinary());
            writeContentLargerThanMaxArchiveSize(nodeStore);
            NodeBuilder builder = nodeStore.getRoot().builder();
            builder.child("filler").remove();
            nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fileStore.fullGC();
        }
    }

    /**
     * The archive written by cleanup: a new tar generation (e.g. {@code data00001b.tar}) of an
     * archive that existed before.
     */
    private static String rewrittenArchive(File fileStoreDir, Set<String> archivesBeforeCleanup) {
        List<String> rewritten = Arrays.stream(fileStoreDir.list())
                .filter(name -> name.endsWith("b.tar") && !archivesBeforeCleanup.contains(name))
                .filter(name -> archivesBeforeCleanup.contains(name.substring(0, name.length() - "b.tar".length()) + "a.tar"))
                .collect(Collectors.toList());
        assertEquals("archives rewritten by cleanup", 1, rewritten.size());
        return rewritten.get(0);
    }

    private static Map<UUID, Set<String>> binaryReferencesBySegment(File fileStoreDir) throws Exception {
        Map<UUID, Set<String>> references = new HashMap<>();
        for (String archive : archiveManager(fileStoreDir).listArchives()) {
            references.putAll(binaryReferencesBySegment(fileStoreDir, archive));
        }
        return references;
    }

    private static Map<UUID, Set<String>> binaryReferencesBySegment(File fileStoreDir, String archive) throws Exception {
        Map<UUID, Set<String>> references = new HashMap<>();
        try (SegmentArchiveReader reader = archiveManager(fileStoreDir).open(archive)) {
            BinaryReferencesIndexLoader.parseBinaryReferencesIndex(reader.getBinaryReferences())
                    .forEach((generation, full, compacted, segment, reference) ->
                            references.computeIfAbsent(segment, id -> new HashSet<>()).add(reference));
        }
        return references;
    }

    private static Set<UUID> segmentsOf(File fileStoreDir, String archive) throws IOException {
        try (SegmentArchiveReader reader = archiveManager(fileStoreDir).open(archive)) {
            return reader.listSegments().stream()
                    .map(entry -> new UUID(entry.getMsb(), entry.getLsb()))
                    .collect(Collectors.toSet());
        }
    }

    private static Set<String> collectReferences(File fileStoreDir)
        throws IOException, InvalidFileStoreVersionException {
        Set<String> references = new HashSet<>();
        try (ReadOnlyFileStore fileStore = fileStoreBuilder(fileStoreDir).buildReadOnly()) {
            fileStore.collectBlobReferences(references::add);
        }
        return references;
    }

    private static String writeExternalBinary(File fileStoreDir, BlobStore blobStore) throws Exception {
        try (FileStore fileStore = fileStoreBuilder(fileStoreDir).withBlobStore(blobStore).build()) {
            return writeBinary(SegmentNodeStoreBuilders.builder(fileStore).build(), "bin", newBinary());
        }
    }

    /**
     * Writes the same long blob id in two archives: the reference in the second archive points to
     * the value stored in the first one, reused by the segment writer's string cache.
     */
    private static String writeBlobIdInTwoArchives(File fileStoreDir, BlobStore blobStore) throws Exception {
        try (FileStore fileStore = fileStoreBuilder(fileStoreDir).withBlobStore(blobStore)
                .withMaxFileSize(MAX_ARCHIVE_SIZE_MB).build()) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            String blobId = writeBinary(nodeStore, "first", newBinary());
            writeContentLargerThanMaxArchiveSize(nodeStore);
            writeBinary(nodeStore, "second", new BlobStoreBlob(blobStore, blobId));
            return blobId;
        }
    }

    private static Blob newBinary() {
        byte[] data = new byte[EXTERNAL_BINARY_SIZE];
        new Random().nextBytes(data);
        return new ArrayBasedBlob(data);
    }

    private static String writeBinary(SegmentNodeStore nodeStore, String name, Blob binary) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.setProperty(name, binary);
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        return ((SegmentBlob) nodeStore.getRoot().getProperty(name).getValue(Type.BINARY)).getBlobId();
    }

    private static void writeContentLargerThanMaxArchiveSize(SegmentNodeStore nodeStore) throws CommitFailedException {
        Random random = new Random(42);
        NodeBuilder builder = nodeStore.getRoot().builder();
        for (int i = 0; i < 2 * MAX_ARCHIVE_SIZE_MB * 1024; i++) {
            byte[] value = new byte[1024];
            random.nextBytes(value);
            builder.child("filler").child("node" + i).setProperty("value", Base64.getEncoder().encodeToString(value));
        }
        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    /**
     * Copies the store, writing every archive without its binary references entry.
     */
    private File copyWithoutBinaryReferences(File fileStoreDir) throws IOException {
        File copy = folder.newFolder("without-binary-references");
        SegmentArchiveManager source = archiveManager(fileStoreDir);
        SegmentArchiveManager target = archiveManager(copy);
        for (String archive : source.listArchives()) {
            try (SegmentArchiveReader reader = source.open(archive)) {
                SegmentArchiveWriter writer = target.create(archive);
                for (SegmentArchiveEntry entry : reader.listSegments()) {
                    ByteBuffer buffer = reader.readSegment(entry.getMsb(), entry.getLsb());
                    byte[] data = new byte[buffer.remaining()];
                    buffer.get(data);
                    writer.writeSegment(entry.getMsb(), entry.getLsb(), data, 0, data.length,
                            entry.getGeneration(), entry.getFullGeneration(), entry.isCompacted());
                }
                writer.writeGraph(reader.getGraph().write());
                writer.close();
            }
        }
        for (File file : fileStoreDir.listFiles((dir, name) -> !name.endsWith(".tar"))) {
            FileUtils.copyFileToDirectory(file, copy);
        }
        return copy;
    }

    private static SegmentArchiveManager archiveManager(File directory) throws IOException {
        return new TarPersistence(directory).createArchiveManager(false, false, new IOMonitorAdapter(),
                new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
    }

    private void assertReferences(File fileStoreDir, File dataStoreDir, int count, String blobId)
        throws IOException, InvalidFileStoreVersionException {
        try (ReadOnlyFileStore fileStore = fileStoreBuilder(fileStoreDir).withBlobStore(newBlobStore(dataStoreDir))
            .buildReadOnly()) {

            Set<String> actualReferences = new HashSet<>();
            fileStore.collectBlobReferences(actualReferences::add);
            assertEquals("Read only store visible references different", count, actualReferences.size());
            if (!StringUtils.isEmpty(blobId)) {
                assertEquals("Binary reference returned should be same", blobId,
                    actualReferences.toArray(new String[0])[0]);
            }
        }
    }
}
