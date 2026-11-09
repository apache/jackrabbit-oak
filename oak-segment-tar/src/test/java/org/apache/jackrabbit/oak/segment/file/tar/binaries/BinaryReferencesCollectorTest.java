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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.segment.LongIdMappingBlobStore;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BinaryReferencesCollectorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void rebuiltIndexWithShortBlobIdsEqualsWrittenIndex() throws Exception {
        File directory = folder.newFolder();
        String blobId = writeExternalBinary(directory, new MemoryBlobStore());

        Set<String> rebuilt = rebuildIndexEntries(directory);

        assertEqualsWrittenIndex(directory, blobId, rebuilt);
    }

    @Test
    public void rebuiltIndexWithLongBlobIdsEqualsWrittenIndex() throws Exception {
        File directory = folder.newFolder();
        String blobId = writeExternalBinary(directory, new LongIdMappingBlobStore());

        Set<String> rebuilt = rebuildIndexEntries(directory);

        assertEqualsWrittenIndex(directory, blobId, rebuilt);
    }

    @Test
    public void rebuiltIndexWithBlobIdInBulkSegmentsEqualsWrittenIndex() throws Exception {
        File directory = folder.newFolder();
        String blobId = writeExternalBinary(directory, new LongIdMappingBlobStore(Segment.MAX_SEGMENT_SIZE + 1));

        Set<String> rebuilt = rebuildIndexEntries(directory);

        assertEqualsWrittenIndex(directory, blobId, rebuilt);
    }

    @Test
    public void rebuiltIndexWithBlobIdInPreviousSegmentEqualsWrittenIndex() throws Exception {
        File directory = folder.newFolder();
        String blobId = writeBlobIdInTwoSegments(directory, new LongIdMappingBlobStore(LongIdMappingBlobStore.DEDUPLICATED_ID_LENGTH));

        Set<String> rebuilt = rebuildIndexEntries(directory);

        assertEqualsWrittenIndex(directory, blobId, rebuilt);
    }

    @Test
    public void blobIdInSegmentThatWasNotAddedFailsWithoutFallbackStore() throws Exception {
        File directory = folder.newFolder();
        String blobId = writeBlobIdInTwoSegments(directory, new LongIdMappingBlobStore(LongIdMappingBlobStore.DEDUPLICATED_ID_LENGTH));
        BinaryReferencesCollector collector = new BinaryReferencesCollector(() -> SegmentStore.EMPTY_STORE);

        addLastSegmentReferencing(directory, blobId, collector);

        assertThrows(SegmentNotFoundException.class, collector::write);
    }

    @Test
    public void blobIdInSegmentThatWasNotAddedIsReadFromFallbackStore() throws Exception {
        File directory = folder.newFolder();
        String blobId = writeBlobIdInTwoSegments(directory, new LongIdMappingBlobStore(LongIdMappingBlobStore.DEDUPLICATED_ID_LENGTH));
        try (ReadOnlyFileStore fallback = FileStoreBuilder.fileStoreBuilder(directory).buildReadOnly()) {
            BinaryReferencesCollector collector = new BinaryReferencesCollector(() -> fallback);

            addLastSegmentReferencing(directory, blobId, collector);

            assertTrue(containsReference(entries(Buffer.wrap(collector.write())), blobId));
        }
    }

    private static void addLastSegmentReferencing(File directory, String blobId, BinaryReferencesCollector collector) throws Exception {
        SegmentArchiveManager archiveManager = archiveManager(directory);
        List<String> archives = archiveManager.listArchives();
        assertEquals(1, archives.size());
        try (SegmentArchiveReader reader = archiveManager.open(archives.get(0))) {
            Set<UUID> referencingSegments = segmentsReferencing(reader.getBinaryReferences(), blobId);
            assertEquals(2, referencingSegments.size());

            SegmentArchiveEntry lastReferencingSegment = null;
            for (SegmentArchiveEntry entry : reader.listSegments()) {
                if (referencingSegments.contains(new UUID(entry.getMsb(), entry.getLsb()))) {
                    lastReferencingSegment = entry;
                }
            }
            collector.addSegment(lastReferencingSegment, readSegment(reader, lastReferencingSegment));
        }
    }

    private static void assertEqualsWrittenIndex(File directory, String blobId, Set<String> rebuilt) throws Exception {
        Set<String> written = writtenIndexEntries(directory);
        assertTrue("written index should contain " + blobId, containsReference(written, blobId));
        assertEquals(written, rebuilt);
    }

    private static Set<String> rebuildIndexEntries(File directory) throws Exception {
        return indexEntriesOfAllArchives(directory, BinaryReferencesCollectorTest::rebuildBinaryReferences);
    }

    private static Set<String> writtenIndexEntries(File directory) throws Exception {
        return indexEntriesOfAllArchives(directory, SegmentArchiveReader::getBinaryReferences);
    }

    private static Set<String> indexEntriesOfAllArchives(File directory, BinaryReferencesReader binaryReferencesReader) throws Exception {
        Set<String> entries = new HashSet<>();
        SegmentArchiveManager archiveManager = archiveManager(directory);
        for (String archive : archiveManager.listArchives()) {
            try (SegmentArchiveReader reader = archiveManager.open(archive)) {
                entries.addAll(entries(binaryReferencesReader.read(reader)));
            }
        }
        return entries;
    }

    private static Buffer rebuildBinaryReferences(SegmentArchiveReader reader) throws IOException {
        BinaryReferencesCollector collector = new BinaryReferencesCollector(() -> SegmentStore.EMPTY_STORE);
        for (SegmentArchiveEntry entry : reader.listSegments()) {
            collector.addSegment(entry, readSegment(reader, entry));
        }
        return Buffer.wrap(collector.write());
    }

    private static Buffer readSegment(SegmentArchiveReader reader, SegmentArchiveEntry entry) throws IOException {
        return reader.readSegment(entry.getMsb(), entry.getLsb());
    }

    private static Set<String> entries(Buffer binaryReferences) throws Exception {
        Set<String> entries = new HashSet<>();
        BinaryReferencesIndexLoader.parseBinaryReferencesIndex(binaryReferences).forEach(
                (generation, full, compacted, segment, reference) ->
                        entries.add(generation + "/" + full + "/" + compacted + "/" + segment + "/" + reference));
        return entries;
    }

    private static boolean containsReference(Set<String> entries, String blobId) {
        return entries.stream().anyMatch(entry -> entry.endsWith("/" + blobId));
    }

    private static Set<UUID> segmentsReferencing(Buffer binaryReferences, String blobId) throws Exception {
        Set<UUID> segments = new HashSet<>();
        BinaryReferencesIndexLoader.parseBinaryReferencesIndex(binaryReferences).forEach(
                (generation, full, compacted, segment, reference) -> {
                    if (reference.equals(blobId)) {
                        segments.add(segment);
                    }
                });
        return segments;
    }

    private static SegmentArchiveManager archiveManager(File directory) throws IOException {
        return new TarPersistence(directory).createArchiveManager(false, false,
                new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
    }

    private static String writeExternalBinary(File directory, BlobStore blobStore) throws Exception {
        try (FileStore fileStore = newFileStore(directory, blobStore)) {
            return writeBinary(SegmentNodeStoreBuilders.builder(fileStore).build(), "binary", newBinary());
        }
    }

    /**
     * Writes the same blob id in two segments: the second one references the blob id value
     * stored in the first one.
     */
    private static String writeBlobIdInTwoSegments(File directory, BlobStore blobStore) throws Exception {
        try (FileStore fileStore = newFileStore(directory, blobStore)) {
            SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            String blobId = writeBinary(nodeStore, "first", newBinary());
            fileStore.flush();
            writeBinary(nodeStore, "second", new BlobStoreBlob(blobStore, blobId));
            return blobId;
        }
    }

    private static FileStore newFileStore(File directory, BlobStore blobStore) throws Exception {
        return FileStoreBuilder.fileStoreBuilder(directory).withBlobStore(blobStore).build();
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

    private interface BinaryReferencesReader {
        Buffer read(SegmentArchiveReader reader) throws IOException;
    }
}
