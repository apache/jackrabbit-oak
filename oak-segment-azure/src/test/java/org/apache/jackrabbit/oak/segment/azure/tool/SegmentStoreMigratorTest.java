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
package org.apache.jackrabbit.oak.segment.azure.tool;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.azure.storage.blob.BlobContainerClient;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoader;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SegmentStoreMigratorTest {

    private static final String ROOT_PREFIX = "oak";

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private BlobContainerClient readBlobContainerClient;

    private SegmentNodeStorePersistence source;

    @Before
    public void setup() throws Exception {
        readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
        source = new AzurePersistence(readBlobContainerClient,
                azurite.getWriteBlobContainerClient("oak-test"),
                azurite.getNoRetryBlobContainerClient("oak-test"),
                ROOT_PREFIX);
    }

    @Test
    public void binaryReferencesOfClosedArchiveAreMigrated() throws Exception {
        String blobId;
        try (FileStore fileStore = newFileStore()) {
            blobId = writeExternalBinary(fileStore);
        }

        File tarDir = migrateToTar();
        assertBinaryReferences(tarDir, listArchives(), blobId);
    }

    @Test
    public void binaryReferencesOfOpenArchiveAreMigrated() throws Exception {
        try (FileStore fileStore = newFileStore()) {
            String blobId = writeExternalBinary(fileStore);
            fileStore.flush();

            File tarDir = migrateToTar();
            assertBinaryReferences(tarDir, listArchives(), blobId);
        }
    }

    @Test
    public void binaryReferencesOfArchiveNotClosedBeforeTerminationAreMigrated() throws Exception {
        String blobId;
        try (FileStore fileStore = newFileStore()) {
            blobId = writeExternalBinary(fileStore);
        }
        for (String archive : listArchives()) {
            removeFilesWrittenOnClose(archive);
        }

        File tarDir = migrateToTar();
        assertBinaryReferences(tarDir, listArchives(), blobId);
    }

    private File migrateToTar() throws Exception {
        File tarDir = folder.newFolder("tar");
        try (SegmentStoreMigrator migrator = new SegmentStoreMigrator.Builder()
                .withSourcePersistence(source, "azure")
                .withTargetPersistence(new TarPersistence(tarDir), "tar")
                .build()) {
            migrator.migrate();
        }
        return tarDir;
    }

    private static void assertBinaryReferences(File tarDir, List<String> archives, String blobId) throws Exception {
        Set<String> references = new HashSet<>();
        SegmentArchiveManager tarManager = new TarPersistence(tarDir).createArchiveManager(false, false,
                new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        for (String archive : archives) {
            try (SegmentArchiveReader reader = tarManager.open(archive)) {
                assertNotNull(archive + " not found in the TAR store", reader);
                Buffer binaryReferences;
                try {
                    binaryReferences = reader.getBinaryReferences();
                } catch (IOException e) {
                    throw new AssertionError(archive + ": binary references not readable after migration", e);
                }
                assertNotNull(archive + ": binary references missing after migration", binaryReferences);
                BinaryReferencesIndexLoader.parseBinaryReferencesIndex(binaryReferences)
                        .forEach((generation, full, compacted, segment, reference) -> references.add(reference));
            }
        }
        assertTrue("binary reference " + blobId + " missing after migration", references.contains(blobId));
    }

    /**
     * Removes the files written when an archive is closed, leaving it as it is after the Oak
     * process terminated before closing it: segments only.
     */
    private void removeFilesWrittenOnClose(String archive) {
        for (String name : new String[] {archive + ".brf", archive + ".gph", archive + ".idx", "closed"}) {
            readBlobContainerClient.getBlobClient(ROOT_PREFIX + "/" + archive + "/" + name).deleteIfExists();
        }
    }

    private List<String> listArchives() throws IOException {
        List<String> archives = source.createArchiveManager(false, false, new IOMonitorAdapter(),
                new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter()).listArchives();
        assertFalse("expected at least one archive", archives.isEmpty());
        return archives;
    }

    private FileStore newFileStore() throws Exception {
        return FileStoreBuilder.fileStoreBuilder(folder.newFolder())
                .withCustomPersistence(source)
                .withBlobStore(new MemoryBlobStore())
                .build();
    }

    private static String writeExternalBinary(FileStore fileStore) throws Exception {
        byte[] data = new byte[100 * 1024];
        new Random(42).nextBytes(data);
        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
        NodeBuilder root = nodeStore.getRoot().builder();
        root.child("content").setProperty("binary", nodeStore.createBlob(new ByteArrayInputStream(data)));
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        Blob blob = nodeStore.getRoot().getChildNode("content").getProperty("binary").getValue(Type.BINARY);
        String blobId = ((SegmentBlob) blob).getBlobId();
        assertNotNull("binary should be stored in the blob store", blobId);
        return blobId;
    }
}
