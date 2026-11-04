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
package org.apache.jackrabbit.oak.segment.azure.tar;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.apache.commons.lang3.RandomUtils;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.AzureRepositoryLock;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.segment.azure.AzurePersistence.WriteMode.TAR_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AzureTarArchiveManagerTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private File dummyDirectory;

    private BlobContainerClient readBlobContainerClient;
    private BlobContainerClient writeBlobContainerClient;
    private BlobContainerClient noRetryBlobContainerClient;


    @Before
    public void setup() throws BlobStorageException, InvalidKeyException, URISyntaxException {
        readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
        writeBlobContainerClient = azurite.getWriteBlobContainerClient("oak-test");
        noRetryBlobContainerClient = azurite.getNoRetryBlobContainerClient("oak-test");
    }

    @Rule
    public final ProvideSystemProperty systemPropertyRule = new ProvideSystemProperty(AzureRepositoryLock.LEASE_DURATION_PROP, "15")
            .and(AzureRepositoryLock.RENEWAL_INTERVAL_PROP, "3")
            .and(AzureRepositoryLock.TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, "9");

    @Before
    public void before() throws IOException {
        dummyDirectory = folder.newFolder("dummy");
    }

    @Test
    public void testRecoveryOfUnclosedArchive() throws Exception {
        withFileStore(createPersistence("source"), writeContent(builder -> {
            builder.setProperty("foo", "bar");
        }));

        withFileStore(createPersistence("source"), store -> {
            writeContent(store, builder -> {
                builder.setProperty("foo1", "bar1");
            });
            copyBlobs("source", "destination");
        });

        List<String> sourceBlobs = readBlobContainerClient.listBlobsByHierarchy("source/").stream()
                .map(BlobItem::getName)
                .collect(Collectors.toUnmodifiableList());
        assertEquals(2, sourceBlobs.stream().filter(name -> name.endsWith(".tar")).count());
        assertEquals(0, sourceBlobs.stream().filter(name -> name.endsWith(".tar.bak")).count());

        // recover unclosed archive data00001a.tar by opening the destination filestore
        withFileStore(createPersistence("destination"), store -> {});

        List<String> destinationBlobs = readBlobContainerClient.listBlobsByHierarchy("destination/").stream()
                .map(BlobItem::getName)
                .collect(Collectors.toUnmodifiableList());
        assertEquals(2, destinationBlobs.stream().filter(name -> name.endsWith(".tar")).count());
        assertEquals(1, destinationBlobs.stream().filter(name -> name.endsWith(".tar.bak")).count());

        SegmentArchiveManager archiveManager = createArchiveManager(createPersistence("destination"));
        try (SegmentArchiveReader recovered = archiveManager.open("data00001a.tar")) {
            assertNotNull(recovered);
        }
    }

    @Test
    public void testReadOnlyRecoveryOfUnclosedArchive() throws Exception {
        withFileStore(createPersistence("source"), writeContent(builder -> {
            builder.setProperty("foo", "bar");
        }));

        withFileStore(createPersistence("source"), store -> {
            writeContent(store, builder -> {
                builder.setProperty("foo1", "bar1");
            });
            copyBlobs("source", "destination");
        });

        List<String> sourceBlobs = readBlobContainerClient.listBlobsByHierarchy("source/").stream()
                .map(BlobItem::getName)
                .collect(Collectors.toUnmodifiableList());
        assertEquals(2, sourceBlobs.stream().filter(name -> name.endsWith(".tar")).count());
        assertEquals(0, sourceBlobs.stream().filter(name -> name.endsWith(".tar.bak")).count());

        // recover unclosed archive data00001a.tar by opening the destination filestore
        withReadOnlyFileStore(createPersistence("destination"), store -> {});

        List<String> destinationBlobs = readBlobContainerClient.listBlobsByHierarchy("destination/").stream()
                .map(BlobItem::getName)
                .collect(Collectors.toUnmodifiableList());
        assertEquals(2, destinationBlobs.stream().filter(name -> name.endsWith(".tar")).count());
        assertEquals(1, destinationBlobs.stream().filter(name -> name.endsWith(".tar.ro.bak")).count());
        assertEquals(0, destinationBlobs.stream().filter(name -> name.endsWith(".tar.bak")).count());

        SegmentArchiveManager archiveManager = createArchiveManager(createPersistence("destination"));
        assertNull(archiveManager.open("data00001a.tar"));
        try (SegmentArchiveReader roRecovered = archiveManager.open("data00001a.tar.ro.bak")) {
            assertNotNull(roRecovered);
        }
    }

    @Test
    public void testNoEmptyArchiveOnUncleanStop() throws Exception {
        withFileStore(createPersistence("source"), writeContent(builder -> {
            builder.setProperty("foo", "bar");
        }));

        withFileStore(createPersistence("source"), store -> {
            store.flush();
            copyBlobs("source", "destination");
        });

        List<String> sourceBlobs = readBlobContainerClient.listBlobsByHierarchy("source/").stream()
                .map(BlobItem::getName)
                .collect(Collectors.toUnmodifiableList());
        assertEquals(1, sourceBlobs.stream().filter(name -> name.endsWith(".tar")).count());
        assertEquals(0, sourceBlobs.stream().filter(name -> name.endsWith(".tar.bak")).count());

        // recover unclosed archive data00001a.tar by opening the destination filestore
        withFileStore(createPersistence("destination"), store -> {});

        List<String> destinationBlobs = readBlobContainerClient.listBlobsByHierarchy("destination/").stream()
                .map(BlobItem::getName)
                .collect(Collectors.toUnmodifiableList());
        assertEquals(1, destinationBlobs.stream().filter(name -> name.endsWith(".tar")).count());
        assertEquals(0, destinationBlobs.stream().filter(name -> name.endsWith(".tar.bak")).count());
    }

    @Test
    public void testExists() throws IOException {
        withWriteableArchiveManager(createPersistence("oak"), manager -> {
            SegmentArchiveWriter writer = manager.create("data00000a.tar");
            assertFalse(manager.exists("data00000a.tar"));

            writeRandomSegment(writer, 128);
            writer.close();
            assertTrue(manager.exists("data00000a.tar"));
        });
    }

    @Test
    public void testArchiveExistsAfterFlush() throws IOException {
        withWriteableArchiveManager(createPersistence("oak"), manager -> {
            SegmentArchiveWriter writer = manager.create("data00000a.tar");
            assertFalse(manager.exists("data00000a.tar"));

            writeRandomSegment(writer, 128);
            writer.flush();
            assertTrue(manager.exists("data00000a.tar"));

            writer.close();
            assertTrue(manager.exists("data00000a.tar"));
        });
    }

    @Test
    public void testMissingSegmentDetectedInFileStore() throws IOException, BlobStorageException, InvalidFileStoreVersionException {
        AzurePersistence persistence = createPersistence("oak");

        // initialize manifest file
        assertFalse(persistence.getManifestFile().exists());
        withFileStore(persistence, store -> {});
        assertTrue(persistence.getManifestFile().exists());

        AtomicReference<UUID> uuid = new AtomicReference<>();
        UUID nonExistent = UUID.randomUUID();
        withWriteableArchiveManager(persistence, manager -> {
            // archive data00000a.tar is created when the FileStore is initialized
            assertEquals(List.of("data00000a.tar"), manager.listArchives());
            SegmentArchiveWriter writer = manager.create("data00001a.tar");
            UUID u = writeRandomSegment(writer, 128);
            uuid.set(u);
            writer.close();

            try (SegmentArchiveReader reader = manager.open("data00001a.tar")) {
                assertNotNull(reader);
                assertNotNull(reader.readSegment(u.getMostSignificantBits(), u.getLeastSignificantBits()));
                assertNull(reader.readSegment(nonExistent.getMostSignificantBits(), nonExistent.getLeastSignificantBits()));
            }
        });

        withFileStore(persistence, store -> {
            UUID existent = uuid.get();
            assertNotNull(store.readSegment(new SegmentId(store, existent.getMostSignificantBits(), existent.getLeastSignificantBits())));
            assertThrows(SegmentNotFoundException.class,
                    () -> store.readSegment(new SegmentId(store, nonExistent.getMostSignificantBits(), nonExistent.getLeastSignificantBits())));
        });
    }

    @Test
    public void testDelete() throws IOException {
        withWriteableArchiveManager(createPersistence("oak"), manager -> {
            SegmentArchiveWriter writer = manager.create("data00000a.tar");
            writeRandomSegment(writer, 128);
            writer.close();
            assertTrue(manager.exists("data00000a.tar"));

            assertTrue(manager.delete("data00000a.tar"));
            assertFalse(manager.exists("data00000a.tar"));

            // delete non-existing archive
            assertFalse(manager.delete("data00005a.tar"));
        });
    }

    private AzurePersistence createPersistence(String folderName) {
        AzurePersistence persistence = new AzurePersistence(
                readBlobContainerClient,
                writeBlobContainerClient,
                noRetryBlobContainerClient,
                folderName,
                TAR_FILE);
        persistence.setWriteAccessController(new WriteAccessController() {
            @Override
            public void checkWritingAllowed() {
                if (!isWritingAllowed()) {
                    // stop tests from waiting for write access indefinitely
                    throw new IllegalStateException("Writing is blocked, acquire the repository lock, e.g. by using withWriteableArchiveManager, to enable writing");
                }
            }
        });
        return persistence;
    }

    private static SegmentArchiveManager createArchiveManager(SegmentNodeStorePersistence persistence) throws IOException {
        return persistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
    }

    private static <E extends Throwable> void withWriteableArchiveManager(AzurePersistence persistence, ThrowingConsumer<SegmentArchiveManager, E> managerConsumer) throws E, IOException {
        RepositoryLock repositoryLock = persistence.lockRepository();
        try {
            SegmentArchiveManager manager = createArchiveManager(persistence);
            managerConsumer.accept(manager);
        } finally {
            repositoryLock.unlock();
        }
    }

    private static <E extends Exception> void writeContent(FileStore fileStore, ThrowingConsumer<NodeBuilder, E> builderConsumer) throws Exception {
        writeContent(builderConsumer).accept(fileStore);
    }

    private static <E extends Exception> ThrowingConsumer<FileStore, Exception> writeContent(ThrowingConsumer<NodeBuilder, E> builderConsumer) {
        return store -> {
            SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(store).build();
            NodeBuilder builder = segmentNodeStore.getRoot().builder();
            builderConsumer.accept(builder);
            segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            store.flush();
        };
    }

    private void copyBlobs(String source, String destination) {
        readBlobContainerClient.listBlobs(new ListBlobsOptions().setPrefix(source + '/'), null).forEach(blobItem -> {
            writeBlobContainerClient.getBlobClient(destination + '/' + blobItem.getName().substring((source + '/').length()))
                    .getAppendBlobClient()
                    .copyFromUrl(readBlobContainerClient.getBlobClient(blobItem.getName()).getBlobUrl());
        });
    }

    private <E extends Throwable> void withFileStore(SegmentNodeStorePersistence persistence, ThrowingConsumer<FileStore, E> withFileStore) throws InvalidFileStoreVersionException, IOException, E {
        try (FileStore store = FileStoreBuilder.fileStoreBuilder(dummyDirectory)
                .withStrictVersionCheck(false)
                .withCustomPersistence(persistence).build()) {
            withFileStore.accept(store);
        }
    }

    private void withReadOnlyFileStore(SegmentNodeStorePersistence persistence, Consumer<ReadOnlyFileStore> withFileStore) throws InvalidFileStoreVersionException, IOException {
        try (ReadOnlyFileStore store = FileStoreBuilder.fileStoreBuilder(dummyDirectory)
                .withStrictVersionCheck(false)
                .withCustomPersistence(persistence).buildReadOnly()) {
            withFileStore.accept(store);
        }
    }

    private static UUID writeRandomSegment(SegmentArchiveWriter writer, int size) throws IOException {
        UUID uuid = UUID.randomUUID();
        byte[] bytes = RandomUtils.insecure().randomBytes(size);
        writer.writeSegment(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), bytes, 0, size, 0, 0, false);
        return uuid;
    }

    interface ThrowingConsumer<T, E extends Throwable> {
        void accept(T t) throws E;
    }
}
