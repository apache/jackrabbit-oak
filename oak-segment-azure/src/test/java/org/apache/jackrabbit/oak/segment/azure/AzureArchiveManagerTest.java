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
package org.apache.jackrabbit.oak.segment.azure;

import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.AbstractPersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.CachingPersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.split.SplitPersistence;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AzureArchiveManagerTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private Fixture fixture;

    @Before
    public void setup() throws Exception {
        fixture = new Fixture(azurite);
    }

    @Test
    public void testRecovery() throws StorageException, URISyntaxException, IOException {
        SegmentArchiveManager manager = fixture.newSegmentArchiveManager();
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        List<UUID> uuids = writeSegments(writer, 10);

        fixture.deleteBlob("oak/data00000a.tar/0005." + uuids.get(5).toString());

        LinkedHashMap<UUID, byte[]> recovered = new LinkedHashMap<>();
        manager.recoverEntries("data00000a.tar", recovered);
        assertEquals(uuids.subList(0, 5), newArrayList(recovered.keySet()));
    }

    @Test
    public void testBackupWithRecoveredEntries() throws StorageException, URISyntaxException, IOException {
        SegmentArchiveManager manager = fixture.newSegmentArchiveManager();
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        List<UUID> uuids = writeSegments(writer, 10);

        fixture.deleteBlob("oak/data00000a.tar/0005." + uuids.get(5).toString());

        LinkedHashMap<UUID, byte[]> recovered = new LinkedHashMap<>();
        manager.recoverEntries("data00000a.tar", recovered);

        manager.backup("data00000a.tar", "data00000a.tar.bak", recovered.keySet());

        for (int i = 0; i <= 4; i++) {
            assertTrue(fixture.blobExists("oak/data00000a.tar/000" + i + "." + uuids.get(i)));
        }

        for (int i = 5; i <= 9; i++) {
            assertFalse(String.format("Segment %s.??? should have been deleted.", "oak/data00000a.tar/000" + i),
                fixture.blobExists("oak/data00000a.tar/000" + i + "." + uuids.get(i)));
        }
    }

    @Test
    public void testUncleanStop() throws URISyntaxException, StorageException {
        fixture.mergeChanges(builder -> builder.setProperty("foo", "bar"));

        fixture.deleteBlob("oak/data00000a.tar/closed");
        fixture.deleteBlob("oak/data00000a.tar/data00000a.tar.brf");
        fixture.deleteBlob("oak/data00000a.tar/data00000a.tar.gph");

        fixture.withSegmentStore(segmentNodeStore ->
            assertEquals("bar", segmentNodeStore.getRoot().getString("foo"))
        );
    }
    
    @Test
    // see OAK-8566
    public void testUncleanStopWithEmptyArchive() throws URISyntaxException, StorageException {
        fixture.mergeChanges(builder -> builder.setProperty("foo", "bar"));

        // make sure there are 2 archives
        fixture.mergeChanges(builder -> builder.setProperty("foo2", "bar2"));

        // remove the segment 0000 from the second archive
        fixture.deleteFirstBlobWithPrefix("oak/data00001a.tar/0000.");
        fixture.deleteBlob("oak/data00001a.tar/closed");

        fixture.withSegmentStore(segmentNodeStore ->
            assertEquals("bar", segmentNodeStore.getRoot().getString("foo"))
        );
    }

    @Test
    public void testUncleanStopSegmentMissing() throws URISyntaxException, StorageException {
        fixture.mergeChanges(builder -> builder.setProperty("foo", "bar"));

        // make sure there are 2 archives
        fixture.flushEachChange(
            b -> b.setProperty("foo0", "bar0"),
            //create segment 0001
            b -> b.setProperty("foo1", "bar1"),
            //create segment 0002
            b -> b.setProperty("foo2", "bar2"),
            //create segment 0003
            b -> b.setProperty("foo3", "bar3")
        );

        // remove the segment 0002 from the second archive
        fixture.deleteFirstBlobWithPrefix("oak/data00001a.tar/0002.");
        fixture.deleteBlob("oak/data00001a.tar/closed");

        fixture.withSegmentStore(segmentNodeStore -> {
            NodeState root = segmentNodeStore.getRoot();
            assertEquals("bar", root.getString("foo"));

            //verify content from recovered segments preserved
            assertEquals("bar1", root.getString("foo1"));
            //content from deleted segments not preserved
            assertNull(root.getString("foo2"));
            assertNull(root.getString("foo3"));
        });

        //recovered archive data00001a.tar should not contain segments 0002 and 0003
        assertFalse(fixture.hasBlobsWithPrefix("oak/data00001a.tar/0002."));
        assertFalse(fixture.hasBlobsWithPrefix("oak/data00001a.tar/0003."));

        assertTrue("Backup directory should have been created", fixture.hasBlobsWithPrefix("oak/data00001a.tar.bak"));
        //backup has all segments but 0002 since it was deleted before recovery
        assertTrue(fixture.hasBlobsWithPrefix("oak/data00001a.tar.bak/0001."));
        assertFalse(fixture.hasBlobsWithPrefix("oak/data00001a.tar.bak/0002."));
        assertTrue(fixture.hasBlobsWithPrefix("oak/data00001a.tar.bak/0003."));
    }

    @Test
    public void testExists() throws IOException {
        SegmentArchiveManager manager = fixture.newSegmentArchiveManager();
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        writeSegments(writer, 10);

        assertTrue(manager.exists("data00000a.tar"));
        assertFalse(manager.exists("data00001a.tar"));
    }

    @Test
    public void testArchiveExistsAfterFlush() throws IOException {
        SegmentArchiveManager manager = fixture.newSegmentArchiveManager();
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        assertFalse(manager.exists("data00000a.tar"));
        writeSegment(writer, UUID.randomUUID());
        writer.flush();
        assertTrue(manager.exists("data00000a.tar"));
    }

    @Test
    public void testSegmentDeletedAfterCreatingReader() throws IOException, StorageException {
        SegmentArchiveManager manager = fixture.newSegmentArchiveManager();
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        assertFalse(manager.exists("data00000a.tar"));
        List<UUID> uuids = writeSegments(writer, 1);

        UUID u = uuids.get(0);
        try (SegmentArchiveReader reader = manager.open("data00000a.tar")) {
            Buffer segment = readSegment(reader, u);
            assertNotNull(segment);

            fixture.deleteFirstBlobWithPrefix("oak/data00000a.tar/0000.");

            assertThrows(FileNotFoundException.class, () -> readSegment(reader, u));
        }
    }

    @Test
    public void testMissingSegmentDetectedInFileStore() {
        fixture.withFileStore(fileStore -> {
            SegmentArchiveManager manager = fixture.newSegmentArchiveManager();
            SegmentArchiveWriter writer = manager.create("data00000a.tar");

            List<UUID> uuids = writeSegments(writer, 1);

            UUID u = uuids.get(0);
            try (SegmentArchiveReader reader = manager.open("data00000a.tar")) {
                assertNotNull(readSegment(reader, u));
            }

            fixture.deleteFirstBlobWithPrefix("oak/data00000a.tar/0000.");

            assertThrows(SegmentNotFoundException.class, () ->
                fileStore.readSegment(new SegmentId(fileStore, u.getMostSignificantBits(), u.getLeastSignificantBits())));
        });
    }

    @Test
    public void testReadOnlyRecovery() throws URISyntaxException, StorageException {
        fixture.withSegmentStore((rwFileStore, segmentNodeStore) -> {
            fixture.flushChanges(rwFileStore, b -> b.setProperty("foo", "bar"));

            assertTrue(fixture.hasBlobsInArchive("oak/data00000a.tar"));
            assertFalse(fixture.hasBlobsInArchive("oak/data00000a.tar.ro.bak"));

            // create read-only FS, while read-write FS is still open
            fixture.withReadOnlyFileStore(roFileStore -> {
                PropertyState fooProperty = segmentNodeStore(roFileStore)
                    .getRoot()
                    .getProperty("foo");
                assertNotNull(fooProperty);
                assertEquals("bar", fooProperty.getValue(Type.STRING));
            });
        });

        assertTrue(fixture.hasBlobsInArchive("oak/data00000a.tar"));
        // after creating a read-only FS, the recovery procedure should not be started since there is another running Oak process
        assertFalse(fixture.hasBlobsInArchive("oak/data00000a.tar.ro.bak"));
    }

    @Test
    public void testCachingPersistenceTarRecovery() throws URISyntaxException, InvalidFileStoreVersionException, IOException, StorageException {
        FileStoreBuilder fileStoreBuilder = FileStoreBuilder.fileStoreBuilder(folder.newFolder())
            .withCustomPersistence(fixture.newAzurePersistence());
        try (FileStore rwFileStore = fileStoreBuilder.build()) {
            fixture.flushChanges(rwFileStore, b -> b.setProperty("foo", "bar"));

            assertTrue(fixture.hasBlobsInArchive("oak/data00000a.tar"));
            assertFalse(fixture.hasBlobsInArchive("oak/data00000a.tar.ro.bak"));

            // create files store with split persistence
            AzurePersistence azureSharedPersistence = fixture.newAzurePersistence();

            CachingPersistence cachingPersistence = new CachingPersistence(createPersistenceCache(), azureSharedPersistence);
            File localFolder = folder.newFolder();
            SegmentNodeStorePersistence localPersistence = new TarPersistence(localFolder);
            SegmentNodeStorePersistence splitPersistence = new SplitPersistence(cachingPersistence, localPersistence);

            // exception should not be thrown here
            try (FileStore ignore = FileStoreBuilder.fileStoreBuilder(localFolder).withCustomPersistence(splitPersistence).build()) {
            }
        }

        assertTrue(fixture.hasBlobsInArchive("oak/data00000a.tar"));
        // after creating a read-only FS, the recovery procedure should not be started since there is another running Oak process
        assertFalse(fixture.hasBlobsInArchive("oak/data00000a.tar.ro.bak"));
    }

    @Test
    public void testShouldNotWriteSegmentsAfterLeaseLost() {
        fixture.persistence = new AzurePersistence(fixture.getOakDirectory()) {
            @Override
            public RepositoryLock lockRepository(Consumer<LockStatus> lockStatusChangedCallback) throws IOException {
                return loseLockAtSecondFailedRenewal(getLockBlob(), lockStatusChangedCallback).lock();
            }
        };

        AtomicInteger lastPersisted = new AtomicInteger(-1);

        int totalChanges = 500;
        // When lease is lost, an exception must be thrown as it becomes illegal to continue writing
        assertThrows(IllegalStateException.class, () ->
            fixture.flushEachChange(totalChanges, (i, builder) -> builder.setProperty("foo" + i, "bar" + i), lastPersisted::set)
        );

        int lastPersistedIndex = lastPersisted.get();
        assertTrue("Writes should have been interrupted, but all " + totalChanges + " changes have been persisted", lastPersistedIndex < totalChanges - 1);
        fixture.withSegmentStore(segmentNodeStore -> {
            assertEquals("bar" + lastPersistedIndex, segmentNodeStore.getRoot().getString("foo" + lastPersistedIndex)); // last successful write
            assertNull("Should have not been able to write after lease is lost:", segmentNodeStore.getRoot()
                .getString("foo" + (lastPersistedIndex + 1)));
        });
    }

    @Test
    public void testShouldSuspendWritesWhenUnableToRenewLease() {
        SuspendWritesState state = new SuspendWritesState();
        fixture.persistence = new AzurePersistence(fixture.getOakDirectory()) {
            @Override
            public RepositoryLock lockRepository(Consumer<LockStatus> lockStatusChangedCallback) throws IOException {
                return failToRenewLeaseThenSucceed(getLockBlob(), lockStatusChangedCallback, state).lock();
            }
        };

        int totalChanges = 200;
        fixture.flushEachChange(totalChanges, (i, builder) -> builder.setProperty("foo" + i, "bar" + i), state.lastPersisted::set);

        assertTrue("Writes should have been suspended", state.writesSuspended.get());
        assertFalse("No writes should have been persisted while suspended", state.writesWhileSuspendedDetected.get());
        assertTrue("Writes should have resumed", state.writesResumed.get());
        int lastIndex = totalChanges - 1;
        assertEquals("All writes have been persisted, after renewal succeeds", state.lastPersisted.get(), lastIndex);
        fixture.withSegmentStore(segmentNodeStore ->
            assertEquals("Last write persisted", "bar" + lastIndex, segmentNodeStore.getRoot().getString("foo" + lastIndex)));
    }

    @NotNull
    private static AzureRepositoryLock loseLockAtSecondFailedRenewal(
        final CloudBlockBlob lockBlob,
        final Consumer<LockStatus> lockStatusChangedCallback) {
        return new AzureRepositoryLock(lockBlob, lockStatusChangedCallback, 1, 1) {
            private int calls = 0;
            
            @Override
            void doRenewLease() throws StorageException {
                calls++;
                if (calls == 1) {
                    throw new StorageException(
                        StorageErrorCodeStrings.OPERATION_TIMED_OUT,
                        "The client could not finish the operation within specified maximum execution timeout.", null);
                }
                try {
                    releaseLease();
                } catch (Exception e) {
                    throw new RuntimeException("Could not release the lease", e);
                }
                throw new StorageException(
                    StorageErrorCodeStrings.LEASE_ID_MISMATCH_WITH_LEASE_OPERATION,
                    "The lease ID specified did not match the lease ID for the blob.", null);

            }
        };
    }

    @NotNull
    private static AzureRepositoryLock failToRenewLeaseThenSucceed(
        final CloudBlockBlob lockBlob,
        Consumer<LockStatus> lockStatusChangedCallback,
        SuspendWritesState state) {
        return new AzureRepositoryLock(lockBlob, state.checkSuspendedWrites(lockStatusChangedCallback), 1, 1) {
            @Override
            void doRenewLease() throws StorageException {
                // Fail to renew 20 times, then succeed
                if (state.renewalCounter.getAndIncrement() < 20) {
                    throw new StorageException(
                        StorageErrorCodeStrings.OPERATION_TIMED_OUT,
                        "The client could not finish the operation within specified maximum execution timeout.", null);
                }
            }
        };
    }

    static class SuspendWritesState {
        final AtomicInteger renewalCounter = new AtomicInteger();
        final AtomicInteger lastPersisted = new AtomicInteger(-1);
        final AtomicInteger persistedBeforeSuspension = new AtomicInteger();
        final AtomicBoolean writesWhileSuspendedDetected = new AtomicBoolean(false);
        final AtomicBoolean writesResumed = new AtomicBoolean(false);
        final AtomicBoolean writesSuspended = new AtomicBoolean(false);

        @NotNull
        Consumer<LockStatus> checkSuspendedWrites(Consumer<LockStatus> lockStatusChangedCallback) {
            return lockStatus -> {
                lockStatusChangedCallback.accept(lockStatus);
                if (lockStatus.equals(LockStatus.RENEWAL_FAILED)) {
                    writesSuspended.set(true);
                    if (renewalCounter.get() <= 1) {
                        persistedBeforeSuspension.set(lastPersisted.get());
                    } else if (lastPersisted.get() - persistedBeforeSuspension.get() > 1) { // diff of 1 is ok, as it may be caused by concurrency
                        writesWhileSuspendedDetected.set(true);
                    }
                } else if (lockStatus.equals(LockStatus.RENEWAL_SUCCEEDED)) {
                    writesResumed.set(true);
                }
            };
        }
    }
    
    @NotNull
    private static List<UUID> writeSegments(SegmentArchiveWriter writer, int maxSegments) throws IOException {
        try {
            List<UUID> uuids = new ArrayList<>();
            for (int i = 0; i < maxSegments; i++) {
                UUID u = UUID.randomUUID();
                writeSegment(writer, u);
                uuids.add(u);
            }

            writer.flush();
            return uuids;
        } finally {
            writer.close();
        }
    }

    private static void writeSegment(SegmentArchiveWriter writer, UUID u) throws IOException {
        writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
    }

    @Nullable
    private static Buffer readSegment(@Nullable SegmentArchiveReader reader, UUID u) throws IOException {
        assertNotNull(reader);
        return reader.readSegment(u.getMostSignificantBits(), u.getLeastSignificantBits());
    }

    @NotNull
    private static SegmentNodeStore segmentNodeStore(FileStore fs) {
        return SegmentNodeStoreBuilders.builder(fs).build();
    }

    @NotNull
    private static SegmentNodeStore segmentNodeStore(ReadOnlyFileStore fs) {
        return SegmentNodeStoreBuilders.builder(fs).build();
    }
    
    private static PersistentCache createPersistenceCache() {
        return new AbstractPersistentCache() {
            @Override
            protected Buffer readSegmentInternal(long msb, long lsb) {
                return null;
            }

            @Override
            public boolean containsSegment(long msb, long lsb) {
                return false;
            }

            @Override
            public void writeSegment(long msb, long lsb, Buffer buffer) {

            }

            @Override
            public void cleanUp() {

            }
        };
    }

    static class Fixture {
        private final CloudBlobContainer container;

        private AzurePersistence persistence;

        public Fixture(AzuriteDockerRule azurite) throws Exception {
            this.container = azurite.getContainer("oak-test");
            this.persistence = newAzurePersistence();
        }

        @NotNull
        AzurePersistence newAzurePersistence() throws URISyntaxException {
            return new AzurePersistence(container.getDirectoryReference("oak"));
        }

        void withSegmentStore(ThrowingConsumer<SegmentNodeStore> segmentNodeStoreConsumer) {
            try (FileStore fs = newFileStore()) {
                segmentNodeStoreConsumer.accept(segmentNodeStore(fs));
            } catch (Exception e) {
                throw rethrow(e);
            }
        }

        void withSegmentStore(ThrowingBiConsumer<FileStore, SegmentNodeStore> segmentNodeStoreConsumer) {
            catching(this::newFileStore, fs -> segmentNodeStoreConsumer.accept(fs, segmentNodeStore(fs)));
        }

        void withFileStore(ThrowingConsumer<FileStore> fileStoreConsumer) {
            catching(this::newFileStore, fileStoreConsumer);
        }

        void withReadOnlyFileStore(ThrowingConsumer<ReadOnlyFileStore> readOnlyFileStoreConsumer) {
            catching(() -> FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(persistence).buildReadOnly(), 
                readOnlyFileStoreConsumer);
        }

        static <T extends Closeable> void catching(ThrowingSupplier<T> supplier, ThrowingConsumer<T> consumer) {
            try (T t = supplier.get()) {
                consumer.accept(t);
            } catch (Exception e) {
                throw rethrow(e);
            }
        }

        private static RuntimeException rethrow(Exception e) {
            if (e instanceof RuntimeException) {
                return (RuntimeException) e;
            }
            return new RuntimeException(e);
        }

        void mergeChanges(Consumer<NodeBuilder> builderConsumer) {
            withSegmentStore(segmentNodeStore -> {
                NodeBuilder builder = segmentNodeStore.getRoot().builder();
                builderConsumer.accept(builder);
                segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            });
        }

        void flushChanges(FileStore fileStore, Consumer<NodeBuilder> builderConsumer) {
            try {
                SegmentNodeStore segmentNodeStore = segmentNodeStore(fileStore);
                NodeBuilder builder = segmentNodeStore.getRoot().builder();
                builderConsumer.accept(builder);
                segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                fileStore.flush();
            } catch (CommitFailedException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        @SafeVarargs
        final void flushEachChange(Consumer<NodeBuilder>... builderConsumers) {
            withSegmentStore((fs, segmentNodeStore) -> {
                NodeBuilder builder = segmentNodeStore.getRoot().builder();
                for (Consumer<NodeBuilder> builderConsumer : builderConsumers) {
                    builderConsumer.accept(builder);
                    segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    fs.flush();
                }
            });
        }

        void flushEachChange(int count, BiConsumer<Integer, NodeBuilder> builderConsumer, Consumer<Integer> afterFlushCallback) {
            withSegmentStore((fs, segmentNodeStore) -> {
                NodeBuilder builder = segmentNodeStore.getRoot().builder();
                for (int i = 0; i < count; i++) {
                    builderConsumer.accept(i, builder);
                    segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                    fs.flush();
                    afterFlushCallback.accept(i);
                }
            });
        }

        @NotNull
        FileStore newFileStore() {
            try {
                return FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(persistence).build();
            } catch (InvalidFileStoreVersionException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        SegmentArchiveManager newSegmentArchiveManager() {
            return persistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        }

        boolean hasBlobsInArchive(String directoryName) throws StorageException, URISyntaxException {
            return container.getDirectoryReference(directoryName).listBlobs().iterator().hasNext();
        }

        void deleteBlob(String blobName) throws StorageException, URISyntaxException {
            this.container.getBlockBlobReference(blobName).delete();
        }

        boolean blobExists(String blobName) throws StorageException, URISyntaxException {
            return this.container.getBlockBlobReference(blobName).exists();
        }

        ListBlobItem getFirstBlobWithPrefix(String prefix) {
            return listBlobsWithPrefix(prefix).next();
        }

        boolean hasBlobsWithPrefix(String prefix) {
            return listBlobsWithPrefix(prefix).hasNext();
        }

        @NotNull
        Iterator<ListBlobItem> listBlobsWithPrefix(String prefix) {
            return container.listBlobs(prefix).iterator();
        }

        void deleteFirstBlobWithPrefix(String prefix) throws StorageException {
            ListBlobItem firstBlob = getFirstBlobWithPrefix(prefix);
            ((CloudBlob) firstBlob).delete();
        }

        @NotNull
        CloudBlobDirectory getOakDirectory() {
            try {
                return container.getDirectoryReference("oak");
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    interface ThrowingConsumer<T> {
        void accept(T t) throws Exception;
    }

    interface ThrowingBiConsumer<T, U> {
        void accept(T t, U u) throws Exception;
    }
    
    interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
