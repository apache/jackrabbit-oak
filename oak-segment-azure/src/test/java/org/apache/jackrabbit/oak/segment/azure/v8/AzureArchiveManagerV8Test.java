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
package org.apache.jackrabbit.oak.segment.azure.v8;

import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
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
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
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
import org.junit.*;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AzureArchiveManagerV8Test {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private CloudBlobContainer container;

    private AzurePersistenceV8 azurePersistenceV8;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        container = azurite.getContainer("oak-test");

        WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();
        azurePersistenceV8 = new AzurePersistenceV8(container.getDirectoryReference("oak"));
        azurePersistenceV8.setWriteAccessController(writeAccessController);
    }

    @Rule
    public final ProvideSystemProperty systemPropertyRule = new ProvideSystemProperty(AzureRepositoryLockV8.LEASE_DURATION_PROP, "15")
            .and(AzureRepositoryLockV8.RENEWAL_INTERVAL_PROP, "3")
            .and(AzureRepositoryLockV8.TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, "9");

    @Test
    public void testRecovery() throws StorageException, URISyntaxException, IOException {
        SegmentArchiveManager manager = azurePersistenceV8.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID u = UUID.randomUUID();
            writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
            uuids.add(u);
        }

        writer.flush();
        writer.close();

        container.getBlockBlobReference("oak/data00000a.tar/0005." + uuids.get(5).toString()).delete();

        LinkedHashMap<UUID, byte[]> recovered = new LinkedHashMap<>();
        manager.recoverEntries("data00000a.tar", recovered);
        assertEquals(uuids.subList(0, 5), newArrayList(recovered.keySet()));
    }

    @Test
    public void testBackupWithRecoveredEntries() throws StorageException, URISyntaxException, IOException {
        SegmentArchiveManager manager = azurePersistenceV8.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID u = UUID.randomUUID();
            writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
            uuids.add(u);
        }

        writer.flush();
        writer.close();

        container.getBlockBlobReference("oak/data00000a.tar/0005." + uuids.get(5).toString()).delete();

        LinkedHashMap<UUID, byte[]> recovered = new LinkedHashMap<>();
        manager.recoverEntries("data00000a.tar", recovered);

        manager.backup("data00000a.tar", "data00000a.tar.bak", recovered.keySet());

        for (int i = 0; i <= 4; i++) {
            assertTrue(container.getBlockBlobReference("oak/data00000a.tar/000"+ i + "." + uuids.get(i)).exists());
        }

        for (int i = 5; i <= 9; i++) {
            assertFalse(String.format("Segment %s.??? should have been deleted.", "oak/data00000a.tar/000"+ i), container.getBlockBlobReference("oak/data00000a.tar/000"+ i + "." + uuids.get(i)).exists());
        }
    }

    @Test
    public void testUncleanStop() throws URISyntaxException, IOException, InvalidFileStoreVersionException, CommitFailedException, StorageException {
        AzurePersistenceV8 p = new AzurePersistenceV8(container.getDirectoryReference("oak"));
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();

        container.getBlockBlobReference("oak/data00000a.tar/closed").delete();
        container.getBlockBlobReference("oak/data00000a.tar/data00000a.tar.brf").delete();
        container.getBlockBlobReference("oak/data00000a.tar/data00000a.tar.gph").delete();

        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));
        fs.close();
    }

    @Test
    // see OAK-8566
    public void testUncleanStopWithEmptyArchive() throws URISyntaxException, IOException, InvalidFileStoreVersionException, CommitFailedException, StorageException {
        AzurePersistenceV8 p = new AzurePersistenceV8(container.getDirectoryReference("oak"));
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();

        // make sure there are 2 archives
        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo2", "bar2");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();

        // remove the segment 0000 from the second archive
        ListBlobItem segment0000 = container.listBlobs("oak/data00001a.tar/0000.").iterator().next();
        ((CloudBlob) segment0000).delete();
        container.getBlockBlobReference("oak/data00001a.tar/closed").delete();

        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));
        fs.close();
    }

    @Test
    public void testUncleanStopSegmentMissing() throws URISyntaxException, IOException, InvalidFileStoreVersionException, CommitFailedException, StorageException {
        AzurePersistenceV8 p = new AzurePersistenceV8(container.getDirectoryReference("oak"));
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();

        // make sure there are 2 archives
        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo0", "bar0");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.flush();
        //create segment 0001
        builder.setProperty("foo1", "bar1");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.flush();
        //create segment 0002
        builder.setProperty("foo2", "bar2");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.flush();
        //create segment 0003
        builder.setProperty("foo3", "bar3");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.flush();
        fs.close();

        // remove the segment 0002 from the second archive
        ListBlobItem segment0002 = container.listBlobs("oak/data00001a.tar/0002.").iterator().next();
        ((CloudBlob) segment0002).delete();
        container.getBlockBlobReference("oak/data00001a.tar/closed").delete();

        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));

        //recovered archive data00001a.tar should not contain segments 0002 and 0003
        assertFalse(container.listBlobs("oak/data00001a.tar/0002.").iterator().hasNext());
        assertFalse(container.listBlobs("oak/data00001a.tar/0003.").iterator().hasNext());

        assertTrue("Backup directory should have been created", container.listBlobs("oak/data00001a.tar.bak").iterator().hasNext());
        //backup has all segments but 0002 since it was deleted before recovery
        assertTrue(container.listBlobs("oak/data00001a.tar.bak/0001.").iterator().hasNext());
        assertFalse(container.listBlobs("oak/data00001a.tar.bak/0002.").iterator().hasNext());
        assertTrue(container.listBlobs("oak/data00001a.tar.bak/0003.").iterator().hasNext());

        //verify content from recovered segments preserved
        assertEquals("bar1", segmentNodeStore.getRoot().getString("foo1"));
        //content from deleted segments not preserved
        assertNull(segmentNodeStore.getRoot().getString("foo2"));
        assertNull(segmentNodeStore.getRoot().getString("foo3"));
        fs.close();
    }

    @Test
    public void testExists() throws IOException, URISyntaxException {
        SegmentArchiveManager manager = azurePersistenceV8.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID u = UUID.randomUUID();
            writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
            uuids.add(u);
        }

        writer.flush();
        writer.close();

        Assert.assertTrue(manager.exists("data00000a.tar"));
        Assert.assertFalse(manager.exists("data00001a.tar"));
    }

    @Test
    public void testArchiveExistsAfterFlush() throws URISyntaxException, IOException {
        SegmentArchiveManager manager = azurePersistenceV8.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        Assert.assertFalse(manager.exists("data00000a.tar"));
        UUID u = UUID.randomUUID();
        writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
        writer.flush();
        Assert.assertTrue(manager.exists("data00000a.tar"));
    }

    @Test(expected = FileNotFoundException.class)
    public void testSegmentDeletedAfterCreatingReader() throws IOException, URISyntaxException, StorageException {
        SegmentArchiveManager manager = azurePersistenceV8.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        Assert.assertFalse(manager.exists("data00000a.tar"));
        UUID u = UUID.randomUUID();
        writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
        writer.flush();
        writer.close();

        SegmentArchiveReader reader = manager.open("data00000a.tar");
        Buffer segment = reader.readSegment(u.getMostSignificantBits(), u.getLeastSignificantBits());
        assertNotNull(segment);

        ListBlobItem segment0000 = container.listBlobs("oak/data00000a.tar/0000.").iterator().next();
        ((CloudBlob) segment0000).delete();

        try {
            // FileNotFoundException should be thrown here
            reader.readSegment(u.getMostSignificantBits(), u.getLeastSignificantBits());
            fail();
        } catch (RepositoryNotReachableException e) {
            fail();
        }
    }

    @Test(expected = SegmentNotFoundException.class)
    public void testMissngSegmentDetectedInFileStore() throws IOException, StorageException, URISyntaxException, InvalidFileStoreVersionException {

        AzurePersistenceV8 azurePersistenceV8 = new AzurePersistenceV8(container.getDirectoryReference("oak"));
        FileStore fileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(azurePersistenceV8).build();

        SegmentArchiveManager manager = azurePersistenceV8.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        //Assert.assertFalse(manager.exists("data00000a.tar"));
        UUID u = UUID.randomUUID();
        writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
        writer.flush();
        writer.close();

        SegmentArchiveReader reader = manager.open("data00000a.tar");
        Buffer segment = reader.readSegment(u.getMostSignificantBits(), u.getLeastSignificantBits());
        assertNotNull(segment);

        ListBlobItem segment0000 = container.listBlobs("oak/data00000a.tar/0000.").iterator().next();
        ((CloudBlob) segment0000).delete();

        // SegmentNotFoundException should be thrown here
        fileStore.readSegment(new SegmentId(fileStore, u.getMostSignificantBits(), u.getLeastSignificantBits()));
    }

    @Test
    public void testReadOnlyRecovery() throws URISyntaxException, InvalidFileStoreVersionException, IOException, CommitFailedException, StorageException {
        AzurePersistenceV8 rwPersistence = new AzurePersistenceV8(container.getDirectoryReference("oak"));
        FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(rwPersistence).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        rwFileStore.flush();

        assertTrue(container.getDirectoryReference("oak/data00000a.tar").listBlobs().iterator().hasNext());
        assertFalse(container.getDirectoryReference("oak/data00000a.tar.ro.bak").listBlobs().iterator().hasNext());

        // create read-only FS
        AzurePersistenceV8 roPersistence = new AzurePersistenceV8(container.getDirectoryReference("oak"));
        ReadOnlyFileStore roFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(roPersistence).buildReadOnly();

        PropertyState fooProperty = SegmentNodeStoreBuilders.builder(roFileStore).build()
                .getRoot()
                .getProperty("foo");
        assertThat(fooProperty, not(nullValue()));
        assertThat(fooProperty.getValue(Type.STRING), equalTo("bar"));

        roFileStore.close();
        rwFileStore.close();

        assertTrue(container.getDirectoryReference("oak/data00000a.tar").listBlobs().iterator().hasNext());
        // after creating a read-only FS, the recovery procedure should not be started since there is another running Oak process
        assertFalse(container.getDirectoryReference("oak/data00000a.tar.ro.bak").listBlobs().iterator().hasNext());
    }

    @Test
    public void testCachingPersistenceTarRecovery() throws URISyntaxException, InvalidFileStoreVersionException, IOException, CommitFailedException, StorageException {
        AzurePersistenceV8 rwPersistence = new AzurePersistenceV8(container.getDirectoryReference("oak"));
        FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(folder.newFolder()).withCustomPersistence(rwPersistence).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        rwFileStore.flush();

        assertTrue(container.getDirectoryReference("oak/data00000a.tar").listBlobs().iterator().hasNext());
        assertFalse(container.getDirectoryReference("oak/data00000a.tar.ro.bak").listBlobs().iterator().hasNext());

        // create files store with split persistence
        AzurePersistenceV8 azureSharedPersistence = new AzurePersistenceV8(container.getDirectoryReference("oak"));

        CachingPersistence cachingPersistence = new CachingPersistence(createPersistenceCache(), azureSharedPersistence);
        File localFolder = folder.newFolder();
        SegmentNodeStorePersistence localPersistence = new TarPersistence(localFolder);
        SegmentNodeStorePersistence splitPersistence = new SplitPersistence(cachingPersistence, localPersistence);

        // exception should not be thrown here
        FileStore splitPersistenceFileStore = FileStoreBuilder.fileStoreBuilder(localFolder).withCustomPersistence(splitPersistence).build();

        assertTrue(container.getDirectoryReference("oak/data00000a.tar").listBlobs().iterator().hasNext());
        // after creating a read-only FS, the recovery procedure should not be started since there is another running Oak process
        assertFalse(container.getDirectoryReference("oak/data00000a.tar.ro.bak").listBlobs().iterator().hasNext());
    }

    @Test
    public void testCollectBlobReferencesForReadOnlyFileStore() throws URISyntaxException, InvalidFileStoreVersionException, IOException, CommitFailedException, StorageException {
        AzurePersistenceV8 rwPersistence = new AzurePersistenceV8(container.getDirectoryReference("oak"));
        try (FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(rwPersistence).build()) {
            SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
            NodeBuilder builder = segmentNodeStore.getRoot().builder();
            builder.setProperty("foo", "bar");
            segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            rwFileStore.flush();

            // file with binary references is not created yet
            assertFalse("brf file should not be present", container.getDirectoryReference("oak/data00000a.tar").getBlockBlobReference("data00000a.tar.brf").exists());

            // create read-only FS, while the rw FS is still open
            AzurePersistenceV8 roPersistence = new AzurePersistenceV8(container.getDirectoryReference("oak"));
            try (ReadOnlyFileStore roFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(roPersistence).buildReadOnly()) {

                PropertyState fooProperty = SegmentNodeStoreBuilders.builder(roFileStore).build()
                        .getRoot()
                        .getProperty("foo");

                assertThat(fooProperty, not(nullValue()));
                assertThat(fooProperty.getValue(Type.STRING), equalTo("bar"));

                assertDoesNotThrow(() -> roFileStore.collectBlobReferences(s -> {
                }));
            }
        }
    }

    @Test
    public void testCollectBlobReferencesDoesNotFailWhenFileIsMissing() throws URISyntaxException, InvalidFileStoreVersionException, IOException, CommitFailedException, StorageException {
        AzurePersistenceV8 rwPersistence = new AzurePersistenceV8(container.getDirectoryReference("oak"));
        try (FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(rwPersistence).build()) {
            SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
            NodeBuilder builder = segmentNodeStore.getRoot().builder();
            builder.setProperty("foo", "bar");
            segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            rwFileStore.flush();

            // file with binary references is not created yet
            assertFalse("brf file should not be present", container.getDirectoryReference("oak/data00000a.tar").getBlockBlobReference("data00000a.tar.brf").exists());

            // create read-only FS, while the rw FS is still open
            AzurePersistenceV8 roPersistence = new AzurePersistenceV8(container.getDirectoryReference("oak"));
            try (ReadOnlyFileStore roFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(roPersistence).buildReadOnly()) {

                PropertyState fooProperty = SegmentNodeStoreBuilders.builder(roFileStore).build()
                        .getRoot()
                        .getProperty("foo");

                assertThat(fooProperty, not(nullValue()));
                assertThat(fooProperty.getValue(Type.STRING), equalTo("bar"));

                HashSet<String> references = new HashSet<>();
                assertDoesNotThrow(() ->
                        roFileStore.collectBlobReferences(references::add));

                assertTrue("No references should have been collected since reference file has not been created", references.isEmpty());
            }
        }
    }

    @Test
    public void testWriteAfterLosingRepoLock() throws Exception {
        CloudBlobDirectory oakDirectory = container.getDirectoryReference("oak");
        AzurePersistenceV8 rwPersistence = new AzurePersistenceV8(oakDirectory);

        CloudBlockBlob blob = container.getBlockBlobReference("oak/repo.lock");

        CloudBlockBlob blobMocked = Mockito.spy(blob);

        Mockito
                .doCallRealMethod()
                .when(blobMocked).renewLease(Mockito.any(), Mockito.any(), Mockito.any());

        AzurePersistenceV8 mockedRwPersistence = Mockito.spy(rwPersistence);
        WriteAccessController writeAccessController = new WriteAccessController();
        AzureRepositoryLockV8 azureRepositoryLockV8 = new AzureRepositoryLockV8(blobMocked, () -> {}, writeAccessController);
        AzureArchiveManagerV8 azureArchiveManagerV8 = new AzureArchiveManagerV8(oakDirectory, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), writeAccessController);


        Mockito
                .doAnswer(invocation -> azureRepositoryLockV8.lock())
                .when(mockedRwPersistence).lockRepository();

        Mockito
                .doReturn(azureArchiveManagerV8)
                .when(mockedRwPersistence).createArchiveManager(Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito
                .doReturn(new AzureJournalFileV8(oakDirectory, "journal.log", writeAccessController))
                .when(mockedRwPersistence).getJournalFile();

        FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(folder.newFolder()).withCustomPersistence(mockedRwPersistence).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();


        // simulate operation timeout when trying to renew lease
        Mockito.reset(blobMocked);

        StorageException storageException =
                new StorageException(StorageErrorCodeStrings.OPERATION_TIMED_OUT, "operation timeout", new TimeoutException());

        Mockito.doThrow(storageException).when(blobMocked).renewLease(Mockito.any(), Mockito.any(), Mockito.any());


        // wait till lease expires
        Thread.sleep(17000);

        // try updating repository
        Thread thread = new Thread(() -> {
            try {
                builder.setProperty("foo", "bar");
                segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
                rwFileStore.flush();
            } catch (Exception e) {
                fail("No Exception expected, but got: " + e.getMessage());
            }
        });
        thread.start();

        Thread.sleep(2000);

        // It should be possible to start another RW file store.
        FileStore rwFileStore2 = FileStoreBuilder.fileStoreBuilder(folder.newFolder()).withCustomPersistence(new AzurePersistenceV8(oakDirectory)).build();
        SegmentNodeStore segmentNodeStore2 = SegmentNodeStoreBuilders.builder(rwFileStore2).build();
        NodeBuilder builder2 = segmentNodeStore2.getRoot().builder();

        //repository hasn't been updated
        assertNull(builder2.getProperty("foo"));

        rwFileStore2.close();
    }

    private PersistentCache createPersistenceCache() {
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

    private static void assertDoesNotThrow(Executable executable) {
        try {
            executable.execute();
        } catch (Exception e) {
            fail("No Exception expected, but got: " + e.getMessage());
        }
    }

    interface Executable {
        void execute() throws Exception;
    }
}
