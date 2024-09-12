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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
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
import java.util.*;
import java.util.concurrent.TimeoutException;

import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.*;

public class AzureArchiveManagerTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private BlobContainerClient readBlobContainerClient;
    private BlobContainerClient writeBlobContainerClient;

    private AzurePersistence azurePersistence;

    @Before
    public void setup() throws BlobStorageException, InvalidKeyException, URISyntaxException {
        readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
        writeBlobContainerClient = azurite.getWriteBlobContainerClient("oak-test");

        WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();
        azurePersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient, "oak");
        azurePersistence.setWriteAccessController(writeAccessController);
    }

    @Rule
    public final ProvideSystemProperty systemPropertyRule = new ProvideSystemProperty(AzureRepositoryLock.LEASE_DURATION_PROP, "15")
            .and(AzureRepositoryLock.RENEWAL_INTERVAL_PROP, "3")
            .and(AzureRepositoryLock.TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, "9");

    @Test
    public void testRecovery() throws BlobStorageException, IOException {
        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID u = UUID.randomUUID();
            writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
            uuids.add(u);
        }

        writer.flush();
        writer.close();

        readBlobContainerClient.getBlobClient("oak/data00000a.tar/0005." + uuids.get(5).toString()).delete();

        LinkedHashMap<UUID, byte[]> recovered = new LinkedHashMap<>();
        manager.recoverEntries("data00000a.tar", recovered);
        assertEquals(uuids.subList(0, 5), newArrayList(recovered.keySet()));
    }

    @Test
    public void testBackupWithRecoveredEntries() throws BlobStorageException, IOException {
        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID u = UUID.randomUUID();
            writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
            uuids.add(u);
        }

        writer.flush();
        writer.close();

        readBlobContainerClient.getBlobClient("oak/data00000a.tar/0005." + uuids.get(5).toString()).delete();

        LinkedHashMap<UUID, byte[]> recovered = new LinkedHashMap<>();
        manager.recoverEntries("data00000a.tar", recovered);

        manager.backup("data00000a.tar", "data00000a.tar.bak", recovered.keySet());

        for (int i = 0; i <= 4; i++) {
            assertTrue(readBlobContainerClient.getBlobClient("oak/data00000a.tar/000"+ i + "." + uuids.get(i)).exists());
        }

        for (int i = 5; i <= 9; i++) {
            assertFalse(String.format("Segment %s.??? should have been deleted.", "oak/data00000a.tar/000"+ i), readBlobContainerClient.getBlobClient("oak/data00000a.tar/000"+ i + "." + uuids.get(i)).exists());
        }
    }

    @Test
    public void testUncleanStop() throws IOException, InvalidFileStoreVersionException, CommitFailedException, BlobStorageException {
        AzurePersistence p = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient,"oak");
        FileStore fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        fs.close();


        readBlobContainerClient.getBlobClient("oak/data00000a.tar/closed").delete();
        readBlobContainerClient.getBlobClient("oak/data00000a.tar/data00000a.tar.brf").delete();
        readBlobContainerClient.getBlobClient("oak/data00000a.tar/data00000a.tar.gph").delete();

        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));
        fs.close();
    }

    @Test
    // see OAK-8566
    public void testUncleanStopWithEmptyArchive() throws IOException, InvalidFileStoreVersionException, CommitFailedException, BlobStorageException {
        AzurePersistence p = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient, "oak");
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
        ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
        listBlobsOptions.setPrefix("oak/data00001a.tar/0000.");

        BlobItem blobItem = readBlobContainerClient.listBlobs(listBlobsOptions, null).iterator().next();
        readBlobContainerClient.getBlobClient(blobItem.getName()).delete();
        readBlobContainerClient.getBlobClient("oak/data00001a.tar/closed").delete();

        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));
        fs.close();
    }

    @Test
    public void testUncleanStopSegmentMissing() throws IOException, InvalidFileStoreVersionException, CommitFailedException, BlobStorageException {
        AzurePersistence p = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient,"oak");
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
        ListBlobsOptions listOptions = new ListBlobsOptions();
        listOptions.setPrefix("oak/data00001a.tar/0002.");
        BlobItem blobItem = readBlobContainerClient.listBlobs(listOptions, null).stream().iterator().next();
        readBlobContainerClient.getBlobClient(blobItem.getName()).getBlockBlobClient().delete();
        readBlobContainerClient.getBlobClient("oak/data00001a.tar/closed").delete();

        fs = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(p).build();
        segmentNodeStore = SegmentNodeStoreBuilders.builder(fs).build();
        assertEquals("bar", segmentNodeStore.getRoot().getString("foo"));

        //recovered archive data00001a.tar should not contain segments 0002 and 0003
        listOptions.setPrefix("oak/data00001a.tar/0002.");
        assertFalse(readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());
        listOptions.setPrefix("oak/data00001a.tar/0003.");
        assertFalse(readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());

        listOptions.setPrefix("oak/data00001a.tar.bak");
        assertTrue("Backup directory should have been created", readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());
        //backup has all segments but 0002 since it was deleted before recovery
        listOptions.setPrefix("oak/data00001a.tar.bak/0001.");
        assertTrue(readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());
        listOptions.setPrefix("oak/data00001a.tar.bak/0002.");
        assertFalse(readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());
        listOptions.setPrefix("oak/data00001a.tar.bak/0003.");
        assertTrue(readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());

        //verify content from recovered segments preserved
        assertEquals("bar1", segmentNodeStore.getRoot().getString("foo1"));
        //content from deleted segments not preserved
        assertNull(segmentNodeStore.getRoot().getString("foo2"));
        assertNull(segmentNodeStore.getRoot().getString("foo3"));
        fs.close();
    }

    @Test
    public void testExists() throws IOException {
        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
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
    public void testArchiveExistsAfterFlush() throws IOException {
        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        Assert.assertFalse(manager.exists("data00000a.tar"));
        UUID u = UUID.randomUUID();
        writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
        writer.flush();
        Assert.assertTrue(manager.exists("data00000a.tar"));
    }

    @Test(expected = FileNotFoundException.class)
    public void testSegmentDeletedAfterCreatingReader() throws IOException, BlobStorageException {
        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        Assert.assertFalse(manager.exists("data00000a.tar"));
        UUID u = UUID.randomUUID();
        writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
        writer.flush();
        writer.close();

        SegmentArchiveReader reader = manager.open("data00000a.tar");
        Buffer segment = reader.readSegment(u.getMostSignificantBits(), u.getLeastSignificantBits());
        assertNotNull(segment);

        ListBlobsOptions listOptions = new ListBlobsOptions();
        listOptions.setPrefix("oak/data00000a.tar/0000.");
        BlobItem segment0000 = readBlobContainerClient.listBlobs(listOptions, null).iterator().next();
        readBlobContainerClient.getBlobClient(segment0000.getName()).delete();

        try {
            // FileNotFoundException should be thrown here
            reader.readSegment(u.getMostSignificantBits(), u.getLeastSignificantBits());
            fail();
        } catch (RepositoryNotReachableException e) {
            fail();
        }
    }

    @Test(expected = SegmentNotFoundException.class)
    public void testMissingSegmentDetectedInFileStore() throws IOException, BlobStorageException, InvalidFileStoreVersionException {

        AzurePersistence azurePersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient,"oak");
        FileStore fileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(azurePersistence).build();

        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");

        //Assert.assertFalse(manager.exists("data00000a.tar"));
        UUID u = UUID.randomUUID();
        writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
        writer.flush();
        writer.close();

        SegmentArchiveReader reader = manager.open("data00000a.tar");
        Buffer segment = reader.readSegment(u.getMostSignificantBits(), u.getLeastSignificantBits());
        assertNotNull(segment);

        ListBlobsOptions listOptions = new ListBlobsOptions();
        listOptions.setPrefix("oak/data00000a.tar/0000.");
        BlobItem segment0000 = readBlobContainerClient.listBlobs(listOptions, null).iterator().next();
        readBlobContainerClient.getBlobClient(segment0000.getName()).delete();

        // SegmentNotFoundException should be thrown here
        fileStore.readSegment(new SegmentId(fileStore, u.getMostSignificantBits(), u.getLeastSignificantBits()));
    }

    @Test
    public void testReadOnlyRecovery() throws InvalidFileStoreVersionException, IOException, CommitFailedException, BlobStorageException {
        AzurePersistence rwPersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient,"oak");
        FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(rwPersistence).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        rwFileStore.flush();

        ListBlobsOptions listOptions = new ListBlobsOptions();
        listOptions.setPrefix("oak/data00000a.tar");
        assertTrue(readBlobContainerClient.listBlobs(listOptions,null).iterator().hasNext());
        listOptions.setPrefix("oak/data00000a.tar.ro.bak");
        assertFalse(readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());

        // create read-only FS
        AzurePersistence roPersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient, "oak");
        ReadOnlyFileStore roFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(roPersistence).buildReadOnly();

        PropertyState fooProperty = SegmentNodeStoreBuilders.builder(roFileStore).build()
                .getRoot()
                .getProperty("foo");
        assertThat(fooProperty, not(nullValue()));
        assertThat(fooProperty.getValue(Type.STRING), equalTo("bar"));

        roFileStore.close();
        rwFileStore.close();

        listOptions.setPrefix("oak/data00000a.tar");
        assertTrue(readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());
        // after creating a read-only FS, the recovery procedure should not be started since there is another running Oak process
        listOptions.setPrefix("oak/data00000a.tar.ro.bak");
        assertFalse(readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());
    }

    @Test
    public void testCachingPersistenceTarRecovery() throws InvalidFileStoreVersionException, IOException, CommitFailedException, BlobStorageException {
        AzurePersistence rwPersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient,"oak");
        FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(folder.newFolder()).withCustomPersistence(rwPersistence).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();
        builder.setProperty("foo", "bar");
        segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        rwFileStore.flush();

        ListBlobsOptions listOptions = new ListBlobsOptions();
        listOptions.setPrefix("oak/data00000a.tar");
        assertTrue(readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());
        listOptions.setPrefix("oak/data00000a.tar.ro.bak");
        assertFalse(readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());

        // create files store with split persistence
        AzurePersistence azureSharedPersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient,"oak");

        CachingPersistence cachingPersistence = new CachingPersistence(createPersistenceCache(), azureSharedPersistence);
        File localFolder = folder.newFolder();
        SegmentNodeStorePersistence localPersistence = new TarPersistence(localFolder);
        SegmentNodeStorePersistence splitPersistence = new SplitPersistence(cachingPersistence, localPersistence);

        // exception should not be thrown here
        FileStore splitPersistenceFileStore = FileStoreBuilder.fileStoreBuilder(localFolder).withCustomPersistence(splitPersistence).build();

        listOptions.setPrefix("oak/data00000a.tar");
        assertTrue(readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());
        // after creating a read-only FS, the recovery procedure should not be started since there is another running Oak process
        listOptions.setPrefix("oak/data00000a.tar.ro.bak");
        assertFalse(readBlobContainerClient.listBlobs(listOptions, null).iterator().hasNext());
    }

    @Test
    public void testCollectBlobReferencesForReadOnlyFileStore() throws InvalidFileStoreVersionException, IOException, CommitFailedException, BlobStorageException {
        AzurePersistence rwPersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient,"oak");
        try (FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(rwPersistence).build()) {
            SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
            NodeBuilder builder = segmentNodeStore.getRoot().builder();
            builder.setProperty("foo", "bar");
            segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            rwFileStore.flush();

            // file with binary references is not created yet
            assertFalse("brf file should not be present", readBlobContainerClient.getBlobClient("oak/data00000a.tar/data00000a.tar.brf").exists());

            // create read-only FS, while the rw FS is still open
            AzurePersistence roPersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient, "oak");
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
    public void testCollectBlobReferencesDoesNotFailWhenFileIsMissing() throws InvalidFileStoreVersionException, IOException, CommitFailedException, BlobStorageException {
        AzurePersistence rwPersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient, "oak");
        try (FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(new File("target")).withCustomPersistence(rwPersistence).build()) {
            SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
            NodeBuilder builder = segmentNodeStore.getRoot().builder();
            builder.setProperty("foo", "bar");
            segmentNodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            rwFileStore.flush();

            // file with binary references is not created yet
            assertFalse("brf file should not be present", readBlobContainerClient.getBlobClient("oak/data00000a.tar/data00000a.tar.brf").exists());

            // create read-only FS, while the rw FS is still open
            AzurePersistence roPersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient, "oak");
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
        BlobContainerClient oakDirectory = readBlobContainerClient.getBlobClient("oak").getContainerClient();
        BlobContainerClient writeOakDirectory = writeBlobContainerClient.getBlobClient("oak").getContainerClient();
        AzurePersistence rwPersistence = new AzurePersistence(oakDirectory, writeOakDirectory, "");

        BlockBlobClient blob = readBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlobLeaseClient leaseClient = new BlobLeaseClientBuilder().blobClient(blob).buildClient();


        BlockBlobClient blobMocked = Mockito.spy(blob);
        BlobLeaseClient blobLeaseMocked = Mockito.spy(leaseClient);

        Mockito
                .doCallRealMethod()
                .when(blobLeaseMocked).renewLease();

        AzurePersistence mockedRwPersistence = Mockito.spy(rwPersistence);
        WriteAccessController writeAccessController = new WriteAccessController();
        AzureRepositoryLock azureRepositoryLock = new AzureRepositoryLock(blobMocked, blobLeaseMocked, () -> {}, writeAccessController);
        AzureArchiveManager azureArchiveManager = new AzureArchiveManager(oakDirectory, writeOakDirectory, "", new IOMonitorAdapter(), new FileStoreMonitorAdapter(), writeAccessController);


        Mockito
                .doAnswer(invocation -> azureRepositoryLock.lock())
                .when(mockedRwPersistence).lockRepository();

        Mockito
                .doReturn(azureArchiveManager)
                .when(mockedRwPersistence).createArchiveManager(Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito
                .doReturn(new AzureJournalFile(oakDirectory, writeOakDirectory, "journal.log", writeAccessController))
                .when(mockedRwPersistence).getJournalFile();

        FileStore rwFileStore = FileStoreBuilder.fileStoreBuilder(folder.newFolder()).withCustomPersistence(mockedRwPersistence).build();
        SegmentNodeStore segmentNodeStore = SegmentNodeStoreBuilders.builder(rwFileStore).build();
        NodeBuilder builder = segmentNodeStore.getRoot().builder();


        // simulate operation timeout when trying to renew lease
        Mockito.reset(blobMocked);

        BlobStorageException storageException =
                //new BlobStorageException("operation timeout", BlobErrorCode.OPERATION_TIMED_OUT, new TimeoutException());
                new BlobStorageException("operation timeout", null, new TimeoutException());

        Mockito.doThrow(storageException).when(blobLeaseMocked).renewLease();


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
        FileStore rwFileStore2 = FileStoreBuilder.fileStoreBuilder(folder.newFolder()).withCustomPersistence(new AzurePersistence(oakDirectory, writeOakDirectory, "")).build();
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
