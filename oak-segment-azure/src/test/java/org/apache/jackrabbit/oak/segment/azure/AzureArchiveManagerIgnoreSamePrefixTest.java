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

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class AzureArchiveManagerIgnoreSamePrefixTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private BlobContainerClient readBlobContainerClient;
    private BlobContainerClient writeBlobContainerClient;

    private AzurePersistence azurePersistence;

    private static final String rootPrefix = "oak";
    private static final String segmentName = "0004.44b4a246-50e0-470a-abe4-5a37a81c37c1";

    @Before
    public void setup() throws BlobStorageException, InvalidKeyException, URISyntaxException, IOException {
        readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
        writeBlobContainerClient = azurite.getWriteBlobContainerClient("oak-test");
        BlobContainerClient noRetryBlobContainerClient = azurite.getNoRetryBlobContainerClient("oak-test");

        WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();
        azurePersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient, noRetryBlobContainerClient, rootPrefix);
        azurePersistence.setWriteAccessController(writeAccessController);
    }

    @Rule
    public final ProvideSystemProperty systemPropertyRule = new ProvideSystemProperty(AzureRepositoryLock.LEASE_DURATION_PROP, "15")
            .and(AzureRepositoryLock.RENEWAL_INTERVAL_PROP, "3")
            .and(AzureRepositoryLock.TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, "9");

    @Test
    public void testRecoveryArchiveIgnoreArchiveSamePrefix() throws BlobStorageException, IOException {
        final String archiveName = "data00000a.tar";
        final String bakArchiveName = archiveName + ".4.bak";

        //create blob with same prefix as archiveName
        writeBlobContainerClient.getBlobClient(rootPrefix + "/" + bakArchiveName + "/" + segmentName)
                .getBlockBlobClient().upload(BinaryData.fromString("test-data-segment-content"));

        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create(archiveName);

        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID u = UUID.randomUUID();
            writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
            uuids.add(u);
        }

        writer.flush();
        writer.close();

        readBlobContainerClient.getBlobClient("oak/" + archiveName + "/0005." + uuids.get(5).toString()).delete();

        LinkedHashMap<UUID, byte[]> recovered = new LinkedHashMap<>();
        manager.recoverEntries(archiveName, recovered);
        assertEquals(uuids.subList(0, 5), new ArrayList<>(recovered.keySet()));
    }

    @Test
    public void testExistsArchiveIgnoreArchiveSamePrefix() {
        final String archiveName = "data00001a.tar";
        final String bakArchiveName = archiveName + ".4.bak";

        writeBlobContainerClient.getBlobClient(rootPrefix + "/" + bakArchiveName + "/" + segmentName)
                .getBlockBlobClient().upload(BinaryData.fromString("test-data-segment-content"));

        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());

        assertFalse(manager.exists(archiveName));
    }

    @Test
    public void testRenameToIgnoreBlobsSamePrefix() {
        final String archiveName = "data00002a.tar";
        final String bakArchiveName = archiveName + ".4.bak";
        final String targetArchiveName = "data00003a.tar";

        writeBlobContainerClient.getBlobClient(rootPrefix + "/" + bakArchiveName + "/" + segmentName)
                .getBlockBlobClient().upload(BinaryData.fromString("test-data-segment-content"));

        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        manager.renameTo(archiveName, targetArchiveName);

        boolean blobExists = readBlobContainerClient.listBlobs(new ListBlobsOptions().setPrefix(rootPrefix + "/" + targetArchiveName), null)
                .iterator().hasNext();

        assertFalse("blob from backup tar archive should not be renamed", blobExists);
    }

    @Test
    public void testCopyFileIgnoreOtherArchivesSamePrefix() throws IOException {
        final String archiveName = "data00003a.tar";
        final String bakArchiveName = archiveName + ".4.bak";
        final String targetArchiveName = "data00004a.tar";

        writeBlobContainerClient.getBlobClient(rootPrefix + "/" + bakArchiveName + "/" + segmentName)
                .getBlockBlobClient().upload(BinaryData.fromString("test-data-segment-content"));

        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        manager.copyFile(archiveName, targetArchiveName);

        boolean blobExistsInTargetArchive = readBlobContainerClient.listBlobs(new ListBlobsOptions().setPrefix(rootPrefix + "/" + targetArchiveName), null)
                .iterator().hasNext();

        assertFalse("blob from backup tar archive should not be copied", blobExistsInTargetArchive);
    }

    @Test
    public void testDeleteIgnoreOtherArchivesSamePrefix() {
        final String archiveName = "data00004a.tar";
        final String bakArchiveName = archiveName + ".4.bak";

        writeBlobContainerClient.getBlobClient(rootPrefix + "/" + bakArchiveName + "/" + segmentName)
                .getBlockBlobClient().upload(BinaryData.fromString("test-data-segment-content"));
        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        manager.delete(archiveName);

        boolean blobExists = readBlobContainerClient.listBlobs(new ListBlobsOptions().setPrefix(rootPrefix + "/" + bakArchiveName + "/"), null)
                .iterator().hasNext();

        assertTrue("blob from backup tar archive should not be deleted", blobExists);
    }


    @Test
    public void testBackupWithRecoveredEntriesOverflow() throws BlobStorageException, IOException {
        final String archiveTestName = "data00005a.tar";
        final String backupArchiveTestName = archiveTestName + ".bak";
        final String extraBackupArchiveTestName = archiveTestName + ".4.bak";

        writeBlobContainerClient.getBlobClient(rootPrefix + "/" + extraBackupArchiveTestName + "/" + segmentName)
                .getBlockBlobClient().getBlobOutputStream().close();

        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create(archiveTestName);

        List<UUID> uuids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            UUID u = UUID.randomUUID();
            writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
            uuids.add(u);
        }

        writer.flush();
        writer.close();

        readBlobContainerClient.getBlobClient(rootPrefix + "/" + archiveTestName + "/0005." + uuids.get(5).toString()).delete();

        LinkedHashMap<UUID, byte[]> recovered = new LinkedHashMap<>();
        manager.recoverEntries(archiveTestName, recovered);

        manager.backup(archiveTestName, archiveTestName + ".bak", recovered.keySet());

        assertFalse("segment from extraBackupArchiveTestName should not be copied to the new backup archive",
                readBlobContainerClient.getBlobClient(rootPrefix + "/" + backupArchiveTestName + "/" + segmentName).exists());
        assertTrue("segment from extraBackupArchiveTestName should not be cleaned",
                readBlobContainerClient.getBlobClient(rootPrefix + "/" + extraBackupArchiveTestName + "/" + segmentName).exists());
    }
}
