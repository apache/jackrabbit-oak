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
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AzurePersistence implements SegmentNodeStorePersistence {
    private static final Logger log = LoggerFactory.getLogger(AzurePersistence.class);

    protected final BlobContainerClient readBlobContainerClient;

    protected final BlobContainerClient writeBlobContainerClient;

    protected final BlobContainerClient noRetryBlobContainerClient;

    protected final String rootPrefix;

    protected AzureHttpRequestLoggingPolicy azureHttpRequestLoggingPolicy;

    protected WriteAccessController writeAccessController = new WriteAccessController();

    public AzurePersistence(BlobContainerClient readBlobContainerClient, BlobContainerClient writeBlobContainerClient, BlobContainerClient noRetryBlobContainerClient, String rootPrefix) {
        this(readBlobContainerClient, writeBlobContainerClient, noRetryBlobContainerClient, rootPrefix, null);
    }

    public AzurePersistence(BlobContainerClient readBlobContainerClient, BlobContainerClient writeBlobContainerClient, BlobContainerClient noRetryBlobContainerClient, String rootPrefix, AzureHttpRequestLoggingPolicy azureHttpRequestLoggingPolicy) {
        this.readBlobContainerClient = readBlobContainerClient;
        this.writeBlobContainerClient = writeBlobContainerClient;
        this.noRetryBlobContainerClient = noRetryBlobContainerClient;
        this.azureHttpRequestLoggingPolicy = azureHttpRequestLoggingPolicy;
        this.rootPrefix = rootPrefix;
    }

    @Override
    public SegmentArchiveManager createArchiveManager(boolean mmap, boolean offHeapAccess, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) {
        attachRemoteStoreMonitor(remoteStoreMonitor);
        return new AzureArchiveManager(readBlobContainerClient, writeBlobContainerClient, rootPrefix, ioMonitor, fileStoreMonitor, writeAccessController);
    }

    @Override
    public boolean segmentFilesExist() {
        try {
            return readBlobContainerClient.listBlobsByHierarchy(rootPrefix + "/").stream()
                    .filter(BlobItem::isPrefix)
                    .anyMatch(blobItem -> blobItem.getName().endsWith(".tar") || blobItem.getName().endsWith(".tar/"));
        } catch (BlobStorageException e) {
            log.error("Can't check if the segment archives exists", e);
            return false;
        }
    }

    @Override
    public JournalFile getJournalFile() {
        return new AzureJournalFile(readBlobContainerClient, writeBlobContainerClient, rootPrefix + "/journal.log", writeAccessController);
    }

    @Override
    public GCJournalFile getGCJournalFile() throws IOException {
        return new AzureGCJournalFile(getAppendBlob("gc.log"));
    }

    @Override
    public ManifestFile getManifestFile() throws IOException {
        return new AzureManifestFile(getBlockBlob("manifest"));
    }

    @Override
    public RepositoryLock lockRepository() throws IOException {
        BlockBlobClient blockBlobClient = getBlockBlob("repo.lock");
        BlockBlobClient noRetryBlockBlobClient = getNoRetryBlockBlob("repo.lock");
        BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(noRetryBlockBlobClient).buildClient();
        return new AzureRepositoryLock(blockBlobClient, blobLeaseClient, () -> {
            log.warn("Lost connection to the Azure. The client will be closed.");
            // TODO close the connection
        }, writeAccessController).lock();
    }

    private BlockBlobClient getBlockBlob(String path) throws IOException {
        try {
            return readBlobContainerClient.getBlobClient(rootPrefix + "/" + path).getBlockBlobClient();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    private BlockBlobClient getNoRetryBlockBlob(String path) throws IOException {
        try {
            return noRetryBlobContainerClient.getBlobClient(rootPrefix + "/" + path).getBlockBlobClient();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    private AppendBlobClient getAppendBlob(String path) throws IOException {
        try {
            return readBlobContainerClient.getBlobClient(rootPrefix + "/" + path).getAppendBlobClient();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    private void attachRemoteStoreMonitor(RemoteStoreMonitor remoteStoreMonitor) {
        if (azureHttpRequestLoggingPolicy != null) {azureHttpRequestLoggingPolicy.setRemoteStoreMonitor(remoteStoreMonitor);}
    }

    public BlobContainerClient getReadBlobContainerClient() {
        return readBlobContainerClient;
    }

    public void setWriteAccessController(WriteAccessController writeAccessController) {
        this.writeAccessController = writeAccessController;
    }
}
