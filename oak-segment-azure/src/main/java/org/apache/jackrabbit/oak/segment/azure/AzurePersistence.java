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

import com.azure.core.http.HttpPipeline;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.apache.jackrabbit.oak.segment.azure.compat.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class AzurePersistence implements SegmentNodeStorePersistence {

    private static int RETRY_ATTEMPTS = Integer.getInteger("segment.azure.retry.attempts", 5);

    private static int RETRY_BACKOFF_SECONDS = Integer.getInteger("segment.azure.retry.backoff", 5);

    private static int TIMEOUT_EXECUTION_SECONDS = Integer.getInteger("segment.timeout.execution", 30);

    private static int TIMEOUT_INTERVAL = Integer.getInteger("segment.timeout.interval", 1);

    private static final Logger log = LoggerFactory.getLogger(AzurePersistence.class);

    /**
     * That directory contains the tar directories, journals, repo.lock, etc.
     */
    protected final CloudBlobDirectory segmentstoreDirectory;

    public AzurePersistence(CloudBlobDirectory segmentStoreDirectory) {
        this.segmentstoreDirectory = segmentStoreDirectory;
    }

    public static BlobContainerClient createBlobContainerClient(String connectionString, String accountName, String containerName) {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                .connectionString(connectionString)
                .endpoint(String.format("https://%s.blob.core.windows.net", accountName));

        return configureRequestOptions(builder)
                .buildClient()
                .getBlobContainerClient(containerName);
    }

    public static BlobContainerClient createBlobContainerClient(StorageSharedKeyCredential credential, URI uri, String containerName) {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                .credential(credential)
                .endpoint(String.format("https://%s", uri.getHost()));

        return configureRequestOptions(builder)
                .buildClient()
                .getBlobContainerClient(containerName);
    }

    public static BlobContainerClient createBlobContainerClient(String connectionString, String containerName) {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                .connectionString(connectionString);

        return configureRequestOptions(builder)
                .buildClient()
                .createBlobContainer(containerName);
    }

    @NotNull
    public static BlobServiceClientBuilder configureRequestOptions(BlobServiceClientBuilder builder) {
        Integer tryTimeout = null;

        RetryPolicyType retryPolicyType = RetryPolicyType.FIXED;
        Integer maxTries = null;
        Long retryDelayInMs = null;
        Long maxRetryDelayInMs = null;

        if (RETRY_ATTEMPTS > 0) {
            maxTries = RETRY_ATTEMPTS;
            retryDelayInMs = (long) RETRY_BACKOFF_SECONDS;
            maxRetryDelayInMs = (long) RETRY_BACKOFF_SECONDS;
        }

        if (TIMEOUT_EXECUTION_SECONDS > 0) {
            tryTimeout = TIMEOUT_EXECUTION_SECONDS;
        }

        if (TIMEOUT_INTERVAL > 0) {
            // TODO OAK-8413: there is no API for default request timeout
        }

        return builder
                .addPolicy(new AzureStorageMonitorPolicy())
                .retryOptions(new RequestRetryOptions(
                        retryPolicyType, maxTries, tryTimeout, retryDelayInMs, maxRetryDelayInMs, null
                ));

    }

    @Override
    public SegmentArchiveManager createArchiveManager(boolean mmap, boolean offHeapAccess, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) {
        attachRemoteStoreMonitor(remoteStoreMonitor);
        return new AzureArchiveManager(segmentstoreDirectory, ioMonitor, fileStoreMonitor);
    }

    @Override
    public boolean segmentFilesExist() {
        try {
            return segmentstoreDirectory.listItemsInDirectory()
                    .anyMatch(filename -> filename.endsWith(".tar"));
        } catch (BlobStorageException e) {
            log.error("Can't check if the segment archives exists", e);
            return false;
        }
    }

    @Override
    public JournalFile getJournalFile() {
        return new AzureJournalFile(segmentstoreDirectory, "journal.log");
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
        return new AzureRepositoryLock(getBlockBlob("repo.lock"), () -> {
            log.warn("Lost connection to the Azure. The client will be closed.");
            // TODO close the connection
        }).lock();
    }

    private BlockBlobClient getBlockBlob(String filename) throws IOException {
        try {
            return segmentstoreDirectory.getBlobClient(filename).getBlockBlobClient();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    private AppendBlobClient getAppendBlob(String filename) {
        return segmentstoreDirectory.getBlobClient(filename).getAppendBlobClient();
    }

    private void attachRemoteStoreMonitor(RemoteStoreMonitor remoteStoreMonitor) {
        AzureStorageMonitorPolicy monitorPolicy = findAzureStorageMonitorPolicy();
        if (monitorPolicy != null) {
            monitorPolicy.setMonitor(remoteStoreMonitor);
        }
    }

    @Nullable
    private AzureStorageMonitorPolicy findAzureStorageMonitorPolicy() {
        HttpPipeline httpPipeline = segmentstoreDirectory.getContainerClient().getHttpPipeline();
        for (int i = 0; i < httpPipeline.getPolicyCount(); i++) {
            HttpPipelinePolicy policy = httpPipeline.getPolicy(i);
            if (policy instanceof AzureStorageMonitorPolicy) {
                return (AzureStorageMonitorPolicy) policy;
            }
        }
        return null;
    }

}
