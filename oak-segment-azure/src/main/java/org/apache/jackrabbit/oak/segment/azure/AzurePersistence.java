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

import com.azure.storage.blob.AppendBlobClient;
import com.azure.storage.blob.BlockBlobClient;
import com.azure.storage.blob.models.BlobItem;
import org.apache.jackrabbit.oak.segment.azure.compat.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AzurePersistence implements SegmentNodeStorePersistence {

    private static int RETRY_ATTEMPTS = Integer.getInteger("segment.azure.retry.attempts", 5);

    private static int RETRY_BACKOFF_SECONDS = Integer.getInteger("segment.azure.retry.backoff", 5);

    private static int TIMEOUT_EXECUTION = Integer.getInteger("segment.timeout.execution", 30);

    private static int TIMEOUT_INTERVAL = Integer.getInteger("segment.timeout.interval", 1);

    private static final Logger log = LoggerFactory.getLogger(AzurePersistence.class);

    /**
     * That directory contains the tar directories, journals, repo.lock, etc.
     */
    protected final CloudBlobDirectory segmentstoreDirectory;

    public AzurePersistence(CloudBlobDirectory segmentStoreDirectory) {
        this.segmentstoreDirectory = segmentStoreDirectory;

        // TODO OAK-8413: add retry handling
//        BlobRequestOptions defaultRequestOptions = segmentStoreDirectory.getServiceClient().getDefaultRequestOptions();
//        if (defaultRequestOptions.getRetryPolicyFactory() == null) {
//            if (RETRY_ATTEMPTS > 0) {
//                defaultRequestOptions.setRetryPolicyFactory(new RetryLinearRetry((int) TimeUnit.SECONDS.toMillis(RETRY_BACKOFF_SECONDS), RETRY_ATTEMPTS));
//            }
//        }
//        if (defaultRequestOptions.getMaximumExecutionTimeInMs() == null) {
//            if (TIMEOUT_EXECUTION > 0) {
//                defaultRequestOptions.setMaximumExecutionTimeInMs((int) TimeUnit.SECONDS.toMillis(TIMEOUT_EXECUTION));
//            }
//        }
//        if (defaultRequestOptions.getTimeoutIntervalInMs() == null) {
//            if (TIMEOUT_INTERVAL > 0) {
//                defaultRequestOptions.setTimeoutIntervalInMs((int) TimeUnit.SECONDS.toMillis(TIMEOUT_INTERVAL));
//            }
//        }
    }

    @Override
    public SegmentArchiveManager createArchiveManager(boolean mmap, boolean offHeapAccess, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) {
        attachRemoteStoreMonitor(remoteStoreMonitor);
        return new AzureArchiveManager(segmentstoreDirectory, ioMonitor, fileStoreMonitor);
    }

    @Override
    public boolean segmentFilesExist() {
        for (BlobItem blobItem : segmentstoreDirectory.listBlobsFlat()) {
            if (blobItem.name().endsWith(".tar")) {
                return true;
            }
        }
        return false;
    }

    @Override
    public JournalFile getJournalFile() {
        return new AzureJournalFile(segmentstoreDirectory, "journal.log");
    }

    @Override
    public GCJournalFile getGCJournalFile() {
        return new AzureGCJournalFile(getAppendBlob("gc.log"));
    }

    @Override
    public ManifestFile getManifestFile() {
        return new AzureManifestFile(getBlockBlob("manifest"));
    }

    @Override
    public RepositoryLock lockRepository() throws IOException {
        return new AzureRepositoryLock(getBlockBlob("repo.lock"), () -> {
            log.warn("Lost connection to the Azure. The client will be closed.");
            // TODO close the connection
        }).lock();
    }

    private BlockBlobClient getBlockBlob(String path) {
        return segmentstoreDirectory.getBlobClient(path).asBlockBlobClient();
    }

    private AppendBlobClient getAppendBlob(String path) {
        return segmentstoreDirectory.getBlobClient(path).asAppendBlobClient();
    }

    // TODO OAK-8413: verify
    private void attachRemoteStoreMonitor(RemoteStoreMonitor remoteStoreMonitor) {
        segmentstoreDirectory.setRemoteStoreMonitor(remoteStoreMonitor);
    }

}
