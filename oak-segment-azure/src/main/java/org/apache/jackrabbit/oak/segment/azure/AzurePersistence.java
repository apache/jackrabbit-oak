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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Date;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RequestCompletedEvent;
import com.microsoft.azure.storage.RetryLinearRetry;
import com.microsoft.azure.storage.StorageEvent;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
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

public class AzurePersistence implements SegmentNodeStorePersistence {

    private static final String RETRY_ATTEMPTS_PROP = "segment.azure.retry.attempts";
    private static final int DEFAULT_RETRY_ATTEMPTS = 5;

    private static final String RETRY_BACKOFF_PROP = "segment.azure.retry.backoff";
    private static final int DEFAULT_RETRY_BACKOFF_SECONDS = 5;

    private static final String TIMEOUT_EXECUTION_PROP = "segment.timeout.execution";
    private static final int DEFAULT_TIMEOUT_EXECUTION = 30;

    private static final String TIMEOUT_INTERVAL_PROP = "segment.timeout.interval";
    private static final int DEFAULT_TIMEOUT_INTERVAL = 1;

    private static final Logger log = LoggerFactory.getLogger(AzurePersistence.class);

    protected final CloudBlobDirectory segmentstoreDirectory;

    protected WriteAccessController writeAccessController = new WriteAccessController();

    public AzurePersistence(CloudBlobDirectory segmentStoreDirectory) {
        this.segmentstoreDirectory = segmentStoreDirectory;

        BlobRequestOptions defaultRequestOptions = segmentStoreDirectory.getServiceClient().getDefaultRequestOptions();
        if (defaultRequestOptions.getRetryPolicyFactory() == null) {
            int retryAttempts = Integer.getInteger(RETRY_ATTEMPTS_PROP, DEFAULT_RETRY_ATTEMPTS);
            if (retryAttempts > 0) {
                Integer retryBackoffSeconds = Integer.getInteger(RETRY_BACKOFF_PROP, DEFAULT_RETRY_BACKOFF_SECONDS);
                defaultRequestOptions.setRetryPolicyFactory(new RetryLinearRetry((int) TimeUnit.SECONDS.toMillis(retryBackoffSeconds), retryAttempts));
            }
        }
        if (defaultRequestOptions.getMaximumExecutionTimeInMs() == null) {
            int timeoutExecution = Integer.getInteger(TIMEOUT_EXECUTION_PROP, DEFAULT_TIMEOUT_EXECUTION);
            if (timeoutExecution > 0) {
                defaultRequestOptions.setMaximumExecutionTimeInMs((int) TimeUnit.SECONDS.toMillis(timeoutExecution));
            }
        }
        if (defaultRequestOptions.getTimeoutIntervalInMs() == null) {
            int timeoutInterval = Integer.getInteger(TIMEOUT_INTERVAL_PROP, DEFAULT_TIMEOUT_INTERVAL);
            if (timeoutInterval > 0) {
                defaultRequestOptions.setTimeoutIntervalInMs((int) TimeUnit.SECONDS.toMillis(timeoutInterval));
            }
        }
    }

    @Override
    public SegmentArchiveManager createArchiveManager(boolean mmap, boolean offHeapAccess, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, RemoteStoreMonitor remoteStoreMonitor) {
        attachRemoteStoreMonitor(remoteStoreMonitor);
        return new AzureArchiveManager(segmentstoreDirectory, ioMonitor, fileStoreMonitor, writeAccessController);
    }

    @Override
    public boolean segmentFilesExist() {
        try {
            for (ListBlobItem i : segmentstoreDirectory.listBlobs(null, false, EnumSet.noneOf(BlobListingDetails.class), null, null)) {
                if (i instanceof CloudBlobDirectory) {
                    CloudBlobDirectory dir = (CloudBlobDirectory) i;
                    String name = Paths.get(dir.getPrefix()).getFileName().toString();
                    if (name.endsWith(".tar")) {
                        return true;
                    }
                }
            }
            return false;
        } catch (StorageException | URISyntaxException e) {
            log.error("Can't check if the segment archives exists", e);
            return false;
        }
    }

    @Override
    public JournalFile getJournalFile() {
        return new AzureJournalFile(segmentstoreDirectory, "journal.log", writeAccessController);
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
        }, writeAccessController).lock();
    }

    private CloudBlockBlob getBlockBlob(String path) throws IOException {
        try {
            return segmentstoreDirectory.getBlockBlobReference(path);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

    private CloudAppendBlob getAppendBlob(String path) throws IOException {
        try {
            return segmentstoreDirectory.getAppendBlobReference(path);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

    private static void attachRemoteStoreMonitor(RemoteStoreMonitor remoteStoreMonitor) {
        OperationContext.getGlobalRequestCompletedEventHandler().addListener(new StorageEvent<RequestCompletedEvent>() {

            @Override
            public void eventOccurred(RequestCompletedEvent e) {
                Date startDate = e.getRequestResult().getStartDate();
                Date stopDate = e.getRequestResult().getStopDate();

                if (startDate != null && stopDate != null) {
                    long requestDuration = stopDate.getTime() - startDate.getTime();
                    remoteStoreMonitor.requestDuration(requestDuration, TimeUnit.MILLISECONDS);
                }

                Exception exception = e.getRequestResult().getException();

                if (exception == null) {
                    remoteStoreMonitor.requestCount();
                } else {
                    remoteStoreMonitor.requestError();
                }
            }

        });
    }

    public CloudBlobDirectory getSegmentstoreDirectory() {
        return segmentstoreDirectory;
    }

    public void setWriteAccessController(WriteAccessController writeAccessController) {
        this.writeAccessController = writeAccessController;
    }
}
