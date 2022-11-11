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

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.util.function.Consumer;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence.*;

public class AzureRepositoryLock implements RepositoryLock {

    private static final Logger log = LoggerFactory.getLogger(AzureRepositoryLock.class);

    private static final int TIMEOUT_SEC = Integer.getInteger("oak.segment.azure.lock.timeout", 0);

    private static final int INTERVAL = 60;

    private final Consumer<LockStatus> lockStatusChangedCallback;

    private final CloudBlockBlob blob;

    private final ExecutorService executor;

    private final int timeoutSec;

    private final int renewalInterval;
    
    private String leaseId;

    private volatile boolean doUpdate;

    public AzureRepositoryLock(CloudBlockBlob blob, Consumer<LockStatus> lockStatusChangedCallback) {
        this(blob, lockStatusChangedCallback, TIMEOUT_SEC, INTERVAL / 2);
    }

    public AzureRepositoryLock(CloudBlockBlob blob, Consumer<LockStatus> lockStatusChangedCallback, int timeoutSec, int renewalInterval) {
        this.lockStatusChangedCallback = lockStatusChangedCallback;
        this.blob = blob;
        this.executor = Executors.newSingleThreadExecutor();
        this.timeoutSec = timeoutSec;
        this.renewalInterval = renewalInterval;
    }
    
    public AzureRepositoryLock lock() throws IOException {
        long start = System.currentTimeMillis();
        Exception ex = null;
        do {
            try {
                blob.openOutputStream().close();
                leaseId = blob.acquireLease(INTERVAL, null);
                log.info("Acquired lease {}", leaseId);
                notifyLockStatusChange(LockStatus.ACQUIRED);
            } catch (StorageException | IOException e) {
                if (ex == null) {
                    log.info("Can't acquire the lease. Retrying every 1s. Timeout is set to {}s.", timeoutSec);
                }
                ex = e;
                if ((System.currentTimeMillis() - start) / 1000 < timeoutSec) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        throw new IOException(e1);
                    }
                } else {
                    break;
                }
            }
        } while (leaseId == null);
        if (leaseId == null) {
            log.error("Can't acquire the lease in {}s.", timeoutSec);
            throw new IOException(ex);
        } else {
            executor.submit(this::refreshLease);
            return this;
        }
    }

    private void refreshLease() {
        doUpdate = true;
        long lastUpdate = System.currentTimeMillis();
        while (doUpdate) {
            try {
                long timeSinceLastUpdate = (System.currentTimeMillis() - lastUpdate) / 1000;
                if (timeSinceLastUpdate > renewalInterval) {
                    notifyLockStatusChange(LockStatus.RENEWAL);
                    doRenewLease();
                    lastUpdate = System.currentTimeMillis();
                    notifyLockStatusChange(LockStatus.RENEWAL_SUCCEEDED);
                }
            } catch (StorageException e) {
                if (e.getErrorCode().equals(StorageErrorCodeStrings.OPERATION_TIMED_OUT)) {
                    log.warn("Could not renew the lease due to the operation timeout. Retry in progress ...", e);
                    notifyLockStatusChange(LockStatus.RENEWAL_FAILED);
                } else {
                    log.error("Can't renew the lease", e);
                    doUpdate = false;
                    notifyLockStatusChange(LockStatus.LOST);
                    return;
                }
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error("Interrupted the lease renewal loop", e);
            }
        }
    }

    private void notifyLockStatusChange(LockStatus renewal) {
        try {
            lockStatusChangedCallback.accept(renewal);
        } catch (RuntimeException e) {
            log.warn("Exception in lock status change callback", e);
        }
    }

    void doRenewLease() throws StorageException {
        BlobRequestOptions blobRequestOptions = new BlobRequestOptions();
        blobRequestOptions.setMaximumExecutionTimeInMs(Math.max(INTERVAL - renewalInterval - 1, INTERVAL / 2 - 1));
        blobRequestOptions.setTimeoutIntervalInMs(5); //TODO: extract field
        blob.renewLease(AccessCondition.generateLeaseCondition(leaseId), blobRequestOptions, null);
    }

    @Override
    public void unlock() throws IOException {
        doUpdate = false;
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            releaseLease();
        }
    }

    void releaseLease() throws IOException {
        if (leaseId == null) {
            return;
        }
        try {
            blob.releaseLease(AccessCondition.generateLeaseCondition(leaseId));
            blob.delete();
            log.info("Released lease {}", leaseId);
            leaseId = null;
            notifyLockStatusChange(LockStatus.RELEASED);
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }
}
