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
import com.microsoft.azure.storage.RetryNoRetry;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AzureRepositoryLock implements RepositoryLock {

    private static final Logger log = LoggerFactory.getLogger(AzureRepositoryLock.class);

    private static final int TIMEOUT_SEC = Integer.getInteger("oak.segment.azure.lock.timeout", 0);
    private static final Integer LEASE_RENEWAL_TIMEOUT_MS = 5000;

    public static final String INTERVAL_PROP = "oak.segment.azure.lock.leaseDuration";
    private static int INTERVAL = Integer.getInteger(INTERVAL_PROP, 60);

    public static final String RENEWAL_FREQUENCY_PROP = "oak.segment.azure.lock.leaseRenewalFrequency";
    private static int RENEWAL_FREQUENCY = Integer.getInteger(RENEWAL_FREQUENCY_PROP, 5);

    public static final String TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP = "oak.segment.azure.lock.blockWritesAfter";
    private static int TIME_TO_WAIT_BEFORE_WRITE_BLOCK = Integer.getInteger(TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, 20);

    private final Runnable shutdownHook;

    private final CloudBlockBlob blob;

    private final ExecutorService executor;

    private final int timeoutSec;

    private WriteAccessController writeAccessController;

    private String leaseId;

    private volatile boolean doUpdate;

    public AzureRepositoryLock(CloudBlockBlob blob, Runnable shutdownHook, WriteAccessController writeAccessController) {
        this(blob, shutdownHook, writeAccessController, TIMEOUT_SEC);
    }

    public AzureRepositoryLock(CloudBlockBlob blob, Runnable shutdownHook, WriteAccessController writeAccessController, int timeoutSec) {
        this.shutdownHook = shutdownHook;
        this.blob = blob;
        this.executor = Executors.newSingleThreadExecutor();
        this.timeoutSec = timeoutSec;
        this.writeAccessController = writeAccessController;

        if (INTERVAL < TIME_TO_WAIT_BEFORE_WRITE_BLOCK || TIME_TO_WAIT_BEFORE_WRITE_BLOCK < RENEWAL_FREQUENCY) {
            throw new IllegalStateException(String.format("The value of %s must be greater than %s and the value of %s must be greater than %s",
                    INTERVAL_PROP, TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, RENEWAL_FREQUENCY_PROP));
        }
    }

    public AzureRepositoryLock lock() throws IOException {
        long start = System.currentTimeMillis();
        Exception ex = null;
        do {
            try {
                blob.openOutputStream().close();
                leaseId = blob.acquireLease(INTERVAL, null);
                writeAccessController.enableWriting();
                log.info("Acquired lease {}", leaseId);
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
        long lastUpdate = 0;
        while (doUpdate) {
            long timeSinceLastUpdate = (System.currentTimeMillis() - lastUpdate) / 1000;
            try {
                if (timeSinceLastUpdate > RENEWAL_FREQUENCY) {

                    BlobRequestOptions requestOptions = new BlobRequestOptions();
                    requestOptions.setMaximumExecutionTimeInMs(LEASE_RENEWAL_TIMEOUT_MS);
                    requestOptions.setRetryPolicyFactory(new RetryNoRetry());
                    blob.renewLease(AccessCondition.generateLeaseCondition(leaseId), requestOptions, null);

                    writeAccessController.enableWriting();
                    lastUpdate = System.currentTimeMillis();
                }
            } catch (StorageException e) {
                timeSinceLastUpdate = (System.currentTimeMillis() - lastUpdate) / 1000;

                if (e.getErrorCode().equals(StorageErrorCodeStrings.OPERATION_TIMED_OUT)) {
                    if (timeSinceLastUpdate > TIME_TO_WAIT_BEFORE_WRITE_BLOCK) {
                        writeAccessController.disableWriting();
                    }
                    log.warn("Could not renew the lease due to the operation timeout. Retry in progress ...", e);
                } else {
                    log.error("Can't renew the lease", e);
                    shutdownHook.run();
                    doUpdate = false;
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

    private void releaseLease() throws IOException {
        try {
            blob.releaseLease(AccessCondition.generateLeaseCondition(leaseId));
            blob.delete();
            log.info("Released lease {}", leaseId);
            leaseId = null;
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }
}
