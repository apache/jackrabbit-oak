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

import com.azure.core.http.RequestConditions;
import com.azure.core.util.Context;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AzureRepositoryLock implements RepositoryLock {

    private static final Logger log = LoggerFactory.getLogger(AzureRepositoryLock.class);

    private static final int TIMEOUT_SEC = Integer.getInteger("oak.segment.azure.lock.timeout", 0);
    private static final Integer LEASE_RENEWAL_TIMEOUT_MS = 5000;

    public static final String LEASE_DURATION_PROP = "oak.segment.azure.lock.leaseDurationInSec";
    private final int leaseDuration = Integer.getInteger(LEASE_DURATION_PROP, 60);

    public static final String RENEWAL_INTERVAL_PROP = "oak.segment.azure.lock.leaseRenewalIntervalInSec";
    private final int renewalInterval = Integer.getInteger(RENEWAL_INTERVAL_PROP, 5);

    public static final String TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP = "oak.segment.azure.lock.blockWritesAfterInSec";
    private final int timeToWaitBeforeWriteBlock = Integer.getInteger(TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, 20);

    private final Runnable shutdownHook;

    private final BlockBlobClient blockBlobClient;

    private final BlobLeaseClient leaseClient;

    private final ExecutorService executor;

    private final int timeoutSec;

    private WriteAccessController writeAccessController;

    private String leaseId;

    private volatile boolean doUpdate;

    public AzureRepositoryLock(BlockBlobClient blockBlobClient, BlobLeaseClient leaseClient, Runnable shutdownHook, WriteAccessController writeAccessController) {
        this(blockBlobClient, leaseClient, shutdownHook, writeAccessController, TIMEOUT_SEC);
    }

    public AzureRepositoryLock(BlockBlobClient blockBlobClient, BlobLeaseClient leaseClient, Runnable shutdownHook, WriteAccessController writeAccessController, int timeoutSec) {
        this.shutdownHook = shutdownHook;
        this.blockBlobClient = blockBlobClient;
        this.leaseClient =  leaseClient;
        this.executor = Executors.newSingleThreadExecutor();
        this.timeoutSec = timeoutSec;
        this.writeAccessController = writeAccessController;

        if (leaseDuration < timeToWaitBeforeWriteBlock || timeToWaitBeforeWriteBlock < renewalInterval) {
            throw new IllegalStateException(String.format("The value of %s must be greater than %s and the value of %s must be greater than %s",
                    LEASE_DURATION_PROP, TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, RENEWAL_INTERVAL_PROP));
        }
    }

    public AzureRepositoryLock lock() throws IOException {
        long start = System.currentTimeMillis();
        Exception ex = null;
        do {
            try {
                blockBlobClient.getBlobOutputStream().close();

                log.info("{} = {}", LEASE_DURATION_PROP, leaseDuration);
                log.info("{} = {}", RENEWAL_INTERVAL_PROP, renewalInterval);
                log.info("{} = {}", TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, timeToWaitBeforeWriteBlock);

                leaseId = leaseClient.acquireLease(leaseDuration);
                writeAccessController.enableWriting();
                log.info("Acquired lease {}", leaseId);
            } catch (Exception e) {
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
                if (timeSinceLastUpdate > renewalInterval) {
                    leaseId = leaseClient.renewLeaseWithResponse((RequestConditions) null, Duration.ofMillis(LEASE_RENEWAL_TIMEOUT_MS), Context.NONE).getValue();

                    writeAccessController.enableWriting();
                    lastUpdate = System.currentTimeMillis();
                }
            } catch (Exception e) {
                timeSinceLastUpdate = (System.currentTimeMillis() - lastUpdate) / 1000;

                if (timeSinceLastUpdate > timeToWaitBeforeWriteBlock) {
                    writeAccessController.disableWriting();
                }

                if (e instanceof BlobStorageException) {
                    BlobStorageException storageException = (BlobStorageException) e;
                    if (Set.of(BlobErrorCode.OPERATION_TIMED_OUT,
                            BlobErrorCode.SERVER_BUSY,
                            BlobErrorCode.INTERNAL_ERROR).contains(storageException.getErrorCode())) {
                        log.warn("Could not renew the lease due to the operation timeout or service unavailability. Retry in progress ...", e);
                    //TODO: ierandra
                    } else if (storageException.getStatusCode() == 306) {
                        log.warn("Client side error. Retry in progress ...", e);
                    } else {
                        log.warn("Could not renew lease due to storage exception. Retry in progress ... ", e);
                    }
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
            leaseClient.releaseLease();
            blockBlobClient.delete();
            log.info("Released lease {}", leaseId);
            leaseId = null;
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

}
