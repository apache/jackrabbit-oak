/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.azure.v8;

import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class AzureRepositoryLockV8Test {

    private static final Logger log = LoggerFactory.getLogger(AzureRepositoryLockV8Test.class);
    public static final String LEASE_DURATION = "15";
    public static final String RENEWAL_INTERVAL = "3";
    public static final String TIME_TO_WAIT_BEFORE_BLOCK = "9";

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        container = azurite.getContainer("oak-test");
    }

    @Rule
    public final ProvideSystemProperty systemPropertyRule = new ProvideSystemProperty(AzureRepositoryLockV8.LEASE_DURATION_PROP, LEASE_DURATION)
            .and(AzureRepositoryLockV8.RENEWAL_INTERVAL_PROP, RENEWAL_INTERVAL)
            .and(AzureRepositoryLockV8.TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, TIME_TO_WAIT_BEFORE_BLOCK);

    @Test
    public void testFailingLock() throws URISyntaxException, IOException, StorageException {
        CloudBlockBlob blob = container.getBlockBlobReference("oak/repo.lock");
        new AzureRepositoryLockV8(blob, () -> {}, new WriteAccessController()).lock();
        try {
            new AzureRepositoryLockV8(blob, () -> {}, new WriteAccessController()).lock();
            fail("The second lock should fail.");
        } catch (IOException e) {
            // it's fine
        }
    }

    @Test
    public void testWaitingLock() throws URISyntaxException, IOException, StorageException, InterruptedException {
        CloudBlockBlob blob = container.getBlockBlobReference("oak/repo.lock");
        Semaphore s = new Semaphore(0);
        new Thread(() -> {
            try {
                RepositoryLock lock = new AzureRepositoryLockV8(blob, () -> {}, new WriteAccessController()).lock();
                s.release();
                Thread.sleep(1000);
                lock.unlock();
            } catch (Exception e) {
                log.error("Can't lock or unlock the repo", e);
            }
        }).start();

        s.acquire();
        new AzureRepositoryLockV8(blob, () -> {}, new WriteAccessController(), 10).lock();
    }

    @Test
    public void testLeaseRefreshUnsuccessful() throws URISyntaxException, StorageException, IOException, InterruptedException {
        CloudBlockBlob blob = container.getBlockBlobReference("oak/repo.lock");

        CloudBlockBlob blobMocked = Mockito.spy(blob);

        // instrument the mock to throw the exception twice when renewing the lease
        StorageException storageException =
                new StorageException(StorageErrorCodeStrings.OPERATION_TIMED_OUT, "operation timeout", new TimeoutException());
        Mockito.doThrow(storageException)
                .doThrow(storageException)
                .doCallRealMethod()
                .when(blobMocked).renewLease(Mockito.any(), Mockito.any(), Mockito.any());

        new AzureRepositoryLockV8(blobMocked, () -> {}, new WriteAccessController()).lock();

        // wait till lease expires
        Thread.sleep(16000);

        // reset the mock to default behaviour
        Mockito.doCallRealMethod().when(blobMocked).renewLease(Mockito.any(), Mockito.any(), Mockito.any());

        try {
            new AzureRepositoryLockV8(blobMocked, () -> {}, new WriteAccessController()).lock();
            fail("The second lock should fail.");
        } catch (IOException e) {
            // it's fine
        }
    }

    @Test
    public void testWritesBlockedOnlyAfterFewUnsuccessfulAttempts() throws Exception {

        CloudBlockBlob blob = container.getBlockBlobReference("oak/repo.lock");

        CloudBlockBlob blobMocked = Mockito.spy(blob);

        // instrument the mock to throw the exception twice when renewing the lease
        StorageException storageException =
                new StorageException(StorageErrorCodeStrings.OPERATION_TIMED_OUT, "operation timeout", new TimeoutException());
        Mockito
                .doCallRealMethod()
                .doThrow(storageException)
                .when(blobMocked).renewLease(Mockito.any(), Mockito.any(), Mockito.any());


        WriteAccessController writeAccessController = new WriteAccessController();

        new AzureRepositoryLockV8(blobMocked, () -> {}, writeAccessController).lock();


        Thread thread = new Thread(() -> {

            while (true) {
                writeAccessController.checkWritingAllowed();

            }
        });

        thread.start();

        Thread.sleep(3000);
        assertFalse("after 3 seconds thread should not be in a waiting state", thread.getState().equals(Thread.State.WAITING));

        Thread.sleep(3000);
        assertFalse("after 6 seconds thread should not be in a waiting state", thread.getState().equals(Thread.State.WAITING));

        Thread.sleep(5000);
        assertTrue("after more than 9 seconds thread should be in a waiting state", thread.getState().equals(Thread.State.WAITING));

        Mockito.doCallRealMethod().when(blobMocked).renewLease(Mockito.any(), Mockito.any(), Mockito.any());
    }
}
