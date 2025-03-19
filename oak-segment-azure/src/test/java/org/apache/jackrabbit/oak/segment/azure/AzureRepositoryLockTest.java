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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.RequestConditions;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
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
import static org.mockito.ArgumentMatchers.*;

public class AzureRepositoryLockTest {

    private static final Logger log = LoggerFactory.getLogger(AzureRepositoryLockTest.class);
    public static final String LEASE_DURATION = "15";
    public static final String RENEWAL_INTERVAL = "3";
    public static final String TIME_TO_WAIT_BEFORE_BLOCK = "9";

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private BlobContainerClient noRetryBlobContainerClient;
    private BlobContainerClient readBlobContainerClient;

    @Before
    public void setup() throws BlobStorageException, InvalidKeyException, URISyntaxException {
        noRetryBlobContainerClient = azurite.getNoRetryBlobContainerClient("oak-test");
        readBlobContainerClient = azurite.getReadBlobContainerClient("oak-test");
    }

    @Rule
    public final ProvideSystemProperty systemPropertyRule = new ProvideSystemProperty(AzureRepositoryLock.LEASE_DURATION_PROP, LEASE_DURATION)
            .and(AzureRepositoryLock.RENEWAL_INTERVAL_PROP, RENEWAL_INTERVAL)
            .and(AzureRepositoryLock.TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, TIME_TO_WAIT_BEFORE_BLOCK);

    @Test
    public void testFailingLock() throws IOException, BlobStorageException {
        BlockBlobClient blockBlobClient = readBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlockBlobClient noRetrtBlockBlobClient = noRetryBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(noRetrtBlockBlobClient).buildClient();
        new AzureRepositoryLock(blockBlobClient, blobLeaseClient, () -> {}, new WriteAccessController()).lock();
        try {
            new AzureRepositoryLock(blockBlobClient, blobLeaseClient, () -> {}, new WriteAccessController()).lock();
            fail("The second lock should fail.");
        } catch (IOException e) {
            // it's fine
        }
    }

    @Test
    public void testWaitingLock() throws BlobStorageException, InterruptedException, IOException {
        BlockBlobClient blockBlobClient = readBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlockBlobClient noRetrtBlockBlobClient = noRetryBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(noRetrtBlockBlobClient).buildClient();
        Semaphore s = new Semaphore(0);
        new Thread(() -> {
            try {
                RepositoryLock lock = new AzureRepositoryLock(blockBlobClient, blobLeaseClient, () -> {}, new WriteAccessController()).lock();
                s.release();
                Thread.sleep(1000);
                lock.unlock();
            } catch (Exception e) {
                log.error("Can't lock or unlock the repo", e);
            }
        }).start();

        s.acquire();
        new AzureRepositoryLock(blockBlobClient, blobLeaseClient, () -> {}, new WriteAccessController(), 10).lock();
    }

    @Test
    public void testLeaseRefreshUnsuccessful() throws BlobStorageException, IOException, InterruptedException {
        BlockBlobClient blockBlobClient = readBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlockBlobClient noRetryBlockBlobClient = noRetryBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(noRetryBlockBlobClient).buildClient();

        BlockBlobClient blobMocked = Mockito.spy(blockBlobClient);
        BlobLeaseClient blobLeaseMocked = Mockito.spy(blobLeaseClient);

        // instrument the mock to throw the exception twice when renewing the lease
        BlobStorageException storageException =
                new BlobStorageException("operation timeout", null, new TimeoutException());
        Mockito.doThrow(storageException)
                .doThrow(storageException)
                .doCallRealMethod()
                .when(blobLeaseMocked).renewLeaseWithResponse((RequestConditions) any(), any(), any());

        new AzureRepositoryLock(blobMocked, blobLeaseMocked, () -> {}, new WriteAccessController()).lock();

        // wait till lease expires
        Thread.sleep(16000);

        // reset the mock to default behaviour
        Mockito.doCallRealMethod().when(blobLeaseMocked).renewLeaseWithResponse((RequestConditions) any(), any(), any());

        try {
            new AzureRepositoryLock(blobMocked, blobLeaseMocked, () -> {}, new WriteAccessController()).lock();
            fail("The second lock should fail.");
        } catch (IOException e) {
            // it's fine
        }
    }

    @Test
    public void testWritesBlockedOnlyAfterFewUnsuccessfulAttempts() throws Exception {
        BlockBlobClient blockBlobClient = readBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlockBlobClient noRetrtBlockBlobClient = noRetryBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(noRetrtBlockBlobClient).buildClient();

        BlockBlobClient blobMocked = Mockito.spy(blockBlobClient);
        BlobLeaseClient blobLeaseMocked = Mockito.spy(blobLeaseClient);

        // instrument the mock to throw the exception twice when renewing the lease
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaderName.fromString("x-ms-error-code"), BlobErrorCode.OPERATION_TIMED_OUT.toString());

        MockAzureHttpResponse mockAzureHttpResponse = new MockAzureHttpResponse(306, "operation timeout");
        mockAzureHttpResponse.setHeaders(headers);

        BlobStorageException storageException =
                new BlobStorageException("operation timeout", mockAzureHttpResponse, new TimeoutException());

        Mockito
                .doCallRealMethod()
                .doThrow(storageException)
                .when(blobLeaseMocked).renewLeaseWithResponse((RequestConditions) isNull(), Mockito.any(), Mockito.any());


        WriteAccessController writeAccessController = new WriteAccessController();

        new AzureRepositoryLock(blobMocked, blobLeaseMocked, () -> {}, writeAccessController).lock();


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

        Mockito.doCallRealMethod().when(blobLeaseMocked).renewLeaseWithResponse((RequestConditions) any(), any(), any());
    }
}
