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

import com.google.common.collect.ImmutableList;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence.LockStatus;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence.LockStatus.ACQUIRED;
import static org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence.LockStatus.ACQUIRE_FAILED;
import static org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence.LockStatus.LOST;
import static org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence.LockStatus.RENEWAL;
import static org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence.LockStatus.RENEWAL_FAILED;
import static org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence.LockStatus.RENEWAL_SUCCEEDED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;

public class AzureRepositoryLockTest {

    private static final Logger log = LoggerFactory.getLogger(AzureRepositoryLockTest.class);

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private CloudBlobContainer container;

    @Before
    public void setup() throws StorageException, InvalidKeyException, URISyntaxException {
        container = azurite.getContainer("oak-test");
    }

    @Test
    public void testFailingLock() throws URISyntaxException, IOException, StorageException {
        CloudBlockBlob blob = container.getBlockBlobReference("oak/repo.lock");
        AtomicReference<LockStatus> status = new AtomicReference<>();
        new AzureRepositoryLock(blob, status::set, 0, 0).lock();
        assertEquals(ACQUIRED, status.get());
        assertThrows("The second lock should fail", IOException.class, () -> new AzureRepositoryLock(blob, status::set, 0, 0).lock());
        assertEquals(ACQUIRE_FAILED, status.get());
    }

    @Test
    public void testWaitingLock() throws URISyntaxException, IOException, StorageException, InterruptedException {
        CloudBlockBlob blob = container.getBlockBlobReference("oak/repo.lock");
        List<LockStatus> statusHistory1 = new ArrayList<>();
        List<LockStatus> statusHistory2 = new ArrayList<>();
        Semaphore s = new Semaphore(0);
        new Thread(() -> {
            try {
                RepositoryLock lock = new AzureRepositoryLock(blob, statusHistory1::add, 10, 10).lock();
                s.release();
                Thread.sleep(1000);
                lock.unlock();
            } catch (Exception e) {
                log.error("Can't lock or unlock the repo", e);
            }
        }).start();

        s.acquire();
        assertEquals(ImmutableList.of(ACQUIRED), statusHistory1);
        new AzureRepositoryLock(blob, statusHistory2::add, 10, 10).lock();
        assertEquals(ImmutableList.of(ACQUIRED, LockStatus.RELEASED), statusHistory1);
        assertEquals(ImmutableList.of(ACQUIRED), statusHistory2);
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
                .when(blobMocked).renewLease(any(), any(), any());

        Queue<LockStatus> statusHistory = new ConcurrentLinkedDeque<>();
        new AzureRepositoryLock(blobMocked, statusHistory::add, 0, 10, 15).lock();

        // wait till lease expires
        Thread.sleep(16000);

        assertEquals("Should fail to renew twice", 2, statusHistory.stream().limit(5).filter(s -> s == RENEWAL_FAILED).count());
        assertTrue("Should succeed to renew after 2 failures",  statusHistory.stream().skip(5).anyMatch(s -> s == RENEWAL_SUCCEEDED));

        // reset the mock to default behaviour
        Mockito.doCallRealMethod().when(blobMocked).renewLease(any(), any(), any());

        assertThrows("The second lock should fail because the first has been renewed", IOException.class, () -> 
            new AzureRepositoryLock(blobMocked, s -> {}, 0, 0).lock());
    }

    @Test
    public void testLeaseLost() throws URISyntaxException, StorageException, IOException, InterruptedException {
        CloudBlockBlob blob = container.getBlockBlobReference("oak/repo.lock");

        CloudBlockBlob blobMocked = Mockito.spy(blob);

        // instrument the mock to throw the exception when renewing the lease
        StorageException storageException =
            new StorageException(
                StorageErrorCodeStrings.LEASE_ID_MISMATCH_WITH_LEASE_OPERATION,
                "The lease ID specified did not match the lease ID for the blob.", null);
        Mockito.doThrow(storageException)
            .when(blobMocked).renewLease(any(), any(), any());

        Queue<LockStatus> statusHistory = new ConcurrentLinkedDeque<>();
        new AzureRepositoryLock(blobMocked, statusHistory::add, 0, 2, 15).lock();

        // wait till lease expires
        Thread.sleep(16000);

        assertEquals("Lease should be lost", ImmutableList.of(ACQUIRED, RENEWAL, LOST), ImmutableList.copyOf(statusHistory));

        // reset the mock to default behaviour
        Mockito.doCallRealMethod().when(blobMocked).renewLease(any(), any(), any());

        AtomicReference<LockStatus> status = new AtomicReference<>();
        new AzureRepositoryLock(blobMocked, status::set, 0, 0).lock();
        assertEquals("The second lock should succeed because the first has been lost", ACQUIRED, status.get());
    }

}
