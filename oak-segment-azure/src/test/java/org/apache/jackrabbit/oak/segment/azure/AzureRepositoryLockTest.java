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
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.RequestConditions;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.segment.file.MetricsRemoteStoreMonitor;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.channels.UnresolvedAddressException;
import java.security.InvalidKeyException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;

public class AzureRepositoryLockTest {

    private static final Logger log = LoggerFactory.getLogger(AzureRepositoryLockTest.class);
    public static final String LEASE_DURATION = "15";
    public static final String RENEWAL_INTERVAL = "3";
    public static final String TIME_TO_WAIT_BEFORE_BLOCK = "9";
    public static final String LEASE_RENEWAL_TIMEOUT = "1000"; // 1 second for faster tests

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
            .and(AzureRepositoryLock.TIME_TO_WAIT_BEFORE_WRITE_BLOCK_PROP, TIME_TO_WAIT_BEFORE_BLOCK)
            .and(AzureRepositoryLock.LEASE_RENEWAL_TIMEOUT_PROP, LEASE_RENEWAL_TIMEOUT);

    @Test
    public void testFailingLock() throws IOException, BlobStorageException {
        BlockBlobClient blockBlobClient = readBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlockBlobClient noRetrtBlockBlobClient = noRetryBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(noRetrtBlockBlobClient).buildClient();
        AzureRepositoryLock lock = new AzureRepositoryLock(blockBlobClient, blobLeaseClient, () -> {}, new WriteAccessController()).lock();
        try {
            new AzureRepositoryLock(blockBlobClient, blobLeaseClient, () -> {}, new WriteAccessController()).lock();
            fail("The second lock should fail.");
        } catch (IOException e) {
            // it's fine
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void repoLockNotDeletedTest() throws IOException, BlobStorageException {
        BlockBlobClient blockBlobClient = readBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();

        // create repo.lock blob
        blockBlobClient.getBlobOutputStream(true).close();

        BlockBlobClient noRetrtBlockBlobClient = noRetryBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(noRetrtBlockBlobClient).buildClient();

        // no exception should be present when calling lock
        AzureRepositoryLock lock = new AzureRepositoryLock(blockBlobClient, blobLeaseClient, () -> {}, new WriteAccessController());
        try {
            lock.lock();
        } finally {
            lock.unlock();
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
        AzureRepositoryLock lock = new AzureRepositoryLock(blockBlobClient, blobLeaseClient, () -> {}, new WriteAccessController(), 10);
        try {
            lock.lock();
        } finally {
            lock.unlock();
        }
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

        AzureRepositoryLock lock = new AzureRepositoryLock(blobMocked, blobLeaseMocked, () -> {}, new WriteAccessController(), 1);
        try {
            lock.lock();

            // wait till lease expires
            Thread.sleep(3000);

            // reset the mock to default behaviour
            Mockito.doCallRealMethod().when(blobLeaseMocked).renewLeaseWithResponse((RequestConditions) any(), any(), any());

            try {
                new AzureRepositoryLock(blobMocked, blobLeaseMocked, () -> {
                }, new WriteAccessController()).lock();
                fail("The second lock should fail.");
            } catch (IOException e) {
                // it's fine
            }
        } finally {
            lock.unlock();
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

        AzureRepositoryLock lock = new AzureRepositoryLock(blobMocked, blobLeaseMocked, () -> {}, writeAccessController);
        Thread thread = null;
        try {
            lock.lock();

            thread = new Thread(() -> {
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
        } finally {
            lock.unlock();
            if (thread != null) {
                thread.interrupt();
            }
        }
    }

    @Test
    public void testClientSideTimeoutExceptionIsRecoverable() throws Exception {
        // Create a delay policy that delays lease renewal requests for 2 seconds (> 1s timeout set via system property)
        // This will cause real IllegalStateException with TimeoutException from Reactor's Mono.block()
        DelayInjectionPolicy delayPolicy = new DelayInjectionPolicy(
                context -> {
                    String url = context.getHttpRequest().getUrl().toString();
                    String method = context.getHttpRequest().getHttpMethod().toString();
                    // Only delay PUT requests to repo.lock with comp=lease (lease renewal operations)
                    // Skip the first request which is the initial lease acquisition
                    return "PUT".equals(method) && url.contains("repo.lock") && url.contains("comp=lease");
                },
                Duration.ofSeconds(2),  // Delay 2 seconds (timeout is 1 second via system property)
                2                        // Delay first 2 renewal requests, then succeed
        );

        // Create container client with the delay policy injected
        BlobContainerClient containerWithDelay = azurite.getContainerClientWithPolicies("oak-test", null, delayPolicy);
        containerWithDelay.createIfNotExists();

        BlockBlobClient blockBlobClient = containerWithDelay.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlobLeaseClient blobLeaseClient = Mockito.spy(new BlobLeaseClientBuilder().blobClient(blockBlobClient).buildClient());

        // Track if shutdown hook was called
        AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        Runnable shutdownHook = () -> shutdownCalled.set(true);

        WriteAccessController writeAccessController = new WriteAccessController();

        AzureRepositoryLock lock = new AzureRepositoryLock(blockBlobClient, blobLeaseClient, shutdownHook, writeAccessController);
        try {
            lock.lock();

            // Enable delay injection after initial lease acquisition
            delayPolicy.setEnabled(true);

            // Wait for at least 3 renewal calls (2 timeouts + 1 success) with a timeout
            Mockito.verify(blobLeaseClient, Mockito.timeout(15000).atLeast(3))
                    .renewLeaseWithResponse((RequestConditions) any(), any(), any());

            assertTrue("Should have delayed at least 2 requests, but delayed: " + delayPolicy.getDelayedRequestCount(),
                    delayPolicy.getDelayedRequestCount() >= 2);

            assertFalse("Shutdown hook should not be called for client-side timeout exceptions", shutdownCalled.get());
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void testIOExceptionIsRecoverable() throws Exception {
        BlockBlobClient blockBlobClient = readBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlockBlobClient noRetryBlockBlobClient = noRetryBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(noRetryBlockBlobClient).buildClient();

        BlockBlobClient blobMocked = Mockito.spy(blockBlobClient);
        BlobLeaseClient blobLeaseMocked = Mockito.spy(blobLeaseClient);

        // Simulate network error wrapped in UncheckedIOException (as reactor does with IOException)
        java.io.UncheckedIOException networkError = new java.io.UncheckedIOException(
                "Connection reset",
                new java.io.IOException("Connection reset by peer"));

        // Track if shutdown hook was called
        AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        Runnable shutdownHook = () -> shutdownCalled.set(true);

        // Instrument the mock to throw the IO exception twice, then succeed
        Mockito.doThrow(networkError)
                .doThrow(networkError)
                .doCallRealMethod()
                .when(blobLeaseMocked).renewLeaseWithResponse((RequestConditions) any(), any(), any());

        WriteAccessController writeAccessController = new WriteAccessController();

        AzureRepositoryLock lock = new AzureRepositoryLock(blobMocked, blobLeaseMocked, shutdownHook, writeAccessController);
        try {
            lock.lock();

            // Wait for at least 3 calls (2 failures + 1 success) with a timeout
            Mockito.verify(blobLeaseMocked, Mockito.timeout(10000).atLeast(3))
                    .renewLeaseWithResponse((RequestConditions) any(), any(), any());

            // Verify that shutdown hook was NOT called - the IO exception should be treated as recoverable
            assertFalse("Shutdown hook should not be called for IO exceptions", shutdownCalled.get());
        } finally {
            // Clean up: stop the renewal thread and release the lease
            lock.unlock();
        }
    }

    @Test
    public void testUnresolvedAddressExceptionIsRecoverable() throws Exception {
        BlockBlobClient blockBlobClient = readBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlockBlobClient noRetryBlockBlobClient = noRetryBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
        BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(noRetryBlockBlobClient).buildClient();

        BlockBlobClient blobMocked = Mockito.spy(blockBlobClient);
        BlobLeaseClient blobLeaseMocked = Mockito.spy(blobLeaseClient);

        // Simulate DNS resolution failure wrapped in a RuntimeException (as Netty/Reactor does)
        RuntimeException dnsError = new RuntimeException(
                "Connection failed",
                new UnresolvedAddressException());

        // Track if shutdown hook was called
        AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        Runnable shutdownHook = () -> shutdownCalled.set(true);

        // Instrument the mock to throw the UnresolvedAddressException twice, then succeed
        Mockito.doThrow(dnsError)
                .doThrow(dnsError)
                .doCallRealMethod()
                .when(blobLeaseMocked).renewLeaseWithResponse((RequestConditions) any(), any(), any());

        WriteAccessController writeAccessController = new WriteAccessController();

        AzureRepositoryLock lock = new AzureRepositoryLock(blobMocked, blobLeaseMocked, shutdownHook, writeAccessController);
        try {
            lock.lock();

            // Wait for at least 3 calls (2 failures + 1 success) with a timeout
            Mockito.verify(blobLeaseMocked, Mockito.timeout(10000).atLeast(3))
                    .renewLeaseWithResponse((RequestConditions) any(), any(), any());

            // Verify that shutdown hook was NOT called - the UnresolvedAddressException should be treated as recoverable
            assertFalse("Shutdown hook should not be called for UnresolvedAddressException", shutdownCalled.get());
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void testRepositoryLockLostMetricOnNonTransientError() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        DefaultStatisticsProvider statisticsProvider = new DefaultStatisticsProvider(executor);
        try (ExecutorCloser ignored = new ExecutorCloser(executor)) {
            MetricsRemoteStoreMonitor monitor = new MetricsRemoteStoreMonitor(statisticsProvider);
            CounterStats lockLostCounter = statisticsProvider.getCounterStats(
                    MetricsRemoteStoreMonitor.REPOSITORY_LOCK_LOST, StatsOptions.DEFAULT);

            BlockBlobClient blockBlobClient = readBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
            BlockBlobClient noRetryBlockBlobClient = noRetryBlobContainerClient.getBlobClient("oak/repo.lock").getBlockBlobClient();
            BlobLeaseClient blobLeaseClient = new BlobLeaseClientBuilder().blobClient(noRetryBlockBlobClient).buildClient();

            BlobLeaseClient blobLeaseMocked = Mockito.spy(blobLeaseClient);

            // Throw a non-transient, non-BlobStorageException error on renewal to trigger the shutdown hook
            Mockito.doThrow(new SecurityException("simulated non-transient error"))
                    .when(blobLeaseMocked).renewLeaseWithResponse((RequestConditions) any(), any(), any());

            assertEquals("Counter should be zero before test", 0, lockLostCounter.getCount());

            AzureRepositoryLock lock = new AzureRepositoryLock(blockBlobClient, blobLeaseMocked,
                    monitor::repositoryLockLost, new WriteAccessController());

            lock.lock();

            await().atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertEquals(
                            "REPOSITORY_LOCK_LOST counter should be incremented after non-transient error",
                            1, lockLostCounter.getCount()));
        }
    }

    /**
     * HTTP pipeline policy that injects delays into specific requests.
     * Used to cause real client-side timeouts in tests.
     */
    private static class DelayInjectionPolicy implements HttpPipelinePolicy {

        private final Predicate<HttpPipelineCallContext> shouldDelay;
        private final Duration delay;
        private final AtomicInteger delayCount;
        private final int maxDelays;
        private final AtomicBoolean enabled;

        /**
         * Creates a delay injection policy.
         *
         * @param shouldDelay Predicate to determine if this request should be delayed
         * @param delay       How long to delay (should exceed the client timeout)
         * @param maxDelays   How many requests to delay before returning to normal
         */
        DelayInjectionPolicy(Predicate<HttpPipelineCallContext> shouldDelay,
                             Duration delay,
                             int maxDelays) {
            this.shouldDelay = shouldDelay;
            this.delay = delay;
            this.maxDelays = maxDelays;
            this.delayCount = new AtomicInteger(0);
            this.enabled = new AtomicBoolean(false);
        }

        @Override
        public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next) {
            // Check if delay injection is enabled and this request should be delayed
            if (enabled.get() && shouldDelay.test(context) && delayCount.get() < maxDelays) {
                delayCount.incrementAndGet();
                log.info("Injecting {}ms delay for request: {} {}",
                        delay.toMillis(),
                        context.getHttpRequest().getHttpMethod(),
                        context.getHttpRequest().getUrl());
                // Delay BEFORE calling next.process() - this will cause the client timeout
                return Mono.delay(delay).then(next.process());
            }
            return next.process();
        }

        int getDelayedRequestCount() {
            return delayCount.get();
        }

        void setEnabled(boolean enabled) {
            this.enabled.set(enabled);
        }
    }
}
