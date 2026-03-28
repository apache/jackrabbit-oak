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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.function.IntSupplier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Compatibility tests for {@link ElasticIndexStatistics}.
 * These assertions intentionally avoid third-party cache types so the same
 * tests can run across cache implementation changes.
 */
public class ElasticIndexStatisticsCompatibilityTest {

    @Mock
    private ElasticConnection elasticConnectionMock;

    @Mock
    private ElasticIndexDefinition indexDefinitionMock;

    @Mock
    private ElasticsearchClient elasticClientMock;

    private AutoCloseable closeable;

    @Before
    public void setUp() {
        this.closeable = MockitoAnnotations.openMocks(this);
        Mockito.when(indexDefinitionMock.getIndexAlias()).thenReturn("test-index");
        Mockito.when(elasticConnectionMock.getClient()).thenReturn(elasticClientMock);
    }

    @After
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void numDocsReturnsMockedCountFromElasticsearch() throws Exception {
        // Baseline behavior: a cache miss should load the document count from Elasticsearch.
        CountResponse countResponse = Mockito.mock(CountResponse.class);
        Mockito.when(countResponse.count()).thenReturn(42L);
        Mockito.when(elasticClientMock.count(ArgumentMatchers.any(CountRequest.class)))
                .thenReturn(countResponse);

        ElasticIndexStatistics indexStatistics =
                new ElasticIndexStatistics(elasticConnectionMock, indexDefinitionMock);
        Assert.assertEquals(42, indexStatistics.numDocs());
    }

    @Test
    public void numDocsCachesResultOnSubsequentCalls() throws Exception {
        // Call numDocs() twice with the same descriptor and verify only the first
        // call reaches Elasticsearch.
        CountResponse countResponse = Mockito.mock(CountResponse.class);
        Mockito.when(countResponse.count()).thenReturn(99L);
        Mockito.when(elasticClientMock.count(ArgumentMatchers.any(CountRequest.class)))
                .thenReturn(countResponse);

        ElasticIndexStatistics indexStatistics =
                new ElasticIndexStatistics(elasticConnectionMock, indexDefinitionMock);
        // first call loads from ES
        Assert.assertEquals(99, indexStatistics.numDocs());
        // second call should be served from cache (same value)
        Assert.assertEquals(99, indexStatistics.numDocs());
        // ES should only have been called once
        Mockito.verify(elasticClientMock, Mockito.times(1)).count(ArgumentMatchers.any(CountRequest.class));
    }

    @Test
    public void numDocsPropagatesIOExceptionAsRuntimeFailure() throws Exception {
        // Use a checked IOException from the client and assert callers still see a
        // runtime failure that preserves the original cause chain.
        Mockito.when(elasticClientMock.count(ArgumentMatchers.any(CountRequest.class)))
                .thenThrow(new IOException("ES down"));

        ElasticIndexStatistics indexStatistics =
                new ElasticIndexStatistics(elasticConnectionMock, indexDefinitionMock);
        try {
            indexStatistics.numDocs();
            Assert.fail("expected RuntimeException when Elasticsearch is unavailable");
        } catch (RuntimeException e) {
            // The exact wrapper type is intentionally not asserted so this test
            // can remain valid across cache implementations.
            Assert.assertNotNull(findCause(e, IOException.class));
            Assert.assertEquals("ES down", findCause(e, IOException.class).getMessage());
        }
    }

    @Test
    public void getDocCountForFieldReturnsMockedCount() throws Exception {
        CountResponse countResponse = Mockito.mock(CountResponse.class);
        Mockito.when(countResponse.count()).thenReturn(10L);
        Mockito.when(elasticClientMock.count(ArgumentMatchers.any(CountRequest.class)))
                .thenReturn(countResponse);

        ElasticIndexStatistics indexStatistics =
                new ElasticIndexStatistics(elasticConnectionMock, indexDefinitionMock);
        Assert.assertEquals(10, indexStatistics.getDocCountFor("someField"));
    }

    @Test
    public void numDocsAndGetDocCountForUseIndependentCacheKeys() throws Exception {
        // numDocs() and getDocCountFor(field) should not alias each other in the cache,
        // so both lookups must hit Elasticsearch once.
        CountResponse countResponse = Mockito.mock(CountResponse.class);
        Mockito.when(countResponse.count()).thenReturn(5L);
        Mockito.when(elasticClientMock.count(ArgumentMatchers.any(CountRequest.class)))
                .thenReturn(countResponse);

        ElasticIndexStatistics indexStatistics =
                new ElasticIndexStatistics(elasticConnectionMock, indexDefinitionMock);
        indexStatistics.numDocs();
        indexStatistics.getDocCountFor("someField");
        // numDocs and getDocCountFor use different cache keys (different StatsRequestDescriptors)
        Mockito.verify(elasticClientMock, Mockito.times(2)).count(ArgumentMatchers.any(CountRequest.class));
    }

    @Test
    public void numDocsRefreshesValueAfterRefreshWindow() throws Exception {
        // Advance a controllable clock past the refresh boundary, then release
        // the blocked refresh and verify callers eventually observe the new value.
        MutableClock clock = new MutableClock();
        CountResponse initialResponse = Mockito.mock(CountResponse.class);
        CountResponse refreshedResponse = Mockito.mock(CountResponse.class);
        Mockito.when(initialResponse.count()).thenReturn(100L);
        Mockito.when(refreshedResponse.count()).thenReturn(1000L);
        CountDownLatch refreshStarted = new CountDownLatch(1);
        CountDownLatch releaseRefresh = new CountDownLatch(1);
        CountDownLatch refreshCompleted = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        Mockito.when(elasticClientMock.count(ArgumentMatchers.any(CountRequest.class)))
                .thenAnswer(invocation -> {
                    if (invocations.getAndIncrement() == 0) {
                        return initialResponse;
                    }
                    refreshStarted.countDown();
                    if (!releaseRefresh.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to release refresh");
                    }
                    refreshCompleted.countDown();
                    return refreshedResponse;
                });

        ElasticIndexStatistics indexStatistics =
                newIndexStatistics(clock);

        Assert.assertEquals(100, indexStatistics.numDocs());
        Mockito.verify(elasticClientMock, Mockito.times(1)).count(ArgumentMatchers.any(CountRequest.class));

        clock.advanceSeconds(2);
        Assert.assertEquals(100, indexStatistics.numDocs());

        Assert.assertTrue("expected refresh to start", refreshStarted.await(5, TimeUnit.SECONDS));
        releaseRefresh.countDown();
        Assert.assertTrue("expected refresh completion", refreshCompleted.await(5, TimeUnit.SECONDS));
        assertEventuallyEquals(1000, indexStatistics::numDocs);
        Mockito.verify(elasticClientMock, Mockito.atLeast(2)).count(ArgumentMatchers.any(CountRequest.class));
    }

    @Test
    public void numDocsReturnsStaleValueWhileRefreshIsInFlight() throws Exception {
        // Advance a controllable clock into the refresh window, then block the
        // reload so the read path can prove it returns the stale cached value.
        MutableClock clock = new MutableClock();
        CountResponse initialResponse = Mockito.mock(CountResponse.class);
        CountResponse refreshedResponse = Mockito.mock(CountResponse.class);
        Mockito.when(initialResponse.count()).thenReturn(100L);
        Mockito.when(refreshedResponse.count()).thenReturn(1000L);

        CountDownLatch refreshStarted = new CountDownLatch(1);
        CountDownLatch releaseRefresh = new CountDownLatch(1);
        CountDownLatch refreshCompleted = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        Mockito.when(elasticClientMock.count(ArgumentMatchers.any(CountRequest.class)))
                .thenAnswer(invocation -> {
                    if (invocations.getAndIncrement() == 0) {
                        return initialResponse;
                    }
                    refreshStarted.countDown();
                    if (!releaseRefresh.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to release refresh");
                    }
                    refreshCompleted.countDown();
                    return refreshedResponse;
                });

        ElasticIndexStatistics indexStatistics =
                newIndexStatistics(clock);

        Assert.assertEquals(100, indexStatistics.numDocs());

        clock.advanceSeconds(2);
        Assert.assertEquals(100, indexStatistics.numDocs());
        Assert.assertTrue("expected refresh to start", refreshStarted.await(5, TimeUnit.SECONDS));

        releaseRefresh.countDown();
        Assert.assertTrue("expected refresh completion", refreshCompleted.await(5, TimeUnit.SECONDS));
        assertEventuallyEquals(1000, indexStatistics::numDocs);
    }

    @Test
    public void numDocsKeepsCachedValueWhenRefreshFails() throws Exception {
        // Advance a controllable clock into the refresh window, then make the
        // asynchronous refresh fail and verify the cached value is preserved.
        MutableClock clock = new MutableClock();
        CountResponse initialResponse = Mockito.mock(CountResponse.class);
        Mockito.when(initialResponse.count()).thenReturn(100L);

        CountDownLatch refreshAttempted = new CountDownLatch(1);
        AtomicInteger invocations = new AtomicInteger();
        Mockito.when(elasticClientMock.count(ArgumentMatchers.any(CountRequest.class)))
                .thenAnswer(invocation -> {
                    if (invocations.getAndIncrement() == 0) {
                        return initialResponse;
                    }
                    refreshAttempted.countDown();
                    throw new IOException("refresh failed");
                });

        ElasticIndexStatistics indexStatistics =
                newIndexStatistics(clock);

        Assert.assertEquals(100, indexStatistics.numDocs());

        clock.advanceSeconds(2);
        Assert.assertEquals(100, indexStatistics.numDocs());
        Assert.assertTrue("expected refresh attempt", refreshAttempted.await(5, TimeUnit.SECONDS));
        Assert.assertEquals(100, indexStatistics.numDocs());
    }

    private ElasticIndexStatistics newIndexStatistics(Clock clock) {
        return new ElasticIndexStatistics(
                elasticConnectionMock,
                indexDefinitionMock,
                ElasticIndexStatistics.setupCountCache(100, 30, 1, clock),
                null);
    }

    private static void assertEventuallyEquals(int expected, IntSupplier supplier) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        int actual = supplier.getAsInt();
        while (System.nanoTime() < deadline && actual != expected) {
            TimeUnit.MILLISECONDS.sleep(25);
            actual = supplier.getAsInt();
        }
        Assert.assertEquals(expected, actual);
    }

    private static Throwable findCause(Throwable throwable, Class<? extends Throwable> type) {
        Throwable current = throwable;
        while (current != null) {
            if (type.isInstance(current)) {
                return current;
            }
            current = current.getCause();
        }
        return null;
    }

    private static final class MutableClock extends Clock {
        private final AtomicLong currentMillis = new AtomicLong();

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(currentMillis.get());
        }

        @Override
        public long millis() {
            return currentMillis.get();
        }

        private void advanceSeconds(long seconds) {
            currentMillis.addAndGet(TimeUnit.SECONDS.toMillis(seconds));
        }
    }
}
