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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

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
        System.clearProperty("oak.elastic.statsExpireSeconds");
        System.clearProperty("oak.elastic.statsRefreshSeconds");
        closeable.close();
    }

    @Test
    public void numDocsReturnsMockedCountFromElasticsearch() throws Exception {
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
        System.setProperty("oak.elastic.statsExpireSeconds", "30");
        System.setProperty("oak.elastic.statsRefreshSeconds", "1");

        CountResponse countResponse = Mockito.mock(CountResponse.class);
        Mockito.when(countResponse.count()).thenReturn(100L);
        Mockito.when(elasticClientMock.count(ArgumentMatchers.any(CountRequest.class)))
                .thenReturn(countResponse);

        ElasticIndexStatistics indexStatistics =
                new ElasticIndexStatistics(elasticConnectionMock, indexDefinitionMock);

        Assert.assertEquals(100, indexStatistics.numDocs());
        Mockito.verify(elasticClientMock, Mockito.times(1)).count(ArgumentMatchers.any(CountRequest.class));

        Mockito.when(countResponse.count()).thenReturn(1000L);

        TimeUnit.MILLISECONDS.sleep(1200);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        int refreshedValue = indexStatistics.numDocs();
        while (System.nanoTime() < deadline) {
            refreshedValue = indexStatistics.numDocs();
            if (refreshedValue == 1000) {
                break;
            }
            TimeUnit.MILLISECONDS.sleep(50);
        }

        Assert.assertEquals(1000, refreshedValue);
        Mockito.verify(elasticClientMock, Mockito.atLeast(2)).count(ArgumentMatchers.any(CountRequest.class));
    }

    @Test
    public void numDocsReturnsStaleValueWhileRefreshIsInFlight() throws Exception {
        System.setProperty("oak.elastic.statsExpireSeconds", "30");
        System.setProperty("oak.elastic.statsRefreshSeconds", "1");

        CountResponse initialResponse = Mockito.mock(CountResponse.class);
        CountResponse refreshedResponse = Mockito.mock(CountResponse.class);
        Mockito.when(initialResponse.count()).thenReturn(100L);
        Mockito.when(refreshedResponse.count()).thenReturn(1000L);

        CountDownLatch refreshStarted = new CountDownLatch(1);
        CountDownLatch releaseRefresh = new CountDownLatch(1);
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
                    return refreshedResponse;
                });

        ElasticIndexStatistics indexStatistics =
                new ElasticIndexStatistics(elasticConnectionMock, indexDefinitionMock);

        Assert.assertEquals(100, indexStatistics.numDocs());

        TimeUnit.MILLISECONDS.sleep(1200);
        Assert.assertEquals(100, indexStatistics.numDocs());
        Assert.assertTrue("expected refresh to start", refreshStarted.await(5, TimeUnit.SECONDS));

        releaseRefresh.countDown();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        int refreshedValue = indexStatistics.numDocs();
        while (System.nanoTime() < deadline && refreshedValue != 1000) {
            TimeUnit.MILLISECONDS.sleep(50);
            refreshedValue = indexStatistics.numDocs();
        }

        Assert.assertEquals(1000, refreshedValue);
    }

    @Test
    public void numDocsKeepsCachedValueWhenRefreshFails() throws Exception {
        System.setProperty("oak.elastic.statsExpireSeconds", "30");
        System.setProperty("oak.elastic.statsRefreshSeconds", "1");

        CountResponse initialResponse = Mockito.mock(CountResponse.class);
        Mockito.when(initialResponse.count()).thenReturn(100L);

        AtomicInteger invocations = new AtomicInteger();
        Mockito.when(elasticClientMock.count(ArgumentMatchers.any(CountRequest.class)))
                .thenAnswer(invocation -> {
                    if (invocations.getAndIncrement() == 0) {
                        return initialResponse;
                    }
                    throw new IOException("refresh failed");
                });

        ElasticIndexStatistics indexStatistics =
                new ElasticIndexStatistics(elasticConnectionMock, indexDefinitionMock);

        Assert.assertEquals(100, indexStatistics.numDocs());

        TimeUnit.MILLISECONDS.sleep(1200);
        Assert.assertEquals(100, indexStatistics.numDocs());

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline && invocations.get() < 2) {
            TimeUnit.MILLISECONDS.sleep(50);
        }

        Assert.assertTrue("expected refresh attempt", invocations.get() >= 2);
        Assert.assertEquals(100, indexStatistics.numDocs());
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
}
