/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
import co.elastic.clients.elasticsearch._types.ShardStatistics;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.cat.ElasticsearchCatClient;
import co.elastic.clients.elasticsearch.cat.IndicesRequest;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.GetIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.GetIndicesSettingsResponse;
import co.elastic.clients.elasticsearch.indices.IndexState;
import co.elastic.clients.elasticsearch.indices.IndicesStatsRequest;
import co.elastic.clients.elasticsearch.indices.IndicesStatsResponse;
import co.elastic.clients.elasticsearch.indices.stats.IndicesStats;
import co.elastic.clients.util.ObjectBuilder;
import org.apache.jackrabbit.oak.cache.api.LoadingCache;
import org.apache.jackrabbit.oak.plugins.index.elastic.internal.ElasticFeatureToggles;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.jackrabbit.oak.plugins.index.TestUtil.assertEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.answersWithDelay;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ElasticIndexStatisticsTest {

    private static final String INDEX_ALIAS = "test-index";
    private static final long CREATION_DATE = 1_700_000_000_000L;
    private static final ShardStatistics SHARDS =
            ShardStatistics.of(s -> s.total(1).successful(1).failed(0));

    @Mock
    private ElasticConnection elasticConnectionMock;

    @Mock
    private ElasticIndexDefinition indexDefinitionMock;

    @Mock
    private ElasticsearchClient elasticClientMock;

    @Mock
    private ElasticsearchIndicesClient indicesClientMock;

    @Mock
    private ElasticsearchCatClient catClientMock;

    private ElasticIndexStatistics indexStatistics;

    private AutoCloseable closeable;

    @Before
    public void setUp() {
        this.closeable = MockitoAnnotations.openMocks(this);
        when(indexDefinitionMock.getIndexAlias()).thenReturn(INDEX_ALIAS);
        when(elasticConnectionMock.getClient()).thenReturn(elasticClientMock);
        when(elasticClientMock.indices()).thenReturn(indicesClientMock);
        when(elasticClientMock.cat()).thenReturn(catClientMock);
        indexStatistics = new ElasticIndexStatistics(elasticConnectionMock, indexDefinitionMock);
    }

    @After
    public void releaseMocks() throws Exception {
        closeable.close();
        ElasticIndexStatistics.FT_OAK_12248_ENABLE.set(false);
        ElasticFeatureToggles.FT_OAK_12381_DISABLE.set(false);
    }

    @Test
    public void storeStatsDefaultPath() throws Exception {
        // Default path (feature toggle FT_OAK-12381 enabled)
        IndicesStats stats = IndicesStats.of(s -> s
                .total(t -> t.store(st -> st.sizeInBytes(2048).reservedInBytes(0)))
                .primaries(p -> p
                        .store(st -> st.sizeInBytes(1024).reservedInBytes(0))
                        .docs(d -> d.count(42).deleted(3L).totalSizeInBytes(0))));
        stubStoreStats(Map.of(INDEX_ALIAS, stats), indexSettings(CREATION_DATE));

        assertEquals(2048, indexStatistics.storeSize());
        assertEquals(1024, indexStatistics.primaryStoreSize());
        assertEquals(42, indexStatistics.luceneNumDocs());
        assertEquals(3, indexStatistics.luceneNumDeletedDocs());
        assertEquals(CREATION_DATE, indexStatistics.creationDate());
    }

    @Test
    public void storeStatsLegacyPath() throws Exception {
        // Legacy path (feature toggle FT_OAK-12381 disabled)
        ElasticFeatureToggles.FT_OAK_12381_DISABLE.set(true);

        stubCatIndices(IndicesRecord.of(r -> r
                .storeSize("2048")
                .priStoreSize("1024")
                .creationDateString(Long.toString(CREATION_DATE))
                .docsCount("42")
                .docsDeleted("3")));

        assertEquals(2048, indexStatistics.storeSize());
        assertEquals(1024, indexStatistics.primaryStoreSize());
        assertEquals(42, indexStatistics.luceneNumDocs());
        assertEquals(3, indexStatistics.luceneNumDeletedDocs());
        assertEquals(CREATION_DATE, indexStatistics.creationDate());
    }

    @Test
    public void storeStatsThrowsWhenIndexDoesNotExist() throws Exception {
        // Neither the stats nor the settings API return anything for an unknown index.
        stubStoreStats(Map.of(), Map.of());
        assertStoreStatsFailure(IllegalStateException.class);
    }

    @Test
    public void legacyStoreStatsThrowsWhenIndexDoesNotExist() throws Exception {
        ElasticFeatureToggles.FT_OAK_12381_DISABLE.set(true);

        stubCatIndices();
        assertStoreStatsFailure(IllegalStateException.class);
    }

    @Test
    public void storeStatsOmitPrimaryValuesWhenPrimariesStatsAreMissing() throws Exception {
        // Only the total stats are available, e.g. because the primaries are unreachable.
        IndicesStats stats = IndicesStats.of(s -> s
                .total(t -> t.store(st -> st.sizeInBytes(2048).reservedInBytes(0))));
        stubStoreStats(Map.of(INDEX_ALIAS, stats), indexSettings(CREATION_DATE));

        assertEquals(2048, indexStatistics.storeSize());
        assertEquals(CREATION_DATE, indexStatistics.creationDate());
        assertEquals(-1, indexStatistics.primaryStoreSize());
        assertEquals(-1, indexStatistics.luceneNumDocs());
        assertEquals(-1, indexStatistics.luceneNumDeletedDocs());
    }

    @Test
    public void storeStatsReturnEmptyStatsOn404WithToggleEnabled() throws Exception {
        ElasticIndexStatistics.FT_OAK_12248_ENABLE.set(true);
        stubStoreStatsFailure(404);

        assertEquals(0, indexStatistics.storeSize());
        assertEquals(0, indexStatistics.primaryStoreSize());
        assertEquals(-1, indexStatistics.creationDate());
        assertEquals(0, indexStatistics.luceneNumDocs());
        assertEquals(0, indexStatistics.luceneNumDeletedDocs());
    }

    @Test
    public void storeStatsPropagatesNonNotFoundExceptions() throws Exception {
        // Only a 404 with the OAK-12248 toggle enabled is treated as an empty index; any other
        // Elasticsearch failure must still be propagated to the caller.
        stubStoreStatsFailure(500);
        assertStoreStatsFailure(ElasticsearchException.class);
    }

    @Test
    public void cachedStatistics() throws Exception {
        Clock.Virtual clock = new Clock.Virtual();
        ElasticIndexStatistics indexStatistics = statisticsWithCountCache(clock);

        // simulate some delay when invoking elastic
        stubCountWithDelay(100);

        // cache miss, read data from elastic
        assertEquals(100, indexStatistics.numDocs());
        verify(elasticClientMock).count(any(CountRequest.class));

        // index count changes in elastic
        stubCountWithDelay(1000);

        // cache hit, old value returned
        assertEquals(100, indexStatistics.numDocs());
        verifyNoMoreInteractions(elasticClientMock);

        // move cache time ahead of 2 minutes, cache reload time expired
        clock.waitFor(Duration.ofMinutes(2));
        // old value is returned, read fresh data from elastic in background
        assertEquals(100, indexStatistics.numDocs());

        assertEventually(() -> {
            try {
                verify(elasticClientMock, times(2)).count(any(CountRequest.class));
            } catch (IOException e) {
                fail(e.getMessage());
            }
            // cache hit, latest value returned
            assertEquals(1000, indexStatistics.numDocs());
        }, 1000);
        verifyNoMoreInteractions(elasticClientMock);

        // index count changes in elastic
        stubCountWithDelay(5000);

        // move cache time ahead of 15 minutes, cache value expired
        clock.waitFor(Duration.ofMinutes(15));

        // cache miss, read data from elastic
        assertEquals(5000, indexStatistics.numDocs());
        verify(elasticClientMock, times(3)).count(any(CountRequest.class));

        // move cache time ahead of 30 minutes, cache value expired
        clock.waitFor(Duration.ofMinutes(30));

        // cache miss, read data using an elastic query
        assertEquals(5000, indexStatistics.getDocCountFor(Query.of(qf -> qf.matchAll(mf -> mf))));
        verify(elasticClientMock, times(4)).count(any(CountRequest.class));

        // call again with the same query but a different instance
        assertEquals(5000, indexStatistics.getDocCountFor(Query.of(qf -> qf.matchAll(mf -> mf))));
        verifyNoMoreInteractions(elasticClientMock);

        // call again with a different query
        assertEquals(5000, indexStatistics.getDocCountFor(Query.of(qf -> qf.matchAll(mf -> mf.boost(100F)))));
        verify(elasticClientMock, times(5)).count(any(CountRequest.class));
    }

    @Test
    public void numDocsReturnsZeroOn404WithToggleEnabled() throws Exception {
        ElasticIndexStatistics.FT_OAK_12248_ENABLE.set(true);
        ElasticIndexStatistics stats = statisticsWithCountCache(null);
        when(elasticClientMock.count(any(CountRequest.class))).thenThrow(elasticsearchException(404));

        assertEquals(0, stats.numDocs());
        verify(elasticClientMock).count(any(CountRequest.class));
    }

    @Test
    public void numDocsRecoverAfterCacheExpiry() throws Exception {
        ElasticIndexStatistics.FT_OAK_12248_ENABLE.set(true);
        Clock.Virtual clock = new Clock.Virtual();
        ElasticIndexStatistics stats = statisticsWithCountCache(clock);
        when(elasticClientMock.count(any(CountRequest.class)))
                .thenThrow(elasticsearchException(404))
                .thenReturn(countResponse(5));

        assertEquals(0, stats.numDocs());

        clock.waitFor(Duration.ofMinutes(11));

        assertEquals(5, stats.numDocs());
        verify(elasticClientMock, times(2)).count(any(CountRequest.class));
    }

    private void stubStoreStats(Map<String, IndicesStats> stats, Map<String, IndexState> settings) throws IOException {
        when(indicesClientMock.stats(
                ArgumentMatchers.<Function<IndicesStatsRequest.Builder, ObjectBuilder<IndicesStatsRequest>>>any()))
                .thenReturn(IndicesStatsResponse.of(r -> r
                        .indices(stats).shards(SHARDS).all(a -> a)));
        when(indicesClientMock.getSettings(
                ArgumentMatchers.<Function<GetIndicesSettingsRequest.Builder, ObjectBuilder<GetIndicesSettingsRequest>>>any()))
                .thenReturn(GetIndicesSettingsResponse.of(r -> r.settings(settings)));
    }

    private static Map<String, IndexState> indexSettings(long creationDate) {
        return Map.of(INDEX_ALIAS, IndexState.of(s -> s
                .settings(outer -> outer.index(inner -> inner.creationDate(creationDate)))));
    }

    private void stubCatIndices(IndicesRecord... records) throws IOException {
        when(catClientMock.indices(
                ArgumentMatchers.<Function<IndicesRequest.Builder, ObjectBuilder<IndicesRequest>>>any()))
                .thenReturn(IndicesResponse.of(r -> r.indices(List.of(records))));
    }

    private void stubStoreStatsFailure(int status) throws IOException {
        when(indicesClientMock.stats(
                ArgumentMatchers.<Function<IndicesStatsRequest.Builder, ObjectBuilder<IndicesStatsRequest>>>any()))
                .thenThrow(elasticsearchException(status));
    }

    private void assertStoreStatsFailure(Class<? extends Throwable> type) {
        try {
            indexStatistics.storeSize();
            fail("expected " + type.getSimpleName() + " when retrieving store stats");
        } catch (RuntimeException e) {
            assertNotNull(findCause(e, type));
        }
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

    private ElasticIndexStatistics statisticsWithCountCache(Clock.Virtual clock) {
        LoadingCache<ElasticIndexStatistics.StatsRequestDescriptor, Integer> cache =
                ElasticIndexStatistics.setupCountCache(100, 10 * 60, 60, clock);
        return new ElasticIndexStatistics(elasticConnectionMock, indexDefinitionMock, cache, null);
    }

    private void stubCountWithDelay(long count) throws IOException {
        CountResponse response = countResponse(count);
        doAnswer(answersWithDelay(250, i -> response))
                .when(elasticClientMock).count(any(CountRequest.class));
    }

    private static CountResponse countResponse(long count) {
        return CountResponse.of(r -> r.count(count).shards(SHARDS));
    }

    private static ElasticsearchException elasticsearchException(int status) {
        return new ElasticsearchException("test", ErrorResponse.of(r -> r
                .status(status).error(e -> e.type("test_exception").reason("test failure"))));
    }
}
