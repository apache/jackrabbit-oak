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

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import org.apache.jackrabbit.guava.common.base.Ticker;
import org.apache.jackrabbit.guava.common.cache.CacheBuilder;
import org.apache.jackrabbit.guava.common.cache.CacheLoader;
import org.apache.jackrabbit.guava.common.cache.LoadingCache;
import org.apache.jackrabbit.guava.common.util.concurrent.ListenableFuture;
import org.apache.jackrabbit.guava.common.util.concurrent.ListenableFutureTask;
import org.apache.jackrabbit.guava.common.util.concurrent.ThreadFactoryBuilder;

import co.elastic.clients.elasticsearch._types.Bytes;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import co.elastic.clients.elasticsearch.core.CountRequest;

/**
 * Cache-based {@code IndexStatistics} implementation providing statistics for Elasticsearch reducing
 * network operations.
 * <p>
 * By default, the cache can contain a max of 10000 entries, statistic values expire after 10 minutes (600 seconds) but are refreshed
 * in background when accessed after 1 minute (60 seconds). These values can be overwritten with the following system properties:
 *
 * <ul>
 *     <li>{@code oak.elastic.statsMaxSize}</li>
 *     <li>{@code oak.elastic.statsExpireSeconds}</li>
 *     <li>{@code oak.elastic.statsRefreshSeconds}</li>
 * </ul>
 */
public class ElasticIndexStatistics implements IndexStatistics {

    private static final String MAX_SIZE = "oak.elastic.statsMaxSize";
    private static final Long MAX_SIZE_DEFAULT = 10000L;
    private static final String EXPIRE_SECONDS = "oak.elastic.statsExpireSeconds";
    private static final Long EXPIRE_SECONDS_DEFAULT = 10 * 60L;
    private static final String REFRESH_SECONDS = "oak.elastic.statsRefreshSeconds";
    private static final Long REFRESH_SECONDS_DEFAULT = 60L;

    private static final ExecutorService REFRESH_EXECUTOR = new ThreadPoolExecutor(
            0, 4, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder()
                    .setNameFormat("elastic-statistics-cache-refresh-thread-%d")
                    .setDaemon(true)
                    .build()
    );

    private final ElasticConnection elasticConnection;
    private final ElasticIndexDefinition indexDefinition;
    private final LoadingCache<StatsRequestDescriptor, Integer> countCache;
    private final LoadingCache<StatsRequestDescriptor, StatsResponse> statsCache;

    ElasticIndexStatistics(@NotNull ElasticConnection elasticConnection,
                           @NotNull ElasticIndexDefinition indexDefinition) {
        this(elasticConnection, indexDefinition, null, null);
    }

    @TestOnly
    ElasticIndexStatistics(@NotNull ElasticConnection elasticConnection,
                           @NotNull ElasticIndexDefinition indexDefinition,
                           @Nullable LoadingCache<StatsRequestDescriptor, Integer> countCache,
                           @Nullable LoadingCache<StatsRequestDescriptor, StatsResponse> statsCache) {
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.countCache = Objects.requireNonNullElseGet(countCache, () ->
                setupCountCache(getCacheMaxSize(), getCacheExpireSeconds(), getCacheRefreshSeconds(), null));
        this.statsCache = Objects.requireNonNullElseGet(statsCache, () ->
                setupCache(getCacheMaxSize(), getCacheExpireSeconds(), getCacheRefreshSeconds(), new StatsCacheLoader(), null));

    }

    /**
     * Returns the approximate number of documents for the remote index bound to the {@code ElasticIndexDefinition}.
     */
    @Override
    public int numDocs() {
        return countCache.getUnchecked(new StatsRequestDescriptor(elasticConnection, indexDefinition.getIndexAlias()));
    }

    /**
     * Returns the approximate number of documents for the {@code field} in the remote index bound to the
     * {@code ElasticIndexDefinition}.
     */
    @Override
    public int getDocCountFor(String field) {
        return countCache.getUnchecked(
                new StatsRequestDescriptor(elasticConnection, indexDefinition.getIndexAlias(), field, null)
        );
    }

    /**
     * Returns the approximate number of documents for the {@code query} in the remote index bound to the
     * {@code ElasticIndexDefinition}.
     */
    public int getDocCountFor(Query query) {
        return countCache.getUnchecked(
                new StatsRequestDescriptor(elasticConnection, indexDefinition.getIndexAlias(), null, query)
        );
    }

    /**
     * Returns the approximate size in bytes for the primary shards of the remote index bound to the
     * {@code ElasticIndexDefinition}.
     */
    public long primaryStoreSize() {
        return statsCache.getUnchecked(
                new StatsRequestDescriptor(elasticConnection, indexDefinition.getIndexAlias())
        ).primaryStoreSize;
    }

    /**
     * Returns the approximate size in bytes for the remote index bound to the {@code ElasticIndexDefinition}, including
     * primary shards and replica shards.
     */
    public long storeSize() {
        return statsCache.getUnchecked(
                new StatsRequestDescriptor(elasticConnection, indexDefinition.getIndexAlias())
        ).storeSize;
    }

    /**
     * Returns the creation date for the remote index bound to the {@code ElasticIndexDefinition}.
     */
    public long creationDate() {
        return statsCache.getUnchecked(
                new StatsRequestDescriptor(elasticConnection, indexDefinition.getIndexAlias())
        ).creationDate;
    }

    /**
     * Returns the number of low level lucene documents for the remote index bound to the
     * {@code ElasticIndexDefinition}. This document count includes hidden nested documents.
     */
    public int luceneNumDocs() {
        return statsCache.getUnchecked(
                new StatsRequestDescriptor(elasticConnection, indexDefinition.getIndexAlias())
        ).luceneDocsCount;
    }

    /**
     * Returns the number of deleted low level lucene documents for the remote index bound to the
     * {@code ElasticIndexDefinition}. This document count includes hidden nested documents.
     */
    public int luceneNumDeletedDocs() {
        return statsCache.getUnchecked(
                new StatsRequestDescriptor(elasticConnection, indexDefinition.getIndexAlias())
        ).luceneDocsDeleted;
    }

    static LoadingCache<StatsRequestDescriptor, Integer> setupCountCache(long maxSize, long expireSeconds, long refreshSeconds, @Nullable Ticker ticker) {
        return setupCache(maxSize, expireSeconds, refreshSeconds, new CountCacheLoader(), ticker);
    }

    static <K, V> LoadingCache<K, V> setupCache(long maxSize, long expireSeconds, long refreshSeconds,
                                                @NotNull CacheLoader<K, V> cacheLoader, @Nullable Ticker ticker) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireSeconds, TimeUnit.SECONDS)
                // https://github.com/google/guava/wiki/CachesExplained#refresh
                .refreshAfterWrite(refreshSeconds, TimeUnit.SECONDS);
        if (ticker != null) {
            cacheBuilder.ticker(ticker);
        }
        return cacheBuilder.build(cacheLoader);
    }

    private Long getCacheMaxSize() {
        return Long.getLong(MAX_SIZE, MAX_SIZE_DEFAULT);
    }

    private Long getCacheExpireSeconds() {
        return Long.getLong(EXPIRE_SECONDS, EXPIRE_SECONDS_DEFAULT);
    }

    private Long getCacheRefreshSeconds() {
        return Long.getLong(REFRESH_SECONDS, REFRESH_SECONDS_DEFAULT);
    }

    static class CountCacheLoader extends CacheLoader<StatsRequestDescriptor, Integer> {

        @Override
        public @NotNull Integer load(@NotNull StatsRequestDescriptor countRequestDescriptor) throws IOException {
            return count(countRequestDescriptor);
        }

        @Override
        public @NotNull ListenableFuture<Integer> reload(@NotNull StatsRequestDescriptor crd, @NotNull Integer oldValue) {
            ListenableFutureTask<Integer> task = ListenableFutureTask.create(() -> count(crd));
            REFRESH_EXECUTOR.execute(task);
            return task;
        }

        private int count(StatsRequestDescriptor crd) throws IOException {
            CountRequest.Builder cBuilder = new CountRequest.Builder();
            cBuilder.index(crd.index);
            if (crd.query != null) {
                cBuilder.query(crd.query);
            } else if (crd.field != null) {
                cBuilder.query(q -> q.exists(e -> e.field(crd.field)));
            } else {
                cBuilder.query(q -> q.matchAll(m -> m));
            }
            return (int) crd.connection.getClient().count(cBuilder.build()).count();
        }
    }

    static class StatsCacheLoader extends CacheLoader<StatsRequestDescriptor, StatsResponse> {

        @Override
        public @NotNull StatsResponse load(@NotNull StatsRequestDescriptor countRequestDescriptor) throws IOException {
            return stats(countRequestDescriptor);
        }

        @Override
        public @NotNull ListenableFuture<StatsResponse> reload(@NotNull StatsRequestDescriptor crd, @NotNull StatsResponse oldValue) {
            ListenableFutureTask<StatsResponse> task = ListenableFutureTask.create(() -> stats(crd));
            REFRESH_EXECUTOR.execute(task);
            return task;
        }

        private StatsResponse stats(StatsRequestDescriptor crd) throws IOException {
            List<IndicesRecord> records = crd.connection.getClient().cat().indices(i -> i
                            .index(crd.index)
                            .bytes(Bytes.Bytes))
                    .valueBody();
            if (records.isEmpty()) {
                throw new IllegalStateException("Cannot retrieve stats for index " + crd.index + " as it does not exist");
            }
            // Assuming a single index matches crd.index
            IndicesRecord record = records.get(0);
            String storeSize = record.storeSize();
            String primaryStoreSize = record.priStoreSize();
            String creationDate = record.creationDateString();
            String luceneDocsCount = record.docsCount();
            String luceneDocsDeleted = record.docsDeleted();

            return new StatsResponse(
                    storeSize != null ? Long.parseLong(storeSize) : -1,
                    primaryStoreSize != null ? Long.parseLong(primaryStoreSize) : -1,
                    creationDate != null ? Long.parseLong(creationDate) : -1,
                    luceneDocsCount != null ? Integer.parseInt(luceneDocsCount) : -1,
                    luceneDocsDeleted != null ? Integer.parseInt(luceneDocsDeleted) : -1
            );
        }
    }

    static class StatsRequestDescriptor {

        @NotNull
        final ElasticConnection connection;
        @NotNull
        final String index;
        @Nullable
        final String field;
        @Nullable
        final Query query;

        StatsRequestDescriptor(@NotNull ElasticConnection connection,
                               @NotNull String index) {
            this(connection, index, null, null);
        }

        StatsRequestDescriptor(@NotNull ElasticConnection connection,
                               @NotNull String index, @Nullable String field, @Nullable Query query) {
            this.connection = connection;
            this.index = index;
            this.field = field;
            this.query = query;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StatsRequestDescriptor that = (StatsRequestDescriptor) o;
            return index.equals(that.index) &&
                    Objects.equals(field, that.field) &&
                    // ES Query objects are not comparable, so we need to compare their string representations
                    Objects.equals(internalQuery(), that.internalQuery());
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, field, internalQuery());
        }

        private String internalQuery() {
            return query != null ? query.toString() : null;
        }
    }

    static class StatsResponse {

        final long storeSize;
        final long primaryStoreSize;
        final long creationDate;
        final int luceneDocsCount;
        final int luceneDocsDeleted;

        StatsResponse(long storeSize, long primaryStoreSize, long creationDate, int luceneDocsCount, int luceneDocsDeleted) {
            this.storeSize = storeSize;
            this.primaryStoreSize = primaryStoreSize;
            this.creationDate = creationDate;
            this.luceneDocsCount = luceneDocsCount;
            this.luceneDocsDeleted = luceneDocsDeleted;
        }
    }
}
