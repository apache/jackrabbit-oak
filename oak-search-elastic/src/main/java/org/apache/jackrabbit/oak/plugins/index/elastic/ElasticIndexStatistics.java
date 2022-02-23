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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;

import co.elastic.clients.elasticsearch._types.Bytes;
import co.elastic.clients.elasticsearch.cat.IndicesResponse;
import co.elastic.clients.elasticsearch.cat.indices.IndicesRecord;
import co.elastic.clients.elasticsearch.core.CountRequest.Builder;

import org.apache.http.util.EntityUtils;
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Cache-based {@code IndexStatistics} implementation providing statistics for Elasticsearch reducing
 * network operations.
 * <p>
 * By default, the cache can contain a max of 10000 entries, statistic values expire after 10 minutes but are refreshed
 * in background when accessed after 1 minute. These values can be overwritten with the following system properties:
 *
 * <ul>
 *     <li>{@code oak.elastic.statsMaxSize}</li>
 *     <li>{@code oak.elastic.statsExpireMin}</li>
 *     <li>{@code oak.elastic.statsRefreshMin}</li>
 * </ul>
 */
public class ElasticIndexStatistics implements IndexStatistics {

    private static final Long MAX_SIZE = Long.getLong("oak.elastic.statsMaxSize", 10000);
    private static final Long EXPIRE_MIN = Long.getLong("oak.elastic.statsExpireMin", 10);
    private static final Long REFRESH_MIN = Long.getLong("oak.elastic.statsRefreshMin", 1);

    private static final LoadingCache<StatsRequestDescriptor, Integer> DEFAULT_COUNT_CACHE =
            setupCountCache(MAX_SIZE, EXPIRE_MIN, REFRESH_MIN, null);

    private static final LoadingCache<StatsRequestDescriptor, Long> SIZE_CACHE =
            setupCache(MAX_SIZE, EXPIRE_MIN, REFRESH_MIN, new SizeCacheLoader(), null);

    private final ElasticConnection elasticConnection;
    private final ElasticIndexDefinition indexDefinition;
    private final LoadingCache<StatsRequestDescriptor, Integer> countCache;

    ElasticIndexStatistics(@NotNull ElasticConnection elasticConnection,
                           @NotNull ElasticIndexDefinition indexDefinition) {
        this(elasticConnection, indexDefinition, DEFAULT_COUNT_CACHE);
    }

    @TestOnly
    ElasticIndexStatistics(@NotNull ElasticConnection elasticConnection,
                           @NotNull ElasticIndexDefinition indexDefinition,
                           @NotNull LoadingCache<StatsRequestDescriptor, Integer> countCache) {
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.countCache = countCache;
    }

    /**
     * Returns the approximate number of documents for the remote index bound to the {@code ElasticIndexDefinition}.
     */
    @Override
    public int numDocs() {
        return countCache.getUnchecked(
                new StatsRequestDescriptor(elasticConnection, indexDefinition.getIndexAlias(), null)
        );
    }

    /**
     * Returns the approximate number of documents for the {@code field} in the remote index bound to the
     * {@code ElasticIndexDefinition}.
     */
    @Override
    public int getDocCountFor(String field) {
        return countCache.getUnchecked(
                new StatsRequestDescriptor(elasticConnection, indexDefinition.getIndexAlias(), field)
        );
    }

    public long size() {
        return SIZE_CACHE.getUnchecked(
                new StatsRequestDescriptor(elasticConnection, indexDefinition.getIndexAlias(), null)
        );
    }

    static LoadingCache<StatsRequestDescriptor, Integer> setupCountCache(long maxSize, long expireMin, long refreshMin, @Nullable Ticker ticker) {
        return setupCache(maxSize, expireMin, refreshMin, new CountCacheLoader(), ticker);
    }

    static <K, V> LoadingCache<K, V> setupCache(long maxSize, long expireMin, long refreshMin,
                                                @NotNull CacheLoader<K, V> cacheLoader, @Nullable Ticker ticker) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireMin, TimeUnit.MINUTES)
                // https://github.com/google/guava/wiki/CachesExplained#refresh
                .refreshAfterWrite(refreshMin, TimeUnit.MINUTES);
        if (ticker != null) {
            cacheBuilder.ticker(ticker);
        }
        return cacheBuilder.build(cacheLoader);
    }

    static class CountCacheLoader extends CacheLoader<StatsRequestDescriptor, Integer> {

        @Override
        public Integer load(StatsRequestDescriptor countRequestDescriptor) throws IOException {
            return count(countRequestDescriptor);
        }

        @Override
        public ListenableFuture<Integer> reload(StatsRequestDescriptor crd, Integer oldValue) {
            ListenableFutureTask<Integer> task = ListenableFutureTask.create(() -> count(crd));
            Executors.newSingleThreadExecutor().execute(task);
            return task;
        }

        private int count(StatsRequestDescriptor crd) throws IOException {
            Builder reqBuilder = new co.elastic.clients.elasticsearch.core.CountRequest.Builder();
            reqBuilder.index(crd.index);
            if (crd.field != null) {
                reqBuilder.query(q->q
                        .exists(e->e
                                .field(crd.field)));
            } else {
                reqBuilder.query(q->q
                        .matchAll(m->m));
            }
            return (int) crd.connection.getClient().count(reqBuilder.build()).count();
        }
    }

    static class SizeCacheLoader extends CacheLoader<StatsRequestDescriptor, Long> {

        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public Long load(StatsRequestDescriptor countRequestDescriptor) throws IOException {
            return size(countRequestDescriptor);
        }

        @Override
        public ListenableFuture<Long> reload(StatsRequestDescriptor crd, Long oldValue) {
            ListenableFutureTask<Long> task = ListenableFutureTask.create(() -> size(crd));
            Executors.newSingleThreadExecutor().execute(task);
            return task;
        }

        private long size(StatsRequestDescriptor crd) throws IOException {
            String size = crd.connection.getClient().cat().indices(i->i
                    .index(crd.index)
                    .bytes(Bytes.Bytes))
                .valueBody().get(0).storeSize();
            if (size != null) {
                return Long.parseLong(size);
            }
            return -1L;
        }
    }

    static class StatsRequestDescriptor {

        @NotNull
        final ElasticConnection connection;
        @NotNull
        final String index;
        @Nullable
        final String field;

        public StatsRequestDescriptor(@NotNull ElasticConnection connection,
                                      @NotNull String index, @Nullable String field) {
            this.connection = connection;
            this.index = index;
            this.field = field;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StatsRequestDescriptor that = (StatsRequestDescriptor) o;
            return index.equals(that.index) &&
                    Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, field);
        }
    }
}
