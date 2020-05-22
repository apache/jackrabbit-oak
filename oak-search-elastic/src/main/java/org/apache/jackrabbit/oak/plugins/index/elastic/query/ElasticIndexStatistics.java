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
package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.IndexStatistics;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.IOException;
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
class ElasticIndexStatistics implements IndexStatistics {

    private static final Long MAX_SIZE = Long.getLong("oak.elastic.statsMaxSize", 10000);
    private static final Long EXPIRE_MIN = Long.getLong("oak.elastic.statsExpireMin", 10);
    private static final Long REFRESH_MIN = Long.getLong("oak.elastic.statsRefreshMin", 1);

    private static final LoadingCache<CountRequestDescriptor, Integer> DEFAULT_STATS_CACHE =
            setupCache(MAX_SIZE, EXPIRE_MIN, REFRESH_MIN, null);

    private final ElasticConnection elasticConnection;
    private final ElasticIndexDefinition indexDefinition;
    private final LoadingCache<CountRequestDescriptor, Integer> statsCache;

    ElasticIndexStatistics(@NotNull ElasticConnection elasticConnection,
                           @NotNull ElasticIndexDefinition indexDefinition) {
        this(elasticConnection, indexDefinition, DEFAULT_STATS_CACHE);
    }

    @TestOnly
    ElasticIndexStatistics(@NotNull ElasticConnection elasticConnection,
                           @NotNull ElasticIndexDefinition indexDefinition,
                           @NotNull LoadingCache<CountRequestDescriptor, Integer> statsCache) {
        this.elasticConnection = elasticConnection;
        this.indexDefinition = indexDefinition;
        this.statsCache = statsCache;
    }

    /**
     * Returns the approximate number of documents for the remote index bound to the {@code ElasticIndexDefinition}.
     */
    @Override
    public int numDocs() {
        return statsCache.getUnchecked(
                new CountRequestDescriptor(elasticConnection, indexDefinition.getRemoteIndexAlias(), null)
        );
    }

    /**
     * Returns the approximate number of documents for the {@code field} in the remote index bound to the
     * {@code ElasticIndexDefinition}.
     */
    @Override
    public int getDocCountFor(String field) {
        return statsCache.getUnchecked(
                new CountRequestDescriptor(elasticConnection, indexDefinition.getRemoteIndexAlias(), field)
        );
    }

    static LoadingCache<CountRequestDescriptor, Integer> setupCache(long maxSize, long expireMin, long refreshMin,
                                                                    @Nullable Ticker ticker) {
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(expireMin, TimeUnit.MINUTES)
                // https://github.com/google/guava/wiki/CachesExplained#refresh
                .refreshAfterWrite(refreshMin, TimeUnit.MINUTES);
        if (ticker != null) {
            cacheBuilder.ticker(ticker);
        }
        return cacheBuilder
                .build(new CacheLoader<CountRequestDescriptor, Integer>() {
                    @Override
                    public Integer load(CountRequestDescriptor countRequestDescriptor) throws IOException {
                        return count(countRequestDescriptor);
                    }

                    @Override
                    public ListenableFuture<Integer> reload(CountRequestDescriptor crd, Integer oldValue) {
                        ListenableFutureTask<Integer> task = ListenableFutureTask.create(() -> count(crd));
                        Executors.newSingleThreadExecutor().execute(task);
                        return task;
                    }
                });
    }

    private static int count(CountRequestDescriptor crd) throws IOException {
        CountRequest countRequest = new CountRequest(crd.index);
        if (crd.field != null) {
            countRequest.query(QueryBuilders.existsQuery(crd.field));
        } else {
            countRequest.query(QueryBuilders.matchAllQuery());
        }

        CountResponse response = crd.connection.getClient().count(countRequest, RequestOptions.DEFAULT);
        return (int) response.getCount();
    }

    static class CountRequestDescriptor {

        @NotNull
        final ElasticConnection connection;
        @NotNull
        final String index;
        @Nullable
        final String field;

        public CountRequestDescriptor(@NotNull ElasticConnection connection,
                                      @NotNull String index, @Nullable String field) {
            this.connection = connection;
            this.index = index;
            this.field = field;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CountRequestDescriptor that = (CountRequestDescriptor) o;
            return index.equals(that.index) &&
                    Objects.equals(field, that.field);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, field);
        }
    }
}
