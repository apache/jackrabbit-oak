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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.async;

import java.time.Duration;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.plugins.index.elastic.internal.ElasticFeatureToggles;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Bounded, time-limited cache of Elastic queries that recently failed with a query-parsing error (e.g. a
 * malformed full-text search term, often caused by malicious/automated probing traffic). Repeated identical
 * queries always fail the same way, so once a query is known to be invalid we can skip calling Elastic
 * altogether until the entry expires, reducing load on the cluster (see OAK-70592).
 * <p>
 * The cache key is expected to be a stable identifier for the "logical" query (e.g. index path + query DSL),
 * independent of pagination state such as {@code search_after}.
 * <p>
 * Entries expire five minutes after being recorded, with a maximum cache size of 1000 entries.
 * Disabled by default; enable via the {@link ElasticFeatureToggles#FT_OAK_12403} feature toggle.
 */
public final class ElasticInvalidQueryCache {

    private static final int MAX_ENTRIES = 1000;
    private static final Cache<String, Boolean> INVALID_QUERIES = setupCache(null);

    private ElasticInvalidQueryCache() {
        // no instances
    }

    static Cache<String, Boolean> setupCache(@Nullable Supplier<Long> ticker) {
        CacheBuilder<String, Boolean> builder = CacheBuilder.<String, Boolean>newBuilder()
                .maximumSize(MAX_ENTRIES)
                .expireAfterWrite(Duration.ofMinutes(5));
        if (ticker != null) {
            builder.ticker(ticker);
        }
        return builder.build();
    }

    /**
     * Records that the query identified by {@code cacheKey} recently failed with a parsing error.
     * No-op when the feature toggle is disabled.
     */
    public static void markInvalid(String cacheKey) {
        if (!ElasticFeatureToggles.FT_OAK_12403_ENABLE.get() || cacheKey == null) {
            return;
        }
        INVALID_QUERIES.put(cacheKey, Boolean.TRUE);
    }

    /**
     * Returns {@code true} if the query identified by {@code cacheKey} recently failed with a parsing error
     * and the corresponding cache entry has not expired yet. Always returns {@code false} when the feature
     * toggle is disabled.
     */
    public static boolean isKnownInvalid(String cacheKey) {
        if (!ElasticFeatureToggles.FT_OAK_12403_ENABLE.get() || cacheKey == null) {
            return false;
        }
        return INVALID_QUERIES.getIfPresent(cacheKey) != null;
    }

    /**
     * Removes every entry from the cache. Intended for tests.
     */
    @TestOnly
    static void clear() {
        INVALID_QUERIES.invalidateAll();
    }
}
