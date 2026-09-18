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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bounded, time-limited cache of Elastic queries that recently failed with a query-parsing error (e.g. a
 * malformed full-text search term, often caused by malicious/automated probing traffic). Repeated identical
 * queries always fail the same way, so once a query is known to be invalid we can skip calling Elastic
 * altogether until the entry expires, reducing load on the cluster (see OAK-70592).
 * <p>
 * The cache key is expected to be a stable identifier for the "logical" query (e.g. index path + query DSL),
 * independent of pagination state such as {@code search_after}.
 * <p>
 * Disabled by default; enable via the {@link #FT_OAK_70592} feature toggle.
 */
public final class ElasticInvalidQueryCache {

    public static final String FT_OAK_70592 = "FT_OAK-70592";
    /**
     * When {@code true}, queries that recently failed with an Elastic parsing error are cached and
     * subsequent identical queries are short-circuited (no call to Elastic, no results) until the cache
     * entry expires. Disabled by default.
     */
    public static final AtomicBoolean FT_OAK_70592_ENABLE = new AtomicBoolean(false);

    private static final int MAX_ENTRIES = 1000;
    private static final long TTL_MS = TimeUnit.MINUTES.toMillis(5);

    private static final Map<String, Long> INVALID_QUERIES = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > MAX_ENTRIES;
        }
    };

    private ElasticInvalidQueryCache() {
        // no instances
    }

    /**
     * Records that the query identified by {@code cacheKey} recently failed with a parsing error.
     * No-op when the feature toggle is disabled.
     */
    public static void markInvalid(String cacheKey) {
        if (!FT_OAK_70592_ENABLE.get() || cacheKey == null) {
            return;
        }
        synchronized (INVALID_QUERIES) {
            INVALID_QUERIES.put(cacheKey, System.currentTimeMillis());
        }
    }

    /**
     * Returns {@code true} if the query identified by {@code cacheKey} recently failed with a parsing error
     * and the corresponding cache entry has not expired yet. Always returns {@code false} when the feature
     * toggle is disabled.
     */
    public static boolean isKnownInvalid(String cacheKey) {
        if (!FT_OAK_70592_ENABLE.get() || cacheKey == null) {
            return false;
        }
        synchronized (INVALID_QUERIES) {
            Long recordedAt = INVALID_QUERIES.get(cacheKey);
            if (recordedAt == null) {
                return false;
            }
            if (System.currentTimeMillis() - recordedAt > TTL_MS) {
                INVALID_QUERIES.remove(cacheKey);
                return false;
            }
            return true;
        }
    }

    /**
     * Removes every entry from the cache. Intended for tests.
     */
    static void clear() {
        synchronized (INVALID_QUERIES) {
            INVALID_QUERIES.clear();
        }
    }
}
