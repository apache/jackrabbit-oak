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
package org.apache.jackrabbit.oak.cache.api.impl;

import java.util.Map;

import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheStats;
import org.apache.jackrabbit.oak.cache.api.Weigher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Exposes an {@link Cache}'s statistics via the {@link org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean}
 * interface by bridging {@link CacheStats} to the Guava shim {@link org.apache.jackrabbit.guava.common.cache.CacheStats} expected
 * by {@link AbstractCacheStats}.
 *
 * <p>The Guava return type from {@link #getCurrentStats()} is kept until TASK-16 updates
 * the base class to use {@link CacheStats} directly.</p>
 *
 * <p>TODO OAK-TASK16: per {@code TASKS.md}, remove this temporary adapter in
 * TASK-16 once {@link AbstractCacheStats} consumes {@link CacheStats}
 * directly.</p>
 */
public class CacheStatsAdapter extends AbstractCacheStats {

    private final Cache<Object, Object> cache;
    private final Weigher<Object, Object> weigher;
    private final long maxWeight;

    /**
     * Creates an adapter for the given cache.
     *
     * @param cache     the cache whose statistics to expose (must not be null)
     * @param name      the JMX bean name (must not be null)
     * @param weigher   optional weigher used to estimate current cache weight; {@code null} if unknown
     * @param maxWeight configured maximum weight for the cache; {@code -1} if unbounded
     */
    @SuppressWarnings("unchecked")
    public CacheStatsAdapter(
            @NotNull Cache<?, ?> cache,
            @NotNull String name,
            @Nullable Weigher<?, ?> weigher,
            long maxWeight) {
        super(name);
        this.cache = (Cache<Object, Object>) cache;
        this.weigher = (Weigher<Object, Object>) weigher;
        this.maxWeight = maxWeight;
    }

    @Override
    protected org.apache.jackrabbit.guava.common.cache.CacheStats getCurrentStats() {
        CacheStats s = cache.stats();
        return new org.apache.jackrabbit.guava.common.cache.CacheStats(
                s.hitCount(), s.missCount(),
                s.loadSuccessCount(), s.loadFailureCount(),
                s.totalLoadTime(), s.evictionCount());
    }

    @Override
    public long getElementCount() {
        return cache.asMap().size();
    }

    @Override
    public long estimateCurrentWeight() {
        if (weigher == null) {
            return -1;
        }
        long total = 0;
        for (Map.Entry<Object, Object> e : cache.asMap().entrySet()) {
            total += weigher.weigh(e.getKey(), e.getValue());
        }
        return total;
    }

    @Override
    public long getMaxTotalWeight() {
        return maxWeight;
    }
}
