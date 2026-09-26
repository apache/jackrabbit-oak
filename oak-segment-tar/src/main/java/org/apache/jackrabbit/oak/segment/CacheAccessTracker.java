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
 *
 */

package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@code Cache} wrapper exposing the number of read accesses and the
 * number of misses of the underlying cache via the {@link StatisticsProvider}.
 */
public class CacheAccessTracker<K, V> implements Cache<K, V> {
    @NotNull
    private final Cache<K, V> delegate;
    @NotNull
    private final CounterStats accessCount;
    @NotNull
    private final CounterStats missCount;

    /**
     * Create a new wrapper exposing access statistics via the already registered
     * {@code accessCount}/{@code missCount}, letting callers that construct many
     * short-lived wrappers around the same kind of cache (e.g. one per record written)
     * reuse already-registered statistics instead of re-registering them on every call.
     */
    public CacheAccessTracker(
            @NotNull Cache<K, V> delegate,
            @NotNull CounterStats accessCount,
            @NotNull CounterStats missCount) {
        this.delegate = delegate;
        this.accessCount = accessCount;
        this.missCount = missCount;
    }

    /**
     * Create a new wrapper exposing the access statistics under the given
     * {@code name} to the passed {@code statisticsProvider}.
     *
     * @deprecated use {@link #CacheAccessTracker(Cache, CounterStats, CounterStats)}
     * with registered counters instead.
     */
    @Deprecated
    public CacheAccessTracker(
            @NotNull String name,
            @NotNull StatisticsProvider statisticsProvider,
            @NotNull Cache<K, V> delegate) {
        this(delegate,
                statisticsProvider.getCounterStats(name + ".access-count", StatsOptions.DEFAULT),
                statisticsProvider.getCounterStats(name + ".miss-count", StatsOptions.DEFAULT));
    }

    @Override
    public void put(@NotNull K key, @NotNull V value) {
        delegate.put(key, value);
    }

    @Override
    public void put(@NotNull K key, @NotNull V value, byte cost) {
        delegate.put(key, value, cost);
    }

    @Nullable
    @Override
    public V get(@NotNull K key) {
        V v = delegate.get(key);
        accessCount.inc();
        if (v == null) {
            missCount.inc();
        }
        return v;
    }
}
