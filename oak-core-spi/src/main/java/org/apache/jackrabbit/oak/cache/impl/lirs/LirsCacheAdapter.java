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
package org.apache.jackrabbit.oak.cache.impl.lirs;

import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import org.apache.jackrabbit.guava.common.cache.RemovalCause;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheStatsSnapshot;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.jetbrains.annotations.NotNull;

/**
 * {@link Cache} adapter wrapping a {@link CacheLIRS} instance.
 *
 * <p>Adapts CacheLIRS's checked loading contract to the Caffeine-aligned Oak
 * API: runtime failures propagate directly and checked loader failures are
 * wrapped in {@link CompletionException}.</p>
 */
class LirsCacheAdapter<K, V> implements Cache<K, V> {

    private final CacheLIRS<K, V> cache;

    LirsCacheAdapter(CacheLIRS<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public V getIfPresent(@NotNull K key) {
        return cache.getIfPresent(key);
    }

    @Override
    public V get(@NotNull K key, @NotNull Function<? super K, ? extends V> mappingFunction) {
        try {
            return cache.get(key, () -> mappingFunction.apply(key));
        } catch (ExecutionException e) {
            throw toCaffeineException(e);
        }
    }

    @Override
    public void put(@NotNull K key, @NotNull V value) {
        cache.put(key, value);
    }

    @Override
    public void invalidate(@NotNull K key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public void invalidateAll(@NotNull Iterable<? extends K> keys) {
        cache.invalidateAll(keys);
    }

    @Override
    public long estimatedSize() {
        return cache.size();
    }

    @Override
    @NotNull
    public CacheStatsSnapshot stats() {
        org.apache.jackrabbit.guava.common.cache.CacheStats s = cache.stats();
        return new CacheStatsSnapshot(
                s.hitCount(), s.missCount(),
                s.loadSuccessCount(), s.loadExceptionCount(),
                s.totalLoadTime(), s.evictionCount());
    }

    @Override
    @NotNull
    public ConcurrentMap<K, V> asMap() {
        return cache.asMap();
    }

    @Override
    @NotNull
    public Map<K, V> getAllPresent(@NotNull Iterable<? extends K> keys) {
        return cache.getAllPresent(keys);
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

    @Override
    public long getUsedWeight() {
        return cache.getUsedMemory();
    }

    @Override
    public void setMaximumWeight(long maximumWeight) {
        cache.setMaxMemory(maximumWeight);
    }

    /**
     * Maps a Guava shim {@code RemovalCause} to the Oak-neutral {@link EvictionCause}.
     */
    public static EvictionCause toOakCause(RemovalCause cause) {
        return CacheLIRS.toOakCause(cause);
    }

    static RuntimeException toCaffeineException(ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new CompletionException(cause == null ? e : cause);
    }
}
