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
package org.apache.jackrabbit.oak.cache.impl.caffeine;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.jackrabbit.oak.cache.CacheStatsSnapshot;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.jetbrains.annotations.NotNull;

/**
 * {@link Cache} adapter wrapping a Caffeine {@link com.github.benmanes.caffeine.cache.Cache}.
 */
public class CaffeineCacheAdapter<K, V> implements Cache<K, V> {

    private final com.github.benmanes.caffeine.cache.Cache<K, V> cache;

    public CaffeineCacheAdapter(com.github.benmanes.caffeine.cache.Cache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public V getIfPresent(@NotNull K key) {
        return cache.getIfPresent(key);
    }

    @Override
    public V get(@NotNull K key, @NotNull Function<? super K, ? extends V> mappingFunction) {
        return cache.get(key, mappingFunction);
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
        return cache.estimatedSize();
    }

    @Override
    @NotNull
    public CacheStatsSnapshot stats() {
        com.github.benmanes.caffeine.cache.stats.CacheStats s = cache.stats();
        return new CacheStatsSnapshot(
                s.hitCount(), s.missCount(),
                s.loadSuccessCount(), s.loadFailureCount(),
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
        return cache.policy().eviction()
                .map(eviction -> eviction.weightedSize().orElse(cache.estimatedSize()))
                .orElse(cache.estimatedSize());
    }

    @Override
    public void setMaximumWeight(long maximumWeight) {
        cache.policy().eviction().ifPresent(eviction -> eviction.setMaximum(maximumWeight));
    }


    /**
     * Maps a Caffeine {@code RemovalCause} to the Oak-neutral {@link EvictionCause}.
     */
    public static EvictionCause toOakCause(RemovalCause cause) {
        return switch (cause) {
            case EXPLICIT   -> EvictionCause.EXPLICIT;
            case REPLACED   -> EvictionCause.REPLACED;
            case SIZE       -> EvictionCause.SIZE;
            case EXPIRED    -> EvictionCause.EXPIRED;
            case COLLECTED  -> EvictionCause.COLLECTED;
        };
    }

}
