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
package org.apache.jackrabbit.oak.cache;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.jackrabbit.guava.common.cache.CacheStats;
import org.apache.jackrabbit.guava.common.cache.RemovalCause;
import org.jetbrains.annotations.NotNull;

/**
 * {@link OakCache} adapter wrapping a {@link CacheLIRS} instance.
 *
 * <p>Exposes the checked {@link ExecutionException} contract used by the
 * legacy Oak-visible cache API.</p>
 */
class LirsCacheAdapter<K, V> implements OakCache<K, V> {

    private final CacheLIRS<K, V> cache;

    LirsCacheAdapter(CacheLIRS<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public V getIfPresent(@NotNull K key) {
        return cache.getIfPresent(key);
    }

    @Override
    public V get(@NotNull K key, @NotNull Callable<? extends V> valueLoader) throws ExecutionException {
        return cache.get(key, valueLoader);
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
    public OakCacheStats stats() {
        CacheStats s = cache.stats();
        return new OakCacheStats(
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

    /**
     * Maps a Guava shim {@code RemovalCause} to the Oak-neutral {@link OakRemovalCause}.
     */
    static OakRemovalCause toOakCause(RemovalCause cause) {
        return switch (cause) {
            case EXPLICIT   -> OakRemovalCause.EXPLICIT;
            case REPLACED   -> OakRemovalCause.REPLACED;
            case SIZE       -> OakRemovalCause.SIZE;
            case EXPIRED    -> OakRemovalCause.EXPIRED;
            case COLLECTED  -> OakRemovalCause.COLLECTED;
        };
    }
}

/**
 * {@link OakLoadingCache} adapter wrapping a loading {@link CacheLIRS} instance.
 *
 * <p>TODO OAK-TASK16: per {@code TASKS.md}, remove this temporary bridge in
 * TASK-16 once loading-cache callers have been migrated off the legacy
 * compatibility contract.</p>
 */
class LirsLoadingCacheAdapter<K, V> extends LirsCacheAdapter<K, V> implements OakLoadingCache<K, V> {

    private final CacheLIRS<K, V> cache;

    LirsLoadingCacheAdapter(CacheLIRS<K, V> cache) {
        super(cache);
        this.cache = cache;
    }

    @Override
    public @NotNull V get(@NotNull K key) throws ExecutionException {
        return cache.get(key);
    }

    @Override
    public void refresh(@NotNull K key) {
        cache.refresh(key);
    }
}
