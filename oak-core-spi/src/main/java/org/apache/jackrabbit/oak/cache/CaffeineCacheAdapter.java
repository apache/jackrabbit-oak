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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.jspecify.annotations.NonNull;

/**
 * {@link OakCache} adapter wrapping a Caffeine {@link Cache}.
 */
class CaffeineCacheAdapter<K, V> implements OakCache<K, V> {

    private final Cache<K, V> cache;

    CaffeineCacheAdapter(Cache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public V getIfPresent(@NonNull K key) {
        return cache.getIfPresent(key);
    }

    @Override
    public V get(@NonNull K key, @NonNull Callable<? extends V> valueLoader) throws ExecutionException {
        try {
            return cache.get(key, k -> callUnchecked(valueLoader));
        } catch (CacheComputationException e) {
            throw new ExecutionException(e.getCause());
        } catch (RuntimeException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public void put(@NonNull K key, @NonNull V value) {
        cache.put(key, value);
    }

    @Override
    public void invalidate(@NonNull K key) {
        cache.invalidate(key);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
    }

    @Override
    public void invalidateAll(@NonNull Iterable<? extends K> keys) {
        cache.invalidateAll(keys);
    }

    @Override
    public long estimatedSize() {
        return cache.estimatedSize();
    }

    @Override
    @NonNull
    public OakCacheStats stats() {
        CacheStats s = cache.stats();
        return new OakCacheStats(
                s.hitCount(), s.missCount(),
                s.loadSuccessCount(), s.loadFailureCount(),
                s.totalLoadTime(), s.evictionCount());
    }

    @Override
    @NonNull
    public ConcurrentMap<K, V> asMap() {
        return cache.asMap();
    }

    @Override
    @NonNull
    public Map<K, V> getAllPresent(@NonNull Iterable<? extends K> keys) {
        return cache.getAllPresent(keys);
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

    /**
     * Maps a Caffeine {@code RemovalCause} to the Oak-neutral {@link OakRemovalCause}.
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

    private static <V> V callUnchecked(Callable<? extends V> valueLoader) {
        try {
            return valueLoader.call();
        } catch (Exception e) {
            throw new CacheComputationException(e);
        }
    }
}

/**
 * {@link OakLoadingCache} adapter wrapping a Caffeine {@link LoadingCache}.
 *
 * <p>TODO OAK-TASK16: per {@code TASKS.md}, remove this temporary bridge in
 * TASK-16 once the migration cleanup drops the Oak-visible loading-cache
 * compatibility layer.</p>
 */
class CaffeineLoadingCacheAdapter<K, V> extends CaffeineCacheAdapter<K, V> implements OakLoadingCache<K, V> {

    private final LoadingCache<K, V> loadingCache;

    CaffeineLoadingCacheAdapter(LoadingCache<K, V> loadingCache) {
        super(loadingCache);
        this.loadingCache = loadingCache;
    }

    @Override
    @NonNull
    public V get(@NonNull K key) throws ExecutionException {
        try {
            return loadingCache.get(key);
        } catch (CacheComputationException e) {
            throw new ExecutionException(e.getCause());
        } catch (RuntimeException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public void refresh(@NonNull K key) {
        loadingCache.refresh(key);
    }
}

/**
 * Internal wrapper used to tunnel checked loader failures through Caffeine's
 * unchecked loader callbacks before restoring them as {@link ExecutionException}
 * on the Oak-visible API surface.
 *
 * <p>TODO OAK-TASK16: per {@code TASKS.md}, remove this helper in TASK-16 once
 * checked-exception compatibility is no longer required on top of Caffeine.</p>
 */
class CacheComputationException extends RuntimeException {

    CacheComputationException(Throwable cause) {
        super(cause);
    }
}
