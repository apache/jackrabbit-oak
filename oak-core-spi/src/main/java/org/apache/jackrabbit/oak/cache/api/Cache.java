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
package org.apache.jackrabbit.oak.cache.api;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.apache.jackrabbit.oak.cache.api.impl.CacheBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

/**
 * A size-bounded, thread-safe cache.
 *
 * <p>Implementations may use different eviction strategies (LIRS, W-TinyLFU/Caffeine,
 * etc.) but callers see only this interface. Obtain instances via {@link CacheBuilder}.</p>
 *
 * <p>The {@link #get(Object, Callable)} method preserves the legacy Oak-visible
 * cache contract: callers supply a {@link Callable} and loading failures are
 * exposed as {@link ExecutionException}.</p>
 *
 * @param <K> the type of cache keys
 * @param <V> the type of cache values
 */
@ProviderType
public interface Cache<K, V> {

    /**
     * Returns the value associated with {@code key} if it is currently in the
     * cache, otherwise {@code null}.
     *
     * @param key the key to look up (must not be null)
     * @return the cached value, or {@code null} if not present
     */
    @Nullable
    V getIfPresent(@NotNull K key);

    /**
     * Returns the value associated with {@code key}, computing it via
     * {@code valueLoader} and caching the result if it was absent.
     *
     * <p>Preserves the legacy Oak-visible cache contract: failures from the loader
     * are exposed as {@link ExecutionException}.</p>
     *
     * @param key         the key whose associated value is to be returned (must not be null)
     * @param valueLoader the loader used to compute a value if the key is absent (must not be null)
     * @return the current (existing or computed) value, or {@code null} if the
     *         loader returns {@code null}
     * @throws ExecutionException if the value cannot be loaded
     */
    @Nullable
    V get(@NotNull K key, @NotNull Callable<? extends V> valueLoader) throws ExecutionException;

    /**
     * Associates {@code value} with {@code key} in the cache. If the cache
     * previously contained a value for {@code key} it is replaced.
     *
     * @param key   the key (must not be null)
     * @param value the value (must not be null)
     */
    void put(@NotNull K key, @NotNull V value);

    /**
     * Discards any cached value for {@code key}.
     *
     * @param key the key to invalidate (must not be null)
     */
    void invalidate(@NotNull K key);

    /**
     * Discards all entries in the cache.
     */
    void invalidateAll();

    /**
     * Discards any cached values for the given keys.
     *
     * <p><em>Note: no Oak module currently calls this method. It may be removed
     * from the interface in a future release if it remains unused.</em></p>
     *
     * @param keys the keys to invalidate (must not be null)
     */
    void invalidateAll(@NotNull Iterable<? extends K> keys);

    /**
     * Returns the approximate number of entries in the cache.
     *
     * <p><em>Note: no Oak module currently calls this method directly;
     * {@code asMap().size()} is used instead. It may be removed from the
     * interface in a future release if it remains unused.</em></p>
     *
     * @return the approximate entry count
     */
    long estimatedSize();

    /**
     * Returns a snapshot of this cache's cumulative statistics. If statistics
     * collection was not enabled via {@link CacheBuilder#recordStats()}, all
     * counters will be zero.
     *
     * @return a stats snapshot (never null)
     */
    @NotNull
    CacheStats stats();

    /**
     * Returns a view of the entries stored in this cache as a thread-safe map.
     * Modifications to the map directly affect the cache.
     *
     * @return a live concurrent map view of the cache entries (never null)
     */
    @NotNull
    ConcurrentMap<K, V> asMap();

    /**
     * Returns a map of the values currently present in the cache for the given
     * keys. The returned map contains only keys that were present at the time
     * of the call.
     *
     * <p><em>Note: no Oak module currently calls this method (CacheLIRS throws
     * {@code UnsupportedOperationException} for it). It may be removed from the
     * interface in a future release if it remains unused.</em></p>
     *
     * @param keys the keys to look up (must not be null)
     * @return a map of keys to cached values for those keys that were present (never null)
     */
    @NotNull
    Map<K, V> getAllPresent(@NotNull Iterable<? extends K> keys);

    /**
     * Performs any pending maintenance operations needed by the cache.
     *
     * <p><em>Note: no Oak module currently calls this method; the CacheLIRS
     * implementation is a no-op. It may be removed from the interface in a
     * future release if it remains unused.</em></p>
     */
    void cleanUp();
}
