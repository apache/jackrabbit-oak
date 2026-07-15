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

import java.util.concurrent.CompletableFuture;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.api.LoadingCache;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.ExecutionException;

/**
 * {@link LoadingCache} adapter wrapping a loading {@link CacheLIRS} instance.
 */
public class LirsLoadingCacheAdapter<K, V> extends LirsCacheAdapter<K, V> implements LoadingCache<K, V> {

    private final CacheLIRS<K, V> cache;

    public LirsLoadingCacheAdapter(CacheLIRS<K, V> cache) {
        super(cache);
        this.cache = cache;
    }

    @Override
    @Nullable
    public V get(@NotNull K key) {
        try {
            return cache.get(key);
        } catch (ExecutionException e) {
            throw toCaffeineException(e);
        }
    }

    /**
     * Triggers a synchronous refresh of the entry for {@code key} and returns a
     * completed future holding the refreshed value.
     *
     * <p>Unlike Caffeine's asynchronous refresh, {@link CacheLIRS#refresh(Object)}
     * is fully synchronous: it loads the new value inside a {@code synchronized}
     * segment lock and puts it back before returning. By the time this method
     * calls {@code getIfPresent}, the cache already contains the updated value.
     * The returned future is therefore always already completed — callers do not
     * need to wait for any background work.</p>
     *
     * <p>If the refresh loader throws, {@code CacheLIRS} logs a warning and keeps
     * the old value in place. In that case {@code getIfPresent} may return the
     * previous value or {@code null} (if the key was absent); the future completes
     * with that result.</p>
     *
     * @param key the key whose value should be refreshed (must not be null)
     * @return a completed future holding the refreshed value, or {@code null} if
     *         the entry was absent or was evicted immediately after the refresh
     */
    @Override
    @NotNull
    public CompletableFuture<V> refresh(@NotNull K key) {
        // CacheLIRS.refresh() is synchronous: the new value is put into the cache
        // inside a synchronized segment lock before this call returns.
        cache.refresh(key);
        return CompletableFuture.completedFuture(cache.getIfPresent(key));
    }
}
