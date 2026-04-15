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

import java.util.concurrent.CompletableFuture;

import org.apache.jackrabbit.oak.cache.api.LoadingCache;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link LoadingCache} adapter wrapping a Caffeine {@link com.github.benmanes.caffeine.cache.LoadingCache}.
 */
public class CaffeineLoadingCacheAdapter<K, V> extends CaffeineCacheAdapter<K, V> implements LoadingCache<K, V> {

    private final com.github.benmanes.caffeine.cache.LoadingCache<K, V> loadingCache;

    public CaffeineLoadingCacheAdapter(com.github.benmanes.caffeine.cache.LoadingCache<K, V> loadingCache) {
        super(loadingCache);
        this.loadingCache = loadingCache;
    }

    @Override
    @Nullable
    public V get(@NotNull K key) {
        return loadingCache.get(key);
    }

    @Override
    @NotNull
    public CompletableFuture<V> refresh(@NotNull K key) {
        return loadingCache.refresh(key);
    }
}
