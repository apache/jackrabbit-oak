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
package org.apache.jackrabbit.oak.cache.api.impl.caffeine;

import org.apache.jackrabbit.oak.cache.api.LoadingCache;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutionException; /**
 * {@link LoadingCache} adapter wrapping a Caffeine {@link com.github.benmanes.caffeine.cache.LoadingCache}.
 *
 * <p>TODO OAK-TASK16: per {@code TASKS.md}, remove this temporary bridge in
 * TASK-16 once the migration cleanup drops the Oak-visible loading-cache
 * compatibility layer.</p>
 */
public class CaffeineLoadingCacheAdapter<K, V> extends CaffeineCacheAdapter<K, V> implements LoadingCache<K, V> {

    private final com.github.benmanes.caffeine.cache.LoadingCache<K, V> loadingCache;

    public CaffeineLoadingCacheAdapter(com.github.benmanes.caffeine.cache.LoadingCache<K, V> loadingCache) {
        super(loadingCache);
        this.loadingCache = loadingCache;
    }

    @Override
    @NotNull
    public V get(@NotNull K key) throws ExecutionException {
        try {
            return loadingCache.get(key);
        } catch (CacheComputationException e) {
            throw new ExecutionException(e.getCause());
        } catch (RuntimeException e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public void refresh(@NotNull K key) {
        loadingCache.refresh(key);
    }
}
