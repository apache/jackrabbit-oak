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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

/**
 * A cache that automatically loads absent entries from a pre-configured
 * {@link CacheLoader}.
 *
 * <p>Obtain instances via {@link CacheBuilder#build(CacheLoader)}.
 * Loading failures follow Caffeine's contract: runtime exceptions are
 * propagated directly and checked loader failures are wrapped in
 * {@link CompletionException}.</p>
 *
 * @param <K> the type of cache keys
 * @param <V> the type of cache values
 */
@ProviderType
public interface LoadingCache<K, V> extends Cache<K, V> {

    /**
     * Returns the value associated with {@code key}, loading it via the
     * pre-configured {@link CacheLoader} if absent.
     *
     * @param key the key whose value should be returned or loaded (must not be null)
     * @return the current or newly loaded value (could be null)
     */
    @Nullable
    V get(@NotNull K key);

    /**
     * Triggers a reload of the value for {@code key}. The stale value remains
     * available until the reload completes. The returned future follows
     * Caffeine's refresh contract; the CacheLIRS implementation completes it
     * after its best-effort synchronous refresh path runs.
     *
     * @param key the key whose value should be refreshed (must not be null)
     * @return a future representing the refresh work (never null)
     */
    @NotNull
    CompletableFuture<V> refresh(@NotNull K key);
}
