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

import java.util.concurrent.CompletionException;

import org.jetbrains.annotations.NotNull;
import org.osgi.annotation.versioning.ProviderType;

/**
 * A cache that automatically loads absent entries from a pre-configured
 * {@link OakCacheLoader}.
 *
 * <p>Obtain instances via {@code OakCacheBuilder.build(OakCacheLoader)}.
 * <!-- TODO OAK-TASK2: restore {@link OakCacheBuilder#build(OakCacheLoader)} once TASK-2 is merged. -->
 * Matches Caffeine's {@code LoadingCache} contract: {@link #get(Object)} throws
 * an unchecked {@link CompletionException} on loader failure. Implementations
 * backed by CacheLIRS bridge the checked {@code ExecutionException} by wrapping
 * it into {@code CompletionException}.</p>
 *
 * @param <K> the type of cache keys
 * @param <V> the type of cache values
 */
@ProviderType
public interface OakLoadingCache<K, V> extends OakCache<K, V> {

    /**
     * Returns the value associated with {@code key}, loading it via the
     * pre-configured {@link OakCacheLoader} if absent.
     *
     * @param key the key whose value should be returned or loaded (must not be null)
     * @return the current or newly loaded value (never null)
     * @throws CompletionException wrapping the loader's exception if loading fails,
     *         matching Caffeine's {@code LoadingCache.get(K)} contract
     */
    @NotNull
    V get(@NotNull K key);

    /**
     * Triggers a reload of the value for {@code key}. The stale value remains
     * available until the reload completes (Caffeine's {@code refreshAfterWrite}
     * semantics; best-effort for the CacheLIRS implementation).
     *
     * @param key the key whose value should be refreshed (must not be null)
     */
    void refresh(@NotNull K key);
}
