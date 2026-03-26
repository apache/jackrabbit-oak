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

import org.jetbrains.annotations.NotNull;

/**
 * Determines the weight of a cache entry.
 *
 * <p>Used with {@code OakCacheBuilder.weigher(OakWeigher)} in combination with
 * {@code OakCacheBuilder.maximumWeight(long)} to create weight-bounded caches.
 * The unit is typically bytes but is cache-specific. The returned weight must
 * be non-negative.</p>
 *
 * <!-- TODO OAK-TASK2: restore {@link OakCacheBuilder#weigher(OakWeigher)} and
 *      {@link OakCacheBuilder#maximumWeight(long)} once TASK-2 is merged. -->
 *
 * @param <K> the type of cache keys
 * @param <V> the type of cache values
 */
@FunctionalInterface
public interface OakWeigher<K, V> {

    /**
     * Returns the weight of the given cache entry.
     *
     * @param key   the cache key (never null)
     * @param value the cache value (never null)
     * @return the weight of the entry; must be non-negative
     */
    int weigh(@NotNull K key, @NotNull V value);
}
