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

import org.apache.jackrabbit.oak.cache.api.impl.CacheBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * Computes or loads a value for a missing cache entry.
 *
 * <p>Used with {@link CacheBuilder#build(CacheLoader)} to create an
 * {@link LoadingCache}. The loader is key-aware (receives the lookup key)
 * and may throw a checked exception.</p>
 *
 * @param <K> the type of cache keys
 * @param <V> the type of cache values
 */
@FunctionalInterface
public interface CacheLoader<K, V> {

    /**
     * Computes the value for the given key.
     *
     * @param key the key whose value should be loaded (never null)
     * @return the loaded value (never null)
     * @throws Exception if the value cannot be loaded
     */
    @NotNull
    V load(@NotNull K key) throws Exception;
}
