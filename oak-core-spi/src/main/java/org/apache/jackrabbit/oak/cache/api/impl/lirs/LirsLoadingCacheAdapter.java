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
package org.apache.jackrabbit.oak.cache.api.impl.lirs;

import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.api.LoadingCache;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutionException; /**
 * {@link LoadingCache} adapter wrapping a loading {@link CacheLIRS} instance.
 *
 * <p>TODO OAK-TASK16: per {@code TASKS.md}, remove this temporary bridge in
 * TASK-16 once loading-cache callers have been migrated off the legacy
 * compatibility contract.</p>
 */
public class LirsLoadingCacheAdapter<K, V> extends LirsCacheAdapter<K, V> implements LoadingCache<K, V> {

    private final CacheLIRS<K, V> cache;

    public LirsLoadingCacheAdapter(CacheLIRS<K, V> cache) {
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
