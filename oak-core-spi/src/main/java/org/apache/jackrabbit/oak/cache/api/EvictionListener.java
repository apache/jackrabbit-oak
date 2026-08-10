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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Callback invoked when an entry is removed from the cache.
 *
 * <p>Register via {@link CacheBuilder#evictionListener(EvictionListener)}.</p>
 *
 * <p>The callback runs on the cache's maintenance executor, not on the thread that triggered the
 * removal, so it may lag behind the triggering operation and may observe a key that has already
 * been re-inserted. Listeners that maintain external accounting must therefore be written to
 * tolerate reordering against concurrent writes, or drain pending work with
 * {@link Cache#cleanUp()} before reading that accounting. Removals are still reported exactly
 * once per removed mapping.</p>
 *
 * <p><b>Warning:</b> it is unsafe to call cache methods from within the listener.
 * Some implementations hold internal locks during the callback.</p>
 *
 * @param <K> the type of cache keys
 * @param <V> the type of cache values
 */
@FunctionalInterface
public interface EvictionListener<K, V> {

    /**
     * Notifies the listener that an entry was removed.
     *
     * @param key   the key of the removed entry (never null)
     * @param value the value of the removed entry (may be null if collected)
     * @param cause the reason the entry was removed (never null)
     */
    void onEviction(@NotNull K key, @Nullable V value, @NotNull EvictionCause cause);
}
