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
package org.apache.jackrabbit.oak.plugins.document.persistentCache.async;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;

public class CacheWriteQueue<K extends CacheValue, V extends CacheValue> {

    private final CacheActionDispatcher dispatcher;

    private final PersistentCache cache;

    private final Map<K, V> map;

    public CacheWriteQueue(CacheActionDispatcher dispatcher, PersistentCache cache, Map<K, V> map) {
        this.dispatcher = dispatcher;
        this.cache = cache;
        this.map = map;
    }

    public boolean addPut(K key, V value) {
        return dispatcher.add(new PutToCacheAction<K, V>(key, value, this));
    }

    /**
     * Queues a write whose entry can be retired before the dispatcher reaches it.
     * @param key persistent cache key
     * @param value original serialized value
     * @param valid checks whether the entry still qualifies for persistence
     * @param writeOrder serializes the write with explicit invalidation
     * @return whether the bounded dispatcher accepted the write
     */
    public boolean addPut(K key, V value, BooleanSupplier valid, Lock writeOrder) {
        return addPut(key, value, valid, writeOrder, () -> { });
    }

    /**
     * Records a successful write only after generation selection and persistence.
     * @param key persistent cache key
     * @param value original serialized value
     * @param valid checks whether the entry still qualifies for persistence
     * @param writeOrder serializes the write with explicit invalidation
     * @param written runs only after a successful write
     * @return whether the bounded dispatcher accepted the write
     */
    public boolean addPut(K key, V value, BooleanSupplier valid, Lock writeOrder, Runnable written) {
        return dispatcher.add(new GuardedPutToCacheAction<>(key, value, this,
                Objects.requireNonNull(valid), Objects.requireNonNull(writeOrder), Objects.requireNonNull(written)));
    }

    public boolean addInvalidate(Iterable<K> keys) {
        return dispatcher.add(new InvalidateCacheAction<K, V>(keys, this));
    }

    PersistentCache getCache() {
        return cache;
    }

    Map<K, V> getMap() {
        return map;
    }
}
