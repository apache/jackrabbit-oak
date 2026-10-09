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

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.function.BooleanSupplier;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;

/**
 * Writes an ASYNC cache entry only while its persistence eligibility remains valid.
 *
 * @param <K> key type
 * @param <V> value type
 */
class GuardedPutToCacheAction<K extends CacheValue, V extends CacheValue>
        implements CacheAction {

    private final PersistentCache cache;

    private final Map<K, V> map;

    private final K key;

    private final V value;
    private final BooleanSupplier valid;
    private final Lock writeOrder;
    private final Runnable written;

    GuardedPutToCacheAction(K key, V value, CacheWriteQueue<K, V> queue,
                     BooleanSupplier valid, Lock writeOrder, Runnable written) {
        this.key = key;
        this.value = value;
        this.cache = queue.getCache();
        this.map = queue.getMap();
        this.valid = valid;
        this.writeOrder = writeOrder;
        this.written = written;
    }

    @Override
    public void execute() {
        writeOrder.lock();
        try {
            if (map != null && valid.getAsBoolean()) {
                cache.switchGenerationIfNeeded();
                map.put(key, value);
                written.run();
            }
        } finally {
            writeOrder.unlock();
        }
    }

    @Override
    public int getMemory() {
        long mem = key.getMemory();
        mem += value.getMemory();
        return (int) Math.min(Integer.MAX_VALUE, mem);
    }

    @Override
    public String toString() {
        return new StringBuilder("GuardedPutToCacheAction[").append(key).append(']').toString();
    }
}
