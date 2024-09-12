/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A synchronized LRU cache. The cache size is limited by the amount of memory
 * (and not number of entries).
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class ConcurrentLRUCache<K, V extends MemoryObject>
        extends LinkedHashMap<K, V> {

    private static final long serialVersionUID = 1L;
    private volatile long maxMemoryBytes;
    private AtomicLong memoryUsed = new AtomicLong();

    public ConcurrentLRUCache(long maxMemoryBytes) {
        super(16, 0.75f, true);
        this.maxMemoryBytes = maxMemoryBytes;
    }

    public String toString() {
        return "cache entries:" + size() + " max:" + maxMemoryBytes + " used:" + memoryUsed;
    }

    public void setSize(int maxMemoryBytes) {
        this.maxMemoryBytes = maxMemoryBytes;
    }

    @Override
    public synchronized V get(Object key) {
        return super.get(key);
    }

    public synchronized Set<K> keys() {
        return new HashSet<>(super.keySet());
    }

    @Override
    public synchronized V put(K key, V value) {
        V old = super.put(key, value);
        if (old != null) {
            memoryUsed.addAndGet(-old.estimatedMemory());
        }
        if (value != null) {
            memoryUsed.addAndGet(value.estimatedMemory());
        }
        return old;
    }

    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> map) {
        for(Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public synchronized V remove(Object key ) {
        V old = super.remove(key);
        if (old != null) {
            memoryUsed.addAndGet(-old.estimatedMemory());
        }
        return old;
    }

    @Override
    public synchronized void clear() {
        super.clear();
        memoryUsed.set(0);
    }

    public void entryWasRemoved(K key, V value) {
        // nothing to do
    }

    @Override
    public synchronized boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        boolean removeEldest = memoryUsed.get() > maxMemoryBytes;
        if (removeEldest) {
            memoryUsed.addAndGet(-eldest.getValue().estimatedMemory());
            entryWasRemoved(eldest.getKey(), eldest.getValue());
        }
        return removeEldest;
    }

}
