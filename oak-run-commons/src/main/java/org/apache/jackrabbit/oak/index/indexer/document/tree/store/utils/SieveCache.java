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
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Sieve cache. It is faster and has a higher hit-rate than LRU.
 * See https://cachemon.github.io/SIEVE-website/
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class SieveCache<K, V extends MemoryObject> {

    private final ConcurrentHashMap<K, Value<K, V>> map = new ConcurrentHashMap<>();
    private final ConcurrentLinkedDeque<Value<K, V>> queue = new ConcurrentLinkedDeque<>();
    private final AtomicReference<Iterator<Value<K, V>>> hand = new AtomicReference<>();

    private volatile long maxMemoryBytes;
    private AtomicLong memoryUsed = new AtomicLong();

    public SieveCache(long maxMemoryBytes) {
        this.maxMemoryBytes = maxMemoryBytes;
        Iterator<Value<K, V>> it = queue.iterator();
        hand.set(it);
    }

    public void setSize(int maxMemoryBytes) {
        this.maxMemoryBytes = maxMemoryBytes;
    }

    public V get(K key) {
        Value<K, V> v = map.get(key);
        if (v == null) {
            return null;
        }
        v.visited = true;
        return v.value;
    }

    public Set<K> keys() {
        return new HashSet<>(map.keySet());
    }

    public V put(K key, V value) {
        Value<K, V> v = new Value<>(key, value);
        memoryUsed.addAndGet(value.estimatedMemory());
        Value<K, V> old = map.put(key, v);
        V result = null;
        if (old == null) {
            queue.add(v);
        } else {
            memoryUsed.addAndGet(-old.value.estimatedMemory());
            result = old.value;
        }
        while (memoryUsed.get() > maxMemoryBytes) {
            Value<K, V> evict;
            synchronized (hand) {
                if (memoryUsed.get() < maxMemoryBytes || map.isEmpty()) {
                    break;
                }
                Iterator<Value<K, V>> it = hand.get();
                while (true) {
                    if (it.hasNext()) {
                        evict = it.next();
                        if (!evict.visited) {
                            break;
                        }
                        evict.visited = false;
                    } else {
                        Iterator<Value<K, V>> it2 = queue.iterator();
                        it = hand.compareAndExchange(it, it2);
                    }
                }
                it.remove();
                evict = map.remove(evict.key);
            }
            if (evict != null) {
                memoryUsed.addAndGet(-evict.value.estimatedMemory());
                entryWasRemoved(evict.key, evict.value);
            }
        }
        return result;
    }

    public void entryWasRemoved(K key, V value) {
        // nothing to do
    }

    public String toString() {
        return "cache entries:" + map.size() +
                " queue:" + queue.size() +
                " max:" + maxMemoryBytes +
                " used:" + memoryUsed;
    }

    public int size() {
        return map.size();
    }

    private static class Value<K, V> {
        private final K key;
        private final V value;
        volatile boolean visited;

        public Value(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "(" + key + ":" + value + ")";
        }
    }

}
