/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.commons.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Cache<K, V extends Cache.Value> {

    int maxMemoryBytes;
    AtomicInteger memoryUsed = new AtomicInteger();
    private final Backend<K, V> backend;

    private final Map<K, V> map = new LinkedHashMap<K, V>(1024, 0.75f, true) {

        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            if (memoryUsed.get() < maxMemoryBytes) {
                return false;
            }
            int memory = eldest.getValue().getMemory();
            memoryUsed.addAndGet(-memory);
            return true;
        }

    };

    private Cache(Backend<K, V> backend, int maxMemoryBytes) {
        this.backend = backend;
        this.maxMemoryBytes = maxMemoryBytes;
    }

    public void put(K key, V value) {
        int memory = value.getMemory();
        // only add elements that are smaller than half the cache size
        if (memory < maxMemoryBytes / 2) {
            memoryUsed.addAndGet(memory);
            synchronized (map) {
                map.put(key, value);
            }
        }
    }

    /**
     * Get the element in the cache if one exists, or add it to the cache if not.
     *
     * @param key the key
     * @param value the value
     * @return the cached element
     */
    public V replace(K key, V value) {
        synchronized (map) {
            if (map.containsKey(key)) {
                return map.get(key);
            }
        }
        put(key, value);
        return value;
    }

    public V get(K key) {
        synchronized (map) {
            V value = map.get(key);
            if (value != null) {
                // object was in the cache - good
                return value;
            }
        }
        // synchronize on the backend when not in the cache
        // to ensure only one thread accessed the backend
        // and loads the object
        synchronized (backend) {
            // another thread might have added it in the meantime
            V value;
            synchronized (map) {
                value = map.get(key);
            }
            if (value == null) {
                value = backend.load(key);
                put(key, value);
            }
            return value;
        }
    }

    /**
     * A cacheable object.
     */
    public interface Value {

        /**
         * Get the memory required in bytes. The method must always return the
         * same value once the element is in the cache.
         *
         * @return the memory used in bytes
         */
        int getMemory();

    }

    /**
     * A cache backend that can load objects from persistent storage.
     *
     * @param <K> the key class
     * @param <V> the value class
     */
    public interface Backend<K, V> {

        /**
         * Load the object. The method does not need to be synchronized
         * (it is synchronized in the cache)
         *
         * @param key the key
         * @return the value
         */
        V load(K key);

    }

    public static <K, V extends Cache.Value> Cache<K, V> newInstance(Backend<K, V> backend, int maxMemoryBytes) {
        return new Cache<K, V>(backend, maxMemoryBytes);
    }

    public void clear() {
        synchronized (map) {
            map.clear();
        }
    }

    public int size() {
        return map.size();
    }

    public int getMemoryUsed() {
        return memoryUsed.get();
    }

    public int getMemoryMax() {
        return maxMemoryBytes;
    }

}
