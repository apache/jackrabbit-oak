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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * In order to avoid leaking values from the metadataMap, following order should
 * be maintained for combining the cache and CacheMetadata:
 *
 * 1. For remove(), removeAll() and clear():
 *
 * - cache.invalidate()
 * - metadata.remove()
 *
 * 2. For put(), putAll() and putFromPersistenceAndIncrement():
 *
 * - metadata.put()
 * - cache.put()
 *
 * 3. For incrementIfPresent():
 *
 * - cache.get()
 * - metadata.incrementIfPresent()
 *
 * 4. For bulk lookup:
 *
 * - cache.getAll()
 * - metadata.incrementIfPresent() for returned values
 *
 * Preserving this order will allow to avoid leaked values in the metadata without
 * an extra synchronization between cache and metadata operations. This strategy
 * is a best-effort option - it may happen that cache values won't have their
 * metadata entries.
 */
public class CacheMetadata<K, V> {

    private final ConcurrentMap<K, MetadataEntry> metadataMap = new ConcurrentHashMap<>();

    private boolean enabled = true;

    boolean isEnabled() {
        return enabled;
    }

    void disable() {
        this.enabled = false;
    }

    void put(K key, V value) {
        if (!enabled) {
            return;
        }
        getOrCreate(key, value, false);
    }

    void putFromPersistenceAndIncrement(K key, V value) {
        if (!enabled) {
            return;
        }
        getOrCreate(key, value, true).incrementCount();
    }

    void increment(K key, V value) {
        if (!enabled) {
            return;
        }
        getOrCreate(key, value, false).incrementCount();
    }

    void incrementIfPresent(K key, V value) {
        if (!enabled) {
            return;
        }
        metadataMap.computeIfPresent(key, (k, metadata) -> {
            if (metadata.isFor(value)) {
                metadata.incrementCount();
            }
            return metadata;
        });
    }

    MetadataEntry remove(Object key) {
        if (!enabled) {
            return null;
        }
        return metadataMap.remove(key);
    }

    MetadataEntry remove(K key, V value) {
        if (!enabled) {
            return null;
        }
        MetadataEntry metadata = metadataMap.get(key);
        if (metadata != null && metadata.isFor(value) && metadataMap.remove(key, metadata)) {
            return metadata;
        }
        return null;
    }

    void removeAll(Iterable<?> keys) {
        if (!enabled) {
            return;
        }
        for (Object k : keys) {
            metadataMap.remove(k);
        }
    }

    void clear() {
        if (!enabled) {
            return;
        }
        metadataMap.clear();
    }

    private MetadataEntry getOrCreate(K key, V value, boolean readFromPersistentCache) {
        if (!enabled) {
            return null;
        }
        return metadataMap.compute(key, (k, metadata) ->
                metadata != null && metadata.isFor(value)
                        ? metadata
                        : new MetadataEntry(value, readFromPersistentCache));
    }


    static class MetadataEntry {

        private final AtomicLong accessCount = new AtomicLong();

        private final Object value;

        private final boolean readFromPersistentCache;

        private MetadataEntry(Object value, boolean readFromPersistentCache) {
            this.value = value;
            this.readFromPersistentCache = readFromPersistentCache;
        }

        boolean isFor(Object value) {
            return this.value == value;
        }

        void incrementCount() {
            accessCount.incrementAndGet();
        }

        long getAccessCount() {
            return accessCount.get();
        }

        boolean isReadFromPersistentCache() {
            return readFromPersistentCache;
        }
    }

}
