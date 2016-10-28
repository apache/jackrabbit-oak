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

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.Maps.newConcurrentMap;

public class CacheMetadata<K> {

    private final ConcurrentMap<K, MetadataEntry> metadataMap = newConcurrentMap();

    private boolean enabled = true;

    boolean isEnabled() {
        return enabled;
    }

    void disable() {
        this.enabled = false;
    }

    void put(K key) {
        if (!enabled) {
            return;
        }
        getOrCreate(key, false);
    }

    void putFromPersistenceAndIncrement(K key) {
        if (!enabled) {
            return;
        }
        getOrCreate(key, true).incrementCount();
    }

    void increment(K key) {
        if (!enabled) {
            return;
        }
        getOrCreate(key, false).incrementCount();
    }

    MetadataEntry remove(Object key) {
        if (!enabled) {
            return null;
        }
        return metadataMap.remove(key);
    }

    void putAll(Iterable<?> keys) {
        if (!enabled) {
            return;
        }
        for (Object k : keys) {
            getOrCreate((K) k, false);
        }
    }

    void incrementAll(Iterable<?> keys) {
        if (!enabled) {
            return;
        }
        for (Object k : keys) {
            getOrCreate((K) k, false).incrementCount();
        }
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

    private MetadataEntry getOrCreate(K key, boolean readFromPersistentCache) {
        if (!enabled) {
            return null;
        }
        MetadataEntry metadata = metadataMap.get(key);
        if (metadata == null) {
            MetadataEntry newEntry = new MetadataEntry(readFromPersistentCache);
            MetadataEntry oldEntry = metadataMap.putIfAbsent(key, newEntry);
            metadata = oldEntry == null ? newEntry : oldEntry;
        }
        return metadata;
    }


    static class MetadataEntry {

        private final AtomicLong accessCount = new AtomicLong();

        private final boolean readFromPersistentCache;

        private MetadataEntry(boolean readFromPersistentCache) {
            this.readFromPersistentCache = readFromPersistentCache;
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
