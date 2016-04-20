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

package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Sets.newHashSet;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FIXME OAK-3348 XXX document
 */
public class RecordCache<T> {
    private static final Logger LOG = LoggerFactory.getLogger(RecordCache.class);
    private static final int RETENTION_THRESHOLD = 1;

    private final ConcurrentMap<Integer, Cache<T>> generations = newConcurrentMap();

    public abstract static class Cache<T> {
        public static <T> Cache<T> disabled() {
            return new Cache<T>() {
                @Override void put(T key, RecordId value) { }
                @Override void put(T key, RecordId value, int cost) { }
                @Override RecordId get(T key) { return null; }
                @Override void clear() { }
            };
        }
        abstract void put(T key, RecordId value);
        abstract void put(T key, RecordId value, int cost);
        abstract RecordId get(T key);
        abstract void clear();
    }

    public static <T> RecordCache<T> disabled() {
        return new RecordCache<T>() {
            @Override public Cache<T> generation(int generation) { return Cache.disabled(); }
            @Override public void clear(int generation) { }
            @Override public void clear() { }
        };
    }

    /**
     */
    protected Cache<T> getCache(int generation) {
        return Cache.disabled();
    }

    public Cache<T> generation(int generation) {
        // Avoid calling getCache on every invocation.
        if (!generations.containsKey(generation)) {
            // The small race here might still result in getCache being called
            // more than once. Implementors much take this into account.
            generations.putIfAbsent(generation, getCache(generation));
        }
        return generations.get(generation);
    }

    public void put(Cache<T> cache, int generation) {
        generations.put(generation, cache);
    }

    public void clearUpTo(int maxGen) {
        Iterator<Integer> it = generations.keySet().iterator();
        while (it.hasNext()) {
            Integer gen =  it.next();
            if (gen <= maxGen) {
                it.remove();
            }
        }
    }

    public void clear(int generation) {
        generations.remove(generation);
    }

    public void clear() {
        generations.clear();
    }

    public static final class LRUCache<T> extends Cache<T> {
        private final Map<T, RecordId> map;

        public LRUCache(final int size) {
            map = new LinkedHashMap<T, RecordId>(size * 4 / 3, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<T, RecordId> eldest) {
                    return size() >= size;
                }
            };
        }

        @Override
        public synchronized void put(T key, RecordId value) {
            map.put(key, value);
        }

        @Override
        public void put(T key, RecordId value, int cost) {
            throw new UnsupportedOperationException("Cannot put with a cost");
        }

        @Override
        public synchronized RecordId get(T key) {
            return map.get(key);
        }

        @Override
        public synchronized void clear() {
            map.clear();
        }
    }

    public static final class DeduplicationCache<T> extends Cache<T> {
        private final int capacity;
        private final List<Map<T, RecordId>> maps;

        private int size;

        private final Set<Integer> muteDepths = newHashSet();

        public DeduplicationCache(int capacity, int maxDepth) {
            checkArgument(capacity > 0);
            checkArgument(maxDepth > 0);
            this.capacity = capacity;
            this.maps = newArrayList();
            for (int k = 0; k < maxDepth; k++) {
                maps.add(new HashMap<T, RecordId>());
            }
        }

        @Override
        public void put(T key, RecordId value) {
            throw new UnsupportedOperationException("Cannot put without a cost");
        }

        @Override
        public synchronized void put(T key, RecordId value, int cost) {
            while (size >= capacity) {
                int d = maps.size() - 1;
                int removed = maps.remove(d).size();
                size -= removed;
                if (removed > 0) {
                    LOG.info("Evicted cache at depth {} as size {} reached capacity {}. " +
                        "New size is {}", d, size + removed, capacity, size);
                }
            }

            if (cost < maps.size()) {
                if (maps.get(cost).put(key, value) == null) {
                    size++;
                }
            } else {
                if (muteDepths.add(cost)) {
                    LOG.info("Not caching {} -> {} as depth {} reaches or exceeds the maximum of {}",
                        key, value, cost, maps.size());
                }
            }
        }

        @Override
        public synchronized RecordId get(T key) {
            for (Map<T, RecordId> map : maps) {
                if (!map.isEmpty()) {
                    RecordId recordId = map.get(key);
                    if (recordId != null) {
                        return recordId;
                    }
                }
            }
            return null;
        }

        @Override
        public synchronized void clear() {
            maps.clear();
        }

    }
}
