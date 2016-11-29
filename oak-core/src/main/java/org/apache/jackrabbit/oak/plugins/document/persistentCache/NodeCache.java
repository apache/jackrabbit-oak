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

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.cache.RemovalCause.COLLECTED;
import static com.google.common.cache.RemovalCause.EXPIRED;
import static com.google.common.cache.RemovalCause.SIZE;
import static com.google.common.collect.Iterables.filter;
import static java.util.Collections.singleton;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache.GenerationCache;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.async.CacheActionDispatcher;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.async.CacheWriteQueue;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.type.DataType;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NodeCache<K, V> implements Cache<K, V>, GenerationCache, EvictionListener<K, V> {

    static final Logger LOG = LoggerFactory.getLogger(NodeCache.class);

    private static final Set<RemovalCause> EVICTION_CAUSES = ImmutableSet.of(COLLECTED, EXPIRED, SIZE);

    private final PersistentCache cache;
    private final Cache<K, V> memCache;
    private final MultiGenerationMap<K, V> map;
    private final CacheType type;
    private final DataType keyType;
    private final DataType valueType;
    private final CacheMetadata<K> memCacheMetadata;
    private final boolean async;
    CacheWriteQueue<K, V> writeQueue;

    NodeCache(
            PersistentCache cache,
            Cache<K, V> memCache,
            DocumentNodeStore docNodeStore,
            DocumentStore docStore,
            CacheType type,
            CacheActionDispatcher dispatcher,
            boolean async) {
        this.cache = cache;
        this.memCache = memCache;
        this.type = type;
        this.async = async;
        PersistentCache.LOG.info("wrapping map " + this.type);
        map = new MultiGenerationMap<K, V>();
        keyType = new KeyDataType(type);
        valueType = new ValueDataType(docNodeStore, docStore, type);
        this.memCacheMetadata = new CacheMetadata<K>();
        if (async) {
            this.writeQueue = new CacheWriteQueue<K, V>(dispatcher, cache, map);
            LOG.info("The persistent cache {} writes will be asynchronous", type);
        } else {
            this.writeQueue = null;
            this.memCacheMetadata.disable();
            LOG.info("The persistent cache {} writes will be synchronous", type);
        }
    }

    @Override
    public void addGeneration(int generation, boolean readOnly) {
        MVMap.Builder<K, V> b = new MVMap.Builder<K, V>().
                keyType(keyType).valueType(valueType);
        String mapName = type.name();
        CacheMap<K, V> m = cache.openMap(generation, mapName, b);
        map.addReadMap(generation, m);
        if (!readOnly) {
            map.setWriteMap(m);
        }
    }

    @Override
    public void removeGeneration(int generation) {
        map.removeReadMap(generation);
    }

    private V readIfPresent(K key) {
        return async ? asyncReadIfPresent(key) : syncReadIfPresent(key);
    }

    private V syncReadIfPresent(K key) {
        cache.switchGenerationIfNeeded();
        V v = map.get(key);
        if (v != null) {
            memCacheMetadata.putFromPersistenceAndIncrement(key);
        }
        return v;
    }

    private V asyncReadIfPresent(K key) {
            MultiGenerationMap.ValueWithGenerationInfo<V> v = map.readValue(key);
            if (v == null) {
                return null;
            }
            if (v.isCurrentGeneration() && !cache.needSwitch()) {
                // don't persist again on eviction
                memCacheMetadata.putFromPersistenceAndIncrement(key);
            } else {
                // persist again during eviction
                memCacheMetadata.increment(key);
            }
            return v.getValue();
    }

    private void write(final K key, final V value) {
        cache.switchGenerationIfNeeded();
        if (value == null) {
            map.remove(key);
        } else {
            map.put(key, value);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    @Nullable
    public V getIfPresent(Object key) {
        memCacheMetadata.increment((K) key);
        V value = memCache.getIfPresent(key);
        if (value == null) {
            memCacheMetadata.remove(key);
        } else {
            return value;
        }

        // it takes care of updating memCacheMetadata
        value = readIfPresent((K) key);
        if (value != null) {
            memCache.put((K) key, value);
        }
        return value;
    }

    @Override
    public V get(K key,
                 Callable<? extends V> valueLoader)
            throws ExecutionException {
        V value = getIfPresent(key);
        if (value != null) {
            return value;
        }

        memCacheMetadata.increment(key);
        value = memCache.get(key, valueLoader);
        if (!async) {
            write((K) key, value);
        }
        return value;
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(
            Iterable<?> keys) {
        Iterable<K> typedKeys = (Iterable<K>) keys;
        memCacheMetadata.incrementAll(keys);
        ImmutableMap<K, V> result = memCache.getAllPresent(keys);
        memCacheMetadata.removeAll(filter(typedKeys, not(in(result.keySet()))));
        return result;
    }

    @Override
    public void put(K key, V value) {
        memCacheMetadata.put(key);
        memCache.put(key, value);
        if (!async) {
            write((K) key, value);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void invalidate(Object key) {
        memCache.invalidate(key);
        memCacheMetadata.remove(key);
        if (async) {
            writeQueue.addInvalidate(singleton((K) key));
        } else {
            write((K) key, null);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        memCacheMetadata.putAll(m.keySet());
        memCache.putAll(m);
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        memCache.invalidateAll(keys);
        memCacheMetadata.removeAll(keys);
    }

    @Override
    public void invalidateAll() {
        memCache.invalidateAll();
        memCacheMetadata.clear();
        map.clear();
    }

    @Override
    public long size() {
        return memCache.size();
    }

    @Override
    public CacheStats stats() {
        return memCache.stats();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        return memCache.asMap();
    }

    @Override
    public void cleanUp() {
        memCache.cleanUp();
        memCacheMetadata.clear();
    }

    /**
     * Invoked on the eviction from the {@link #memCache}
     */
    @Override
    public void evicted(K key, V value, RemovalCause cause) {
        if (async && EVICTION_CAUSES.contains(cause) && value != null) {
            CacheMetadata.MetadataEntry metadata = memCacheMetadata.remove(key);
            boolean qualifiesToPersist = true;
            if (metadata != null && metadata.isReadFromPersistentCache()) {
                qualifiesToPersist = false;
            } else if (metadata != null && metadata.getAccessCount() < 1) {
                qualifiesToPersist = false;
            }

            if (qualifiesToPersist) {
                writeQueue.addPut(key, value);
            }
        }
    }
}
