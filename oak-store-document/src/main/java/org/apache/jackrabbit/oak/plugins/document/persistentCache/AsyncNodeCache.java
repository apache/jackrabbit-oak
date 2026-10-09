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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache.GenerationCache;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.async.CacheActionDispatcher;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.async.CacheWriteQueue;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.type.DataType;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry-owned persistence metadata for caches using asynchronous maintenance. */
public class AsyncNodeCache<K extends CacheValue, V extends CacheValue>
        extends CacheEntries<K, V> implements GenerationCache, EvictionListener<K, CacheEntry<V>> {

    static final Logger LOG = LoggerFactory.getLogger(AsyncNodeCache.class);

    private static final Set<EvictionCause> EVICTION_CAUSES = Set.of(EvictionCause.COLLECTED, EvictionCause.EXPIRED, EvictionCause.SIZE);

    private final PersistentCache cache;
    private final PersistentCacheStats stats;
    private final MultiGenerationMap<K, V> map;
    private final CacheType type;
    private final DataType keyType;
    private final DataType valueType;
    private final ReentrantLock writeOrder = new ReentrantLock();
    private final DocumentNodeStore nodeStore;
    private final boolean async;
    CacheWriteQueue<K, V> writeQueue;

    AsyncNodeCache(
            PersistentCache cache,
            Cache<K, CacheEntry<V>> memCache,
            DocumentNodeStore docNodeStore,
            DocumentStore docStore,
            CacheType type,
            CacheActionDispatcher dispatcher,
            StatisticsProvider statisticsProvider,
            boolean async) {
        super(memCache);
        this.cache = cache;
        this.type = type;
        this.nodeStore = docNodeStore;
        this.async = async;
        PersistentCache.LOG.info("wrapping map " + this.type);
        map = new MultiGenerationMap<K, V>();
        keyType = new KeyDataType(type);
        valueType = new ValueDataType(docNodeStore, docStore, type);
        if (async) {
            this.writeQueue = new CacheWriteQueue<K, V>(dispatcher, cache, map);
            LOG.info("The persistent cache {} writes will be asynchronous", type);
        } else {
            this.writeQueue = null;
            LOG.info("The persistent cache {} writes will be synchronous", type);
        }
        this.stats = new PersistentCacheStats(type, statisticsProvider);
    }

    @Override
    public CacheType getType() {
        return type;
    }

    @Override
    public void addGeneration(int generation, boolean readOnly) {
        MVMap.Builder<K, V> b = new MVMap.Builder<K, V>().
                keyType(keyType).valueType(valueType);
        CacheMap<K, V> m = cache.openMap(generation, type.getMapName(), b);
        map.addReadMap(generation, m);
        if (!readOnly) {
            map.setWriteMap(m);
            stats.addWriteGeneration(generation);
        }
    }

    @Override
    public void removeGeneration(int generation) {
        map.removeReadMap(generation);
        stats.removeReadGeneration(generation);
    }

    @Override
    boolean tracksAccess() {
        return async;
    }

    private V readIfPresent(K key, CacheEntry.KeyState<K> state) {
        return async ? asyncReadIfPresent(key, state) : syncReadIfPresent(key, state);
    }

    private V syncReadIfPresent(K key, CacheEntry.KeyState<K> state) {
        cache.switchGenerationIfNeeded();
        TimerStats.Context ctx = stats.startReadTimer();
        V v = map.get(key);
        ctx.stop();
        if (v != null) {
            putFromPersistence(key, v, true, state);
        }
        return v;
    }

    private V asyncReadIfPresent(K key, CacheEntry.KeyState<K> state) {
        TimerStats.Context ctx = stats.startReadTimer();
        try {
            CacheMap<K, V> generation = map.getWriteMap();
            MultiGenerationMap.ValueWithGenerationInfo<V> v = map.readValue(key);
            if (v == null) {
                return null;
            }
            // A concurrent rotation may have moved this value into the old generation.
            if (v.isCurrentGeneration() && !cache.needSwitch() && generation == map.getWriteMap()) {
                // don't persist again on eviction
                putFromPersistence(key, v.getValue(), true, state);
            } else {
                // persist again during eviction
                putFromPersistence(key, v.getValue(), false, state);
            }
            return v.getValue();
        } finally {
            ctx.stop();
        }
    }

    private void broadcast(final K key, final V value) {
        cache.broadcast(type, buffer -> {
                keyType.write(buffer, key);
                if (value == null) {
                    buffer.put((byte) 0);
                } else {
                    buffer.put((byte) 1);
                    valueType.write(buffer, value);
                }
                return null;
            });
    }

    private void write(final K key, final V value) {
        cache.switchGenerationIfNeeded();
        if (value == null) {
            map.remove(key);
        } else {
            if (!type.shouldCache(nodeStore, key)){
                return;
            }
            map.put(key, value);

            long memory = 0L;
            memory += keyType.getMemory(key);
            memory += valueType.getMemory(value);
            stats.markBytesWritten(memory);
            stats.markPut();
        }
    }

    @Override
    @Nullable
    public V getIfPresent(K key) {
        V value = super.getIfPresent(key);
        if (value != null) {
            return value;
        }

        CacheEntry.KeyState<K> state = keyState(key);
        synchronized (state) {
            value = super.getIfPresent(key);
            if (value != null || !type.shouldCache(nodeStore, key)) {
                return value;
            }
            stats.markRequest();
            value = readIfPresent(key, state);
            if (value != null) {
                stats.markHit();
            }
            return value;
        }
    }

    @Override
    public V get(K key,
            Function<? super K, ? extends V> mappingFunction) {

        V value = getIfPresent(key);
        if (value != null) {
            return value;
        }

        TimerStats.Context ctx = stats.startLoaderTimer();
        try {
            CacheEntry.KeyState<K> state = keyState(key);
            synchronized (state) {
                value = getWhileLocked(key, mappingFunction, state);
                if (!async) {
                    write(key, value);
                }
            }
            broadcast(key, value);
            return value;
        } catch (RuntimeException e) {
            stats.markException();
            throw e;
        } finally {
            ctx.stop();
        }
    }

    @Override
    public void put(K key, V value) {
        CacheEntry.KeyState<K> state = keyState(key);
        synchronized (state) {
            putWhileLocked(key, value, state);
            if (!async) {
                write(key, value);
            }
        }
        broadcast(key, value);
    }

    @Override
    public void invalidate(K key) {
        CacheEntry.KeyState<K> state = keyState(key);
        synchronized (state) {
            writeOrder.lock();
            try {
                state.retireLatest();
                if (!async || !writeQueue.addInvalidate(Collections.singleton(key))) {
                    write(key, null);
                }
            } finally {
                writeOrder.unlock();
            }
            // Removal callbacks may acquire writeOrder: do not hold it across cache operations.
            memCache.invalidate(key);
        }
        broadcast(key, null);
        stats.markInvalidateOne();
    }

    @Override
    public void invalidateAll() {
        writeOrder.lock();
        try {
            clearGeneration.incrementAndGet();
            map.clear();
        } finally {
            writeOrder.unlock();
        }
        super.invalidateAll();
        stats.markInvalidateAll();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void receive(ByteBuffer buff) {
        K key = (K) keyType.read(buff);
        CacheEntry.KeyState<K> state = keyState(key);
        synchronized (state) {
            V value;
            if (buff.get() == 0) {
                value = null;
                invalidateWhileLocked(key, state);
            } else {
                value = (V) valueType.read(buff);
                putWhileLocked(key, value, state);
            }
            stats.markRecvBroadcast();
            if (!async) {
                write(key, value);
            }
        }
    }

    /** Queues only the evicted entry's eligible value for persistence. */
    @Override
    public void evicted(K key, CacheEntry<V> entry, EvictionCause cause) {
        if (entry == null) {
            return;
        }
        if (cause == EvictionCause.EXPLICIT || cause == EvictionCause.REPLACED) {
            entry.retire();
            return;
        }
        if (!async || cause == null || !EVICTION_CAUSES.contains(cause)) {
            return;
        }
        synchronized (entry) {
            if (entry.evictionQueued || entry.isRetired() || entry.clearGeneration != clearGeneration.get()) {
                return;
            }
            if (entry.isFromPersistence()) {
                stats.markPutRejectedAlreadyPersisted();
            } else if (entry.getAccessCount() < 1) {
                stats.markPutRejectedEntryNotUsed();
            } else if (!type.shouldCache(nodeStore, key)) {
                stats.markPutRejectedAsCachedInSecondary();
            } else {
                V value = entry.getValue();
                boolean added = writeQueue.addPut(key, value,
                        () -> !entry.isRetired() && entry.clearGeneration == clearGeneration.get(), writeOrder,
                        () -> {
                            stats.markBytesWritten((long) keyType.getMemory(key) + valueType.getMemory(value));
                            stats.markPut();
                        });
                if (added) {
                    entry.evictionQueued = true;
                } else {
                    stats.markPutRejectedQueueFull();
                }
            }
        }
    }

    public PersistentCacheStats getPersistentCacheStats() {
        return stats;
    }

    Map<K, V> getGenerationalMap() {
        return Collections.unmodifiableMap(map);
    }
}
