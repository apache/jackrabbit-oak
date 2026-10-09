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

import java.lang.ref.WeakReference;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheStatsSnapshot;

/** A live raw-value view over a cache whose entries own their access metadata. */
class CacheEntries<K extends CacheValue, V extends CacheValue> implements Cache<K, V> {

    final Cache<K, CacheEntry<V>> memCache;
    final AtomicLong clearGeneration = new AtomicLong();
    private final ConcurrentMap<K, CacheEntry<V>> entries;
    private final Map<K, WeakReference<CacheEntry.KeyState<K>>> keyStates = new WeakHashMap<>();
    private final ConcurrentMap<K, V> values = new ValueMap();

    CacheEntries(Cache<K, CacheEntry<V>> memCache) {
        this.memCache = memCache;
        this.entries = memCache.asMap();
    }

    @SuppressWarnings("unchecked")
    CacheEntry.KeyState<K> keyState(K key) {
        Objects.requireNonNull(key);
        CacheEntry<V> current = entries.get(key);
        if (current != null) {
            // The entry keeps its canonical state and weak registry key alive even after eviction.
            return (CacheEntry.KeyState<K>) current.getKeyState();
        }
        synchronized (keyStates) {
            WeakReference<CacheEntry.KeyState<K>> reference = keyStates.get(key);
            CacheEntry.KeyState<K> state = reference == null ? null : reference.get();
            if (state == null) {
                // Replace an expired state together with its canonical weak key.
                // WeakHashMap.put alone retains an equal, older key object.
                keyStates.remove(key);
                state = new CacheEntry.KeyState<>(key);
                keyStates.put(key, new WeakReference<>(state));
            }
            return state;
        }
    }

    CacheEntry<V> newEntry(CacheEntry.KeyState<K> state, V value, boolean persisted, boolean accessed) {
        Objects.requireNonNull(value);
        state.retireLatest();
        CacheEntry<V> entry = new CacheEntry<>(value, state, clearGeneration.get(), persisted, accessed);
        state.latest = new WeakReference<>(entry);
        return entry;
    }

    boolean tracksAccess() {
        return true;
    }

    private void accessed(CacheEntry<V> entry) {
        if (entry != null && tracksAccess()) {
            entry.accessed();
        }
    }

    @Override
    public V getIfPresent(K key) {
        if (!tracksAccess()) {
            return value(memCache.getIfPresent(key));
        }
        CacheEntry<V> entry = entries.get(key);
        // Count the hit before Caffeine can evict the entry while processing the read.
        if (entry != null) {
            entry.accessed();
        }
        CacheEntry<V> returned = memCache.getIfPresent(key);
        if (returned != null && returned != entry) {
            returned.accessed();
        }
        return value(returned);
    }

    @Override
    public V get(K key, Function<? super K, ? extends V> loader) {
        Objects.requireNonNull(loader);
        CacheEntry.KeyState<K> state = keyState(key);
        synchronized (state) {
            return getWhileLocked(key, loader, state);
        }
    }

    V getWhileLocked(K key, Function<? super K, ? extends V> loader, CacheEntry.KeyState<K> state) {
        Objects.requireNonNull(loader);
        if (tracksAccess()) {
            accessed(entries.get(key));
        }
        return value(memCache.get(key, k -> {
            V loaded = loader.apply(k);
            return loaded == null ? null : newEntry(state, loaded, false, tracksAccess());
        }));
    }

    void putWhileLocked(K key, V value, CacheEntry.KeyState<K> state) {
        memCache.put(key, newEntry(state, value, false, false));
    }

    @Override
    public void put(K key, V value) {
        CacheEntry.KeyState<K> state = keyState(key);
        synchronized (state) {
            putWhileLocked(key, value, state);
        }
    }

    void putFromPersistence(K key, V value, boolean persisted) {
        CacheEntry.KeyState<K> state = keyState(key);
        synchronized (state) {
            putFromPersistence(key, value, persisted, state);
        }
    }

    void putFromPersistence(K key, V value, boolean persisted, CacheEntry.KeyState<K> state) {
        memCache.put(key, newEntry(state, value, persisted, tracksAccess()));
    }

    void invalidateWhileLocked(K key, CacheEntry.KeyState<K> state) {
        state.retireLatest();
        memCache.invalidate(key);
    }

    @Override
    public Map<K, V> getAllPresent(Iterable<? extends K> keys) {
        Map<K, V> result = new LinkedHashMap<>();
        for (K key : keys) {
            V present = getIfPresent(key);
            if (present != null) {
                result.put(key, present);
            }
        }
        return result;
    }

    @Override
    public void invalidate(K key) {
        CacheEntry.KeyState<K> state = keyState(key);
        synchronized (state) {
            invalidateWhileLocked(key, state);
        }
    }

    @Override
    public void invalidateAll(Iterable<? extends K> keys) {
        keys.forEach(this::invalidate);
    }

    @Override
    public void invalidateAll() {
        invalidateMemory();
    }

    private void invalidateMemory() {
        clearGeneration.incrementAndGet();
        synchronized (keyStates) {
            for (WeakReference<CacheEntry.KeyState<K>> reference : keyStates.values()) {
                CacheEntry.KeyState<K> state = reference.get();
                if (state != null) {
                    state.retireLatest();
                }
            }
        }
        memCache.invalidateAll();
    }

    @Override
    public long estimatedSize() { return memCache.estimatedSize(); }

    @Override
    public CacheStatsSnapshot stats() { return memCache.stats(); }

    @Override
    public ConcurrentMap<K, V> asMap() { return values; }

    @Override
    public void cleanUp() { memCache.cleanUp(); }

    @Override
    public long getUsedWeight() { return memCache.getUsedWeight(); }

    @Override
    public void setMaximumWeight(long weight) { memCache.setMaximumWeight(weight); }

    private V value(CacheEntry<V> entry) {
        return entry == null ? null : entry.getValue();
    }

    private final class ValueMap extends AbstractMap<K, V> implements ConcurrentMap<K, V> {
        @Override
        public V get(Object key) {
            CacheEntry<V> entry = entries.get(key);
            accessed(entry);
            return value(entry);
        }

        @Override
        public int size() { return entries.size(); }

        @Override
        public boolean containsKey(Object key) { return entries.containsKey(key); }

        @Override
        public V put(K key, V value) {
            CacheEntry.KeyState<K> state = keyState(key);
            synchronized (state) {
                return CacheEntries.this.value(entries.put(key, newEntry(state, value, false, false)));
            }
        }

        @Override
        public V putIfAbsent(K key, V value) {
            Objects.requireNonNull(value);
            CacheEntry.KeyState<K> state = keyState(key);
            synchronized (state) {
                CacheEntry<V> existing = entries.get(key);
                if (existing != null) { return existing.getValue(); }
                return CacheEntries.this.value(entries.putIfAbsent(key, newEntry(state, value, false, false)));
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public V remove(Object key) {
            CacheEntry.KeyState<K> state = keyState((K) key);
            synchronized (state) {
                state.retireLatest();
                return value(entries.remove(key));
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean remove(Object key, Object expected) {
            CacheEntry.KeyState<K> state = keyState((K) key);
            synchronized (state) {
                CacheEntry<V> current = entries.get(key);
                if (current == null || !Objects.equals(current.getValue(), expected)) { return false; }
                if (!entries.remove(key, current)) { return false; }
                current.retire();
                return true;
            }
        }

        @Override
        public V replace(K key, V replacement) {
            Objects.requireNonNull(replacement);
            CacheEntry.KeyState<K> state = keyState(key);
            synchronized (state) {
                if (!entries.containsKey(key)) { return null; }
                return value(entries.replace(key, newEntry(state, replacement, false, false)));
            }
        }

        @Override
        public boolean replace(K key, V expected, V replacement) {
            Objects.requireNonNull(replacement);
            CacheEntry.KeyState<K> state = keyState(key);
            synchronized (state) {
                CacheEntry<V> current = entries.get(key);
                if (current == null || !Objects.equals(current.getValue(), expected)) { return false; }
                return entries.replace(key, current, newEntry(state, replacement, false, false));
            }
        }

        @Override
        public V compute(K key, BiFunction<? super K, ? super V, ? extends V> function) {
            Objects.requireNonNull(function);
            CacheEntry.KeyState<K> state = keyState(key);
            synchronized (state) {
                return value(entries.compute(key, (k, old) -> {
                    V replacement = function.apply(k, value(old));
                    if (replacement == null) {
                        if (old != null) { old.retire(); }
                        return null;
                    }
                    return old != null && replacement == old.getValue()
                            ? old : newEntry(state, replacement, false, false);
                }));
            }
        }

        @Override
        public V computeIfAbsent(K key, Function<? super K, ? extends V> function) {
            Objects.requireNonNull(function);
            return compute(key, (k, existing) -> existing == null ? function.apply(k) : existing);
        }

        @Override
        public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> function) {
            Objects.requireNonNull(function);
            return compute(key, (k, existing) -> existing == null ? null : function.apply(k, existing));
        }

        @Override
        public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> function) {
            Objects.requireNonNull(value);
            Objects.requireNonNull(function);
            return compute(key, (k, existing) -> existing == null ? value : function.apply(existing, value));
        }

        @Override
        public void clear() { invalidateMemory(); }

        @Override
        public Set<Map.Entry<K, V>> entrySet() {
            return new AbstractSet<Map.Entry<K, V>>() {
                @Override
                public int size() { return entries.size(); }

                @Override
                public Iterator<Map.Entry<K, V>> iterator() {
                    Iterator<Map.Entry<K, CacheEntry<V>>> iterator = entries.entrySet().iterator();
                    return new Iterator<Map.Entry<K, V>>() {
                        private Map.Entry<K, CacheEntry<V>> current;
                        @Override
                        public boolean hasNext() { return iterator.hasNext(); }
                        @Override
                        public Map.Entry<K, V> next() {
                            current = iterator.next();
                            K key = current.getKey();
                            return new SimpleEntry<K, V>(key, current.getValue().getValue()) {
                                @Override
                                public V setValue(V replacement) {
                                    V previous = ValueMap.this.put(key, replacement);
                                    super.setValue(replacement);
                                    return previous;
                                }
                            };
                        }
                        @Override
                        public void remove() {
                            if (current == null) { throw new IllegalStateException(); }
                            ValueMap.this.remove(current.getKey(), current.getValue().getValue());
                            current = null;
                        }
                    };
                }
            };
        }
    }
}
