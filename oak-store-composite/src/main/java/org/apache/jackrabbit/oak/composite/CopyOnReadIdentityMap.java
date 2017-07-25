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
package org.apache.jackrabbit.oak.composite;

import com.google.common.base.Function;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * This map wraps around the passed argument and caches all the returned values.
 * It is meant to be wrapped around the result of
 * {@link com.google.common.collect.Maps#transformValues(Map, Function)} method
 * or its variant. This allows to preserve the laziness of map transformation and
 * at the same time to avoid re-calculating the same values.
 * <br>
 * It's immutable and used IdentityHashMap for caching values.
 *
 * @param <K> - the type of keys maintained by this map
 * @param <V> - the type of mapped values
 */
class CopyOnReadIdentityMap<K, V> implements Map<K, V> {

    private final Map<K, V> map;

    private Integer cachedSize;

    private Boolean cachedIsEmpty;

    private Map<K, V> cachedValues;

    private boolean allValuesCached;

    public CopyOnReadIdentityMap(Map<K, V> wrappedMap) {
        this.map = wrappedMap;
    }

    @Override
    public int size() {
        if (allValuesCached) {
            return cachedValues.size();
        }
        if (cachedSize == null) {
            cachedSize = map.size();
        }
        return cachedSize;
    }

    @Override
    public boolean isEmpty() {
        if (allValuesCached) {
            return cachedValues.isEmpty();
        }
        if (cachedIsEmpty == null) {
            if (cachedSize == null) {
                cachedIsEmpty = map.isEmpty();
            } else {
                cachedIsEmpty = cachedSize > 0;
            }
        }
        return cachedIsEmpty;
    }

    @Override
    public boolean containsKey(Object key) {
        if (allValuesCached) {
            return cachedValues.containsKey(key);
        }
        if (cachedValues != null && cachedValues.containsKey(key)) {
            return true;
        } else {
            return map.containsKey(key);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        if (allValuesCached) {
            return cachedValues.containsValue(value);
        }
        for (K k : keySet()) {
            V v = get(k);
            if (value == null && v == null) {
                return true;
            } else if (value != null && value.equals(v)) {
                return true;
            }
        }
        if (cachedValues == null) {
            cachedValues = Collections.emptyMap();
        }
        allValuesCached = true;
        return false;
    }

    @Override
    public V get(Object key) {
        if (allValuesCached) {
            return cachedValues.get(key);
        }
        if (cachedValues != null && cachedValues.containsKey(key)) {
            return cachedValues.get(key);
        } else if (map.containsKey(key)) {
            initCachedValues();
            V v = map.get(key);
            cachedValues.put((K) key, v);
            return v;
        } else {
            return null;
        }
    }

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public Collection<V> values() {
        readAll();
        return cachedValues.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        readAll();
        return cachedValues.entrySet();
    }

    private void readAll() {
        if (allValuesCached) {
            return;
        }
        initCachedValues();
        for (Entry<K, V> e : map.entrySet()) {
            if (!cachedValues.containsKey(e.getKey())) {
                cachedValues.put(e.getKey(), e.getValue());
            }
        }
        allValuesCached = true;
    }

    private void initCachedValues() {
        if (cachedValues == null) {
            cachedValues = new IdentityHashMap<>(map.size());
        }
    }

    @Override
    public String toString() {
        readAll();
        return cachedValues.toString();
    }
}
