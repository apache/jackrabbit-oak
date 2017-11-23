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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class MultiGenerationMap<K, V> implements Map<K, V> {
    
    private volatile CacheMap<K, V> write;
    private ConcurrentSkipListMap<Integer, CacheMap<K, V>> read = 
            new ConcurrentSkipListMap<Integer, CacheMap<K, V>>();
    
    MultiGenerationMap() {
    }
    
    public void setWriteMap(CacheMap<K, V> m) {
        write = m;
    }

    public void addReadMap(int generation, CacheMap<K, V> m) {
        read.put(generation, m);
    }
    
    public void removeReadMap(int generation) {
        read.remove(generation);
    }
    
    @Override
    public V put(K key, V value) {
        CacheMap<K, V> m = write;
        if (m == null) {
            // closed concurrently
            return null;
        }
        return m.put(key, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(Object key) {
        ValueWithGenerationInfo<V> value = readValue(key);
        if (value == null) {
            return null;
        } else if (!value.isCurrentGeneration()) {
            put((K) key, value.value);
        }
        return value.getValue();
    }

    ValueWithGenerationInfo<V> readValue(Object key) {
        for (int generation : read.descendingKeySet()) {
            CacheMap<K, V> m = read.get(generation);
            if (m != null) {
                V value = m.get(key);
                if (value != null) {
                    return new ValueWithGenerationInfo<V>(value, m == write);
                }
            }
        }
        return null;
    }
    
    @Override
    public boolean containsKey(Object key) {
        for (int generation : read.descendingKeySet()) {
            CacheMap<K, V> m = read.get(generation);
            if (m != null) {
                if (m.containsKey(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public V remove(Object key) {
        return write.remove(key);
    }

    @Override
    public void clear() {
        write.clear();
    }
    
    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException();
    }

    static class ValueWithGenerationInfo<V> {

        private final V value;

        private final boolean isCurrentGeneration;

        private ValueWithGenerationInfo(V value, boolean isCurrentGeneration) {
            this.value = value;
            this.isCurrentGeneration = isCurrentGeneration;
        }

        V getValue() {
            return value;
        }

        boolean isCurrentGeneration() {
            return isCurrentGeneration;
        }
    }
}
