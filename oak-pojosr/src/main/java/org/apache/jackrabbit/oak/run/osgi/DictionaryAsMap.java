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

package org.apache.jackrabbit.oak.run.osgi;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A wrapper around a dictionary access it as a Map
 * Taken from org.apache.felix.utils.collections.DictionaryAsMap
 */
class DictionaryAsMap<U, V> extends AbstractMap<U, V> {

    private Dictionary<U, V> dict;

    public DictionaryAsMap(Dictionary<U, V> dict) {
        this.dict = dict;
    }

    @Override
    public Set<Entry<U, V>> entrySet() {
        return new AbstractSet<Entry<U, V>>() {
            @Override
            public Iterator<Entry<U, V>> iterator() {
                final Enumeration<U> e = dict.keys();
                return new Iterator<Entry<U, V>>() {
                    private U key;

                    public boolean hasNext() {
                        return e.hasMoreElements();
                    }

                    public Entry<U, V> next() {
                        key = e.nextElement();
                        return new KeyEntry(key);
                    }

                    public void remove() {
                        if (key == null) {
                            throw new IllegalStateException();
                        }
                        dict.remove(key);
                    }
                };
            }

            @Override
            public int size() {
                return dict.size();
            }
        };
    }

    @Override
    public V put(U key, V value) {
        return dict.put(key, value);
    }

    class KeyEntry implements Map.Entry<U, V> {

        private final U key;

        KeyEntry(U key) {
            this.key = key;
        }

        public U getKey() {
            return key;
        }

        public V getValue() {
            return dict.get(key);
        }

        public V setValue(V value) {
            return DictionaryAsMap.this.put(key, value);
        }
    }

}
