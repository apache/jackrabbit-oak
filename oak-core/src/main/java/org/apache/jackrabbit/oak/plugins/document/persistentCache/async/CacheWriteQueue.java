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
package org.apache.jackrabbit.oak.plugins.document.persistentCache.async;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

/**
 * A fronted for the {@link CacheActionDispatcher} creating actions and maintaining their state.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class CacheWriteQueue<K, V> {

    private final CacheActionDispatcher dispatcher;

    private final PersistentCache cache;

    private final Map<K, V> map;

    final Multiset<K> queuedKeys = HashMultiset.create();

    final Set<K> waitsForInvalidation = new HashSet<K>();

    public CacheWriteQueue(CacheActionDispatcher dispatcher, PersistentCache cache, Map<K, V> map) {
        this.dispatcher = dispatcher;
        this.cache = cache;
        this.map = map;
    }

    /**
     * Add new invalidate action.
     *
     * @param keys to be invalidated
     */
    public void addInvalidate(Iterable<K> keys) {
        synchronized(this) {
            for (K key : keys) {
                queuedKeys.add(key);
                waitsForInvalidation.add(key);
            }
        }
        dispatcher.add(new InvalidateCacheAction<K, V>(this, keys));
    }

    /**
     * Add new put action
     *
     * @param key to be put to cache
     * @param value to be put to cache
     */
    public void addPut(K key, V value) {
        synchronized(this) {
            queuedKeys.add(key);
            waitsForInvalidation.remove(key);
        }
        dispatcher.add(new PutToCacheAction<K, V>(this, key, value));
    }

    /**
     * Check if the last action added for this key was invalidate
     *
     * @param key to check 
     * @return {@code true} if the last added action was invalidate
     */
    public synchronized boolean waitsForInvalidation(K key) {
        return waitsForInvalidation.contains(key);
    }

    /**
     * Remove the action state when it's finished or cancelled.
     *
     * @param key to be removed
     */
    synchronized void remove(K key) {
        queuedKeys.remove(key);
        if (!queuedKeys.contains(key)) {
            waitsForInvalidation.remove(key);
        }
    }

    PersistentCache getCache() {
        return cache;
    }

    Map<K, V> getMap() {
        return map;
    }
}