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

import java.util.Map;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVMap.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache map. This map supports re-opening the store if this is needed.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class CacheMap<K, V> {
    
    static final Logger LOG = LoggerFactory.getLogger(CacheMap.class);
    
    private final MapFactory factory;
    private final String name;
    private final MVMap.Builder<K, V> builder;
    private int openCount;
    private volatile Map<K, V> map;
    private volatile boolean closed;

    
    public CacheMap(MapFactory factory, String name, Builder<K, V> builder) {
        this.factory = factory;
        this.name = name;
        this.builder = builder;
        openMap();
    }
    
    private void reopen(int i, Exception e) {
        if (i > 10) {
            LOG.warn("Too many re-opens; disabling this cache map", e);
            closed = true;
            return;
        }
        // clear the interrupt flag, to avoid re-opening many times
        Thread.interrupted();
        if (i == 0) {
            LOG.warn("Re-opening map " + name, e);
        } else {
            LOG.debug("Re-opening map " + name + " again", e);
            LOG.warn("Re-opening map " + name + " again");
        }
        openMap();
    }
    
    public V put(K key, V value) {
        for (int i = 0;; i++) {
            if (closed) {
                return null;
            }
            try {
                return map.put(key, value);
            } catch (Exception e) {
                reopen(i, e);
            }
        }
    }
    
    public V get(Object key) {
        for (int i = 0;; i++) {
            if (closed) {
                return null;
            }
            try {
                return map.get(key);
            } catch (Exception e) {
                reopen(i, e);
            }
        }
    }
    
    public boolean containsKey(Object key) {
        for (int i = 0;; i++) {
            if (closed) {
                return false;
            }
            try {
                return map.containsKey(key);
            } catch (Exception e) {
                reopen(i, e);
            }
        }        
    }
    
    public V remove(Object key) {
        for (int i = 0;; i++) {
            if (closed) {
                return null;
            }
            try {
                return map.remove(key);
            } catch (Exception e) {
                reopen(i, e);
            }
        }        
    }

    public void clear() {
        for (int i = 0;; i++) {
            if (closed) {
                return;
            }
            try {
                map.clear();
            } catch (Exception e) {
                reopen(i, e);
            }
        }        
    }
    
    void openMap() {
        openCount = factory.reopenStoreIfNeeded(openCount);
        Map<K, V> m2 = factory.openMap(name, builder);
        if (m2 != null) {
            map = m2;
        }
    }
    
}
