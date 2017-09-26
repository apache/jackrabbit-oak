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

import java.util.Map;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;

/**
 * An invalidate cache action.
 *
 * @param <K> key type
 * @param <V> value type
 */
class InvalidateCacheAction<K, V> implements CacheAction<K, V> {

    private final PersistentCache cache;

    private final Map<K, V> map;

    private final Iterable<K> keys;

    InvalidateCacheAction(Iterable<K> keys, CacheWriteQueue<K, V> queue) {
        this.keys = keys;
        this.cache = queue.getCache();
        this.map = queue.getMap();
    }

    @Override
    public void execute() {
        if (map != null) {
            for (K key : keys) {
                cache.switchGenerationIfNeeded();
                map.remove(key);
            }
        }
    }

    @Override
    public String toString() {
        return new StringBuilder("InvalidateCacheAction").append(Iterables.toString(keys)).toString();
    }
}