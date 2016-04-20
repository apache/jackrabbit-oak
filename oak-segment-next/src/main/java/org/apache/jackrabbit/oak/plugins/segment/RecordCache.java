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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FIXME OAK-3348 XXX document
 */
abstract class RecordCache<T> {
    // FIXME OAK-3348 XXX this caches retain in mem refs to old gens. assess impact and mitigate/fix
    private static final RecordCache<?> DISABLED_CACHE = new RecordCache<Object>() {
        @Override
        RecordId get(Object key) {
            return null;
        }

        @Override
        RecordId put(Object key, RecordId value) {
            return null;
        }

        @Override
        void clear() { }
    };

    @SuppressWarnings("unchecked")
    static <T> RecordCache<T> newRecordCache(final int initialSize, final int size, boolean disabled) {
        if (disabled) {
            return (RecordCache<T>) DISABLED_CACHE;
        } else {
            return new LRURecordCache<T>(initialSize, size);
        }
    }

    abstract RecordId get(Object key);
    abstract RecordId put(T key, RecordId value);
    abstract void clear();

    private static final class LRURecordCache<T> extends RecordCache<T> {
        private final Map<T, RecordId> cache;

        private LRURecordCache(int initialSize, final int size) {
            cache = new LinkedHashMap<T, RecordId>(initialSize, 0.9f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<T, RecordId> eldest) {
                    return size() > size;
                }
            };
        }

        @Override
        synchronized RecordId get(Object key) {
            return cache.get(key);
        }

        @Override
        synchronized RecordId put(T key, RecordId value) {
            return cache.put(key, value);
        }

        @Override
        public synchronized void clear() {
            cache.clear();
        }
    }
}
