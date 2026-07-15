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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class CacheTest {

    @Test
    public void memoryBoundCacheTest() {
        ConcurrentLRUCache<String, MemoryValue> cache = new ConcurrentLRUCache<>(1000);
        for (int i = 0; i < 200; i++) {
            cache.put("k" + i, new MemoryValue("v" + i, 10));
        }
        assertEquals(101, cache.size());
    }

    @Test
    public void sieveCacheTest() {
        AtomicLong removed = new AtomicLong();
        SieveCache<String, MemoryValue> cache = new SieveCache<>(1000) {
            @Override
            public void entryWasRemoved(String key, MemoryValue value) {
                removed.incrementAndGet();
            }
        };
        for (int i = 0; i < 200; i++) {
            cache.put("k" + i, new MemoryValue("v" + i, 10));
        }
        assertEquals(100, removed.get());
        assertEquals(100, cache.size());
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 50; i++) {
                cache.put("k" + i, new MemoryValue("v" + i, 10));
            }
        }
        assertEquals(150, removed.get());
        assertEquals(100, cache.size());
    }

    static class MemoryValue implements MemoryObject {

        final String value;
        final int memory;

        MemoryValue(String value, int memory) {
            this.value = value;
            this.memory = memory;
        }

        @Override
        public long estimatedMemory() {
            return memory;
        }

        public String toString() {
            return value;
        }

    }
}
