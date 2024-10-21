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

package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.segment.RecordCache.newRecordCache;
import static org.apache.jackrabbit.oak.segment.TestUtils.newRecordId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.Test;

public class RecordCacheTest {
    private final Random rnd = new Random();
    private final MemoryStore store = new MemoryStore();
    private final SegmentIdProvider idProvider = store.getSegmentIdProvider();

    public RecordCacheTest() throws IOException {}

    @Test
    public void emptyCache() {
        RecordCache<String> cache = newRecordCache(0);
        assertNull(cache.get("any"));

        cache.put("key", (newRecordId(idProvider, rnd)));
        assertNull(cache.get("key"));
    }

    @Test
    public void putAndGet() {
        RecordCache<String> cache = newRecordCache(10);
        assertNull(cache.get("any"));

        RecordId value = newRecordId(idProvider, rnd);
        cache.put("key", value);
        assertEquals(value, cache.get("key"));
    }

    @Test
    public void concurrentPutAndGet() throws ExecutionException, InterruptedException {
        final int SIZE = 16384;

        RecordCache<String> cache = newRecordCache(SIZE);
        HashMap<String, RecordId> values = new HashMap<>(SIZE);
        List<Integer> indices = new ArrayList<>(SIZE);

        for (int k = 0; k < SIZE; k ++) {
            String key = "key-" + k;
            RecordId value = newRecordId(idProvider, rnd);
            values.put(key, value);
            indices.add(k);
        }

        Collections.shuffle(indices);
        ExecutorService executor = Executors.newFixedThreadPool(16);
        List<Future<String>> putFutures = new ArrayList<>(SIZE);
        List<Future<Void>> getFutures = new ArrayList<>(SIZE);

        for (int k = 0; k < SIZE; k ++) {
            int idx = k;
            putFutures.add(executor.submit(() -> {
                String key = "key-" + idx;
                cache.put(key, values.get(key));
                return key;
            }));
        }

        for (Future<String> future : putFutures) {
            getFutures.add(executor.submit(() -> {
                String key = future.get();
                assertEquals(values.get(key), cache.get(key));
                return null;
            }));
        }

        for (Future<Void> future : getFutures) {
            future.get();
        }
    }

    @Test
    public void invalidate() {
        RecordCache<String> cache = newRecordCache(100);
        Map<String, RecordId> keys = new LinkedHashMap<>();
        for (int k = 0; k < 100; k ++) {
            String key = "key-" + k;
            RecordId value = newRecordId(idProvider, rnd);
            keys.put(key, value);
            cache.put(key, value);
        }

        for (Entry<String, RecordId> entry : keys.entrySet()) {
            assertEquals(entry.getKey() + " should be in the cache",
                    entry.getValue(), cache.get(entry.getKey()));
        }

        RecordId value = newRecordId(idProvider, rnd);
        // This should invalidate "key-0", which is the least recently accessed key
        cache.put("key", value);
        keys.put("key", value);
        keys.remove("key-0");
        for (Entry<String, RecordId> entry : keys.entrySet()) {
            assertEquals(entry.getKey() + " should be in the cache",
                    entry.getValue(), cache.get(entry.getKey()));
        }
    }

}
