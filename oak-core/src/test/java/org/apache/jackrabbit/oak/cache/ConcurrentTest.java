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
package org.apache.jackrabbit.oak.cache;

import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

/**
 * Tests the LIRS cache by concurrently reading and writing.
 */
public class ConcurrentTest {

    @Test
    public void testRandomOperations() throws Exception {
        Random r = new Random(1);
        final CacheLIRS<Integer, Integer> cache = new CacheLIRS.Builder().
                maximumWeight(100).build();
        final Exception[] ex = new Exception[1];
        int size = 3;
        Thread[] threads = new Thread[size];
        for (int i = 0; i < size; i++) {
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        cache.cleanUp();
                        cache.containsKey(1);
                        cache.containsValue(1);
                        cache.entrySet();
                        cache.getMaxMemory();
                        cache.getIfPresent(1);
                        cache.getAverageMemory();
                        cache.getMemory(1);
                        cache.getUsedMemory();
                        cache.invalidate(1);
                        cache.invalidateAll();
                        cache.isEmpty();
                        cache.keySet();
                        cache.peek(1);
                        cache.put(1, 10);
                        cache.refresh(1);
                        cache.remove(1);
                        cache.setAverageMemory(10);
                        cache.setMaxMemory(10);
                        cache.size();
                        cache.stats();
                        ConcurrentMap<Integer, Integer> map = cache.asMap();
                        map.size();
                        map.isEmpty();
                        map.containsKey(1);
                        map.containsValue(1);
                        map.get(1);
                        map.put(1, 10);
                        map.remove(1);
                        map.clear();
                        map.keySet();
                        map.values();
                        map.entrySet();
                        map.putIfAbsent(1, 10);
                        map.remove(1);
                        map.remove(1, 10);
                        map.replace(1, 10, 100);
                        map.replace(1, 10);
                        cache.get(1);
                        cache.getUnchecked(1);
                    } catch (Exception e) {
                        ex[0] = e;
                    }
                }
            };
            t.start();
            threads[i] = t;
        }
        try {
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() < start + 100) {
                for (int i = 0; i < 100000 && ex[0] == null; i++) {
                    cache.put(r.nextInt(1000), r.nextInt(10000));
                }
            }
        } finally {
            for (Thread t : threads) {
                t.join();
            }
        }
        if (ex[0] != null) {
            throw ex[0];
        }
    }
    
}
