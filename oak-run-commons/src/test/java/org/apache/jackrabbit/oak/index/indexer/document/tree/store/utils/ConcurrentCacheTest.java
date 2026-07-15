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

import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class ConcurrentCacheTest {

    @Test
    public void testRandomOperations() throws Exception {
        Random r = new Random(1);
        int maxMemory = 1000;
//        final MemoryBoundCache<Integer, MemoryValue> cache =
//              new MemoryBoundCache<>(maxMemory);
        final SieveCache<Integer, MemoryValue> cache =
                new SieveCache<>(maxMemory);
        final Exception[] ex = new Exception[1];
        int size = 3;
        Thread[] threads = new Thread[size];
        final AtomicBoolean stop = new AtomicBoolean();
        for (int i = 0; i < size; i++) {
            Thread t = new Thread() {
                @Override
                public void run() {
                    while (!stop.get()) {
                        try {
                            cache.get(r.nextInt(maxMemory));
                            cache.keys();
                            cache.put(r.nextInt(maxMemory), new MemoryValue("1", 1 + r.nextInt(10)));
                            cache.size();
                        } catch (Exception e) {
                            ex[0] = e;
                        }
                    }
                }
            };
            t.start();
            threads[i] = t;
        }
        try {
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() < start + 1000) {
                for (int i = 0; i < 100000 && ex[0] == null; i++) {
                    cache.put(r.nextInt(maxMemory), new MemoryValue("1", 1 + r.nextInt(10)));
                }
            }
        } finally {
            stop.set(true);
            for (Thread t : threads) {
                t.join();
            }
        }
        if (ex[0] != null) {
            throw ex[0];
        }
        int memorySum = 0;
        for(Integer k : cache.keys()) {
            memorySum += cache.get(k).memory;
        }
        System.out.println(cache.toString());
        assertTrue(memorySum >= 0);
        assertTrue("totalMemory: " + memorySum, memorySum <= maxMemory);
        assertTrue(cache.size() >= memorySum / 10);
        assertTrue(cache.size() < memorySum);
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

