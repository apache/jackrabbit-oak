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

import static org.junit.Assert.assertFalse;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Ignore;
import org.junit.Test;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Compares the LIRS cache by concurrently reading.
 */
public class ConcurrentPerformanceTest {

    private static Cache<Integer, Integer> buildCache(boolean lirs,
            int concurrencyLevel) {
        if (lirs) {
            return new CacheLIRS.Builder<Integer, Integer>()
                    .segmentCount(concurrencyLevel).maximumSize(1000).build();
        }
        return CacheBuilder.newBuilder().concurrencyLevel(concurrencyLevel)
                .maximumSize(1000).build();
    }

    @Test
    @Ignore
    public void testPerformance() throws Exception {
        System.out.println("warmup...");
        testPerformance(2, 2, false);
        testPerformance(2, 2, true);
        testPerformance(2, 2, false);
        testPerformance(2, 2, true);
        System.out.println("test");
        for (int concurrencyLevel = 1; concurrencyLevel <= 128; concurrencyLevel *= 4) {
            for (int threadCount = 1; threadCount <= 128; threadCount *= 4) {
                testPerformance(threadCount, concurrencyLevel, false);
            }
        }
        for (int concurrencyLevel = 1; concurrencyLevel <= 128; concurrencyLevel *= 4) {
            for (int threadCount = 1; threadCount <= 128; threadCount *= 4) {
                testPerformance(threadCount, concurrencyLevel, true);
            }
        }
    }

    private static void testPerformance(int threadCount, int concurrencyLevel,
            boolean lirs) throws Exception {
        final Cache<Integer, Integer> cache = buildCache(lirs, concurrencyLevel);
        Thread[] threads = new Thread[threadCount];
        final AtomicBoolean stop = new AtomicBoolean();
        final AtomicInteger count = new AtomicInteger();
        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread() {
                @Override
                public void run() {
                    Random r = new Random();
                    while (!stop.get()) {
                        final int key = r.nextInt(20000);
                        try {
                            cache.get(key, new Callable<Integer>() {

                                @Override
                                public Integer call() throws Exception {
                                    return key;
                                }

                            });
                        } catch (ExecutionException e) {
                            count.set(Integer.MIN_VALUE);
                            stop.set(true);
                        }
                        count.incrementAndGet();
                    }
                }
            };
            t.start();
            threads[i] = t;
        }
        // test for 1000 ms
        Thread.sleep(1000);
        stop.set(true);
        System.out.println("threads " + threadCount +
                " concurrency " + concurrencyLevel + " ops/s " + count + 
                " " + (lirs ? "lirs" : "guava"));
        for (Thread t : threads) {
            t.join(1000);
            // if the thread is still alive after 1 second, we assume
            // there is a deadlock - we just let the threads alive,
            // but report a failure (what else could we do?)
            if (t.isAlive()) {
                assertFalse("Deadlock detected!", t.isAlive());
            }
        }
    }

}
