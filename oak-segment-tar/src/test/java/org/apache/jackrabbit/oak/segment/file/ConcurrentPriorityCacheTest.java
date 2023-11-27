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

package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.segment.CacheWeights;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.lang.Integer.valueOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class ConcurrentPriorityCacheTest {

    private final int concurrency;

    @Parameterized.Parameters
    public static List<Integer> concurrencyLevels() {
        return Arrays.asList(1, 2, 4, 8, 16, 32);
    }

    public ConcurrentPriorityCacheTest(int concurrency) {
        this.concurrency = concurrency;
    }

    @Test
    public void concurrentReadWrite() throws ExecutionException, InterruptedException {
        final int SIZE = 16384;
        PriorityCache<String, Integer> cache = new PriorityCache<>(SIZE, 10);

        ExecutorService executor1 = Executors.newFixedThreadPool(concurrency);
        ExecutorService executor2 = Executors.newFixedThreadPool(concurrency);

        List<Future<Boolean>> putFutures = new ArrayList<>(SIZE);
        List<Future<Void>> getFutures = new ArrayList<>(SIZE);

        for (int k = 0; k < SIZE; k++) {
            int idx = k;
            putFutures.add(executor1.submit(
                    () -> cache.put("key-" + idx, idx, 0, (byte) 0)));
        }

        for (int k = 0; k < SIZE; k++) {
            int idx = k;
            getFutures.add(executor2.submit(() -> {
                if (putFutures.get(idx).get()) {
                    assertEquals(valueOf(idx), cache.get("key-" + idx, 0));
                    assertNull(cache.get("key-" + idx, 1));
                } else {
                    assertNull(cache.get("key-" + idx, 0));
                }
                return null;
            }));
        }

        for (Future<Void> future : getFutures) {
            future.get();
        }
    }

    @Test
    public void concurrentUpdateKey() throws ExecutionException, InterruptedException {
        PriorityCache<String, Byte> cache = new PriorityCache<>(1, 5);

        List<Byte> costs = new ArrayList<>(Byte.MAX_VALUE + 1);
        for (int k = 0; k <= Byte.MAX_VALUE; k++) {
            costs.add((byte) k);
        }
        Collections.shuffle(costs);

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Future<Boolean>> futures = new ArrayList<>(Byte.MAX_VALUE + 1);

        for (int k = 0; k <= Byte.MAX_VALUE; k++) {
            byte cost = costs.get(k);
            futures.add(executor.submit(() -> cache.put("key-" + cost, cost, 0, cost)));
        }

        for (Future<Boolean> future : futures) {
            future.get();
        }

        assertEquals( Byte.valueOf(Byte.MAX_VALUE), cache.get("key-" + Byte.MAX_VALUE, 0));
    }

    @Test
    public void concurrentUpdateWithNewGeneration() throws ExecutionException, InterruptedException {
        final int NUM_GENERATIONS = 256;
        PriorityCache<String, Integer> cache = new PriorityCache<>(1, 5);

        List<Integer> generations = new ArrayList<>(NUM_GENERATIONS);
        for (int k = 0; k < NUM_GENERATIONS; k++) {
            generations.add(k);
        }
        Collections.shuffle(generations);

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Future<Boolean>> futures = new ArrayList<>(NUM_GENERATIONS);

        for (int k = 0; k < NUM_GENERATIONS; k++) {
            int gen = generations.get(k);
            futures.add(executor.submit(() -> cache.put("key", gen, gen, (byte) 0)));
        }

        for (Future<Boolean> future : futures) {
            future.get();
        }

        assertEquals(Integer.valueOf(NUM_GENERATIONS - 1), cache.get("key", NUM_GENERATIONS-1));
    }

    @Test
    public void concurrentGenerationPurge() throws ExecutionException, InterruptedException {
        PriorityCache<String, Integer> cache = new PriorityCache<>(65536);

        for (int gen = 4; gen >= 0; gen--) {
            // Backward iteration avoids earlier generations are replaced with later ones
            for (int k = 0; k < 100; k++) {
                assumeTrue("All test keys are in the cache",
                        cache.put("key-" + gen + "-" + k, 0, gen, (byte) 0));
            }
        }

        assertEquals(500, cache.size());

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Future<Void>> futures = new ArrayList<>(concurrency * 4);

        for (int i = 0; i < concurrency; i++) {
            futures.add(executor.submit(() -> {
                cache.purgeGenerations(generation -> generation == 1);
                return null;
            }));
            futures.add(executor.submit(() -> {
                cache.purgeGenerations(generation -> generation == 4);
                return null;
            }));
            futures.add(executor.submit(() -> {
                cache.purgeGenerations(generation -> generation <= 1);
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            future.get();
        }

        assertEquals(200, cache.size());
    }

    @Test
    public void concurrentEvictionCount() throws ExecutionException, InterruptedException {
        Random rnd = new Random();
        PriorityCache<String, Integer> cache = new PriorityCache<>(128, 2, CacheWeights.noopWeigher());

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Future<Boolean>> futures = new ArrayList<>(256);

        for (int b = Byte.MIN_VALUE; b <= Byte.MAX_VALUE; b++) {
            int value = b;
            futures.add(executor.submit(() ->
                    cache.put("k-" + value + "-" + rnd.nextInt(1000), value, 0, (byte) value)));
        }

        int count = 0;
        for (Future<Boolean> future : futures) {
            if (future.get()) {
                count++;
            }
        }

        assertEquals(count, cache.size() + cache.getStats().evictionCount());
    }

    @Test
    public void concurrentLoadExceptionCount() throws ExecutionException, InterruptedException {
        Random rnd = new Random();
        PriorityCache<String, Integer> cache = new PriorityCache<>(16);

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Future<Boolean>> futures = new ArrayList<>(1000);

        for (int i = 0; i < 1000; i++) {
            int value = i;
            futures.add(executor.submit(() ->
                    cache.put("k-" + value + "-" + rnd.nextInt(1000), value, 0, (byte) 0)));
        }

        int success = 0;
        int failure = 0;

        for (Future<Boolean> future : futures) {
            if (future.get()) {
                success++;
            } else {
                failure++;
            }
        }

        assertEquals(0, cache.getStats().evictionCount());
        assertEquals(success, cache.size());
        assertEquals(failure, cache.getStats().loadExceptionCount());
    }

}
