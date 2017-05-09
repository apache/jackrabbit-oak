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
 *
 */

package org.apache.jackrabbit.oak.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.junit.Before;
import org.junit.Test;

public class CacheStatsTest {
    private static final String NAME = "cache stats";
    private static final int KEYS = 100;

    private final Weigher<Integer, Integer> weigher = new Weigher<Integer, Integer>() {
        @Override
        public int weigh(@Nonnull Integer key, @Nonnull Integer value) {
            return 1;
        }
    };

    private final Cache<Integer, Integer> cache = CacheBuilder.newBuilder()
                .recordStats()
                .maximumWeight(Long.MAX_VALUE)
                .weigher(weigher)
                .build();

    private final CacheStats cacheStats =
            new CacheStats(cache, NAME, weigher, Long.MAX_VALUE);

    private int misses;
    private int fails;
    private long loadTime;

    @Before
    public void setup() {
        for (int k = 0; k < KEYS; k++) {
            cache.put(k, k);
        }

        for (int k = 0; k < 100; k++) {
            final int key = 4 * k;
            try {
                cache.get(key, new Callable<Integer>() {
                    @Override
                    public Integer call() throws Exception {
                        long t0 = System.nanoTime();
                        try {
                            if (key % 10 == 0) {
                                fails++;
                                throw new Exception("simulated load failure");
                            } else {
                                misses++;
                                return key;
                            }
                        } finally {
                            loadTime += System.nanoTime() - t0;
                        }

                    }
                });
            } catch (ExecutionException ignore) { }
        }
    }

    @Test
    public void name() throws Exception {
        assertEquals(NAME, cacheStats.getName());
    }

    @Test
    public void getRequestCount() {
        assertEquals(KEYS, cacheStats.getRequestCount());
    }

    @Test
    public void getHitCount() {
        assertEquals(KEYS - misses - fails, cacheStats.getHitCount());
    }

    @Test
    public void getHitRate() {
        assertEquals((KEYS - (double) misses - fails)/KEYS, cacheStats.getHitRate(), Double.MIN_VALUE);
    }

    @Test
    public void getMissCount() {
        assertEquals(misses + fails, cacheStats.getMissCount());
    }

    @Test
    public void getMissRate() {
        assertEquals(((double)misses + fails)/KEYS, cacheStats.getMissRate(), Double.MIN_VALUE);
    }

    @Test
    public void getLoadCount() {
        assertEquals(misses + fails, cacheStats.getLoadCount());
    }

    @Test
    public void getLoadSuccessCount() {
        assertEquals(misses, cacheStats.getLoadSuccessCount());
    }

    @Test
    public void getLoadExceptionCount() {
        assertEquals(fails, cacheStats.getLoadExceptionCount());
    }

    @Test
    public void getLoadExceptionRate() {
        assertEquals((double)fails/(misses + fails), cacheStats.getLoadExceptionRate(), Double.MIN_VALUE);
    }

    @Test
    public void getTotalLoadTime() {
        assertTrue(loadTime <= cacheStats.getTotalLoadTime());
    }

    @Test
    public void getAverageLoadPenalty() {
        assertTrue(((double)loadTime/(misses + fails)) <= cacheStats.getAverageLoadPenalty());
    }

    @Test
    public void getEvictionCount() {
        assertEquals(0, cacheStats.getEvictionCount());
    }

    @Test
    public void getElementCount() {
        assertEquals(KEYS + misses, cacheStats.getElementCount());
    }

    @Test
    public void getMaxTotalWeight() {
        assertEquals(Long.MAX_VALUE, cacheStats.getMaxTotalWeight());
    }

    @Test
    public void estimateCurrentWeight() {
        assertEquals(KEYS + misses, cacheStats.estimateCurrentWeight());
    }

    @Test
    public void resetStats() {
        cacheStats.resetStats();
        assertEquals(0, cacheStats.getRequestCount());
        assertEquals(0, cacheStats.getHitCount());
        assertEquals(1.0, cacheStats.getHitRate(), Double.MIN_VALUE);
        assertEquals(0, cacheStats.getMissCount());
        assertEquals(0.0, cacheStats.getMissRate(), Double.MIN_VALUE);
        assertEquals(0, cacheStats.getLoadCount());
        assertEquals(0, cacheStats.getLoadSuccessCount());
        assertEquals(0, cacheStats.getLoadExceptionCount());
        assertEquals(0, cacheStats.getLoadExceptionRate(), Double.MIN_VALUE);
        assertEquals(0, cacheStats.getTotalLoadTime());
        assertEquals(0, cacheStats.getAverageLoadPenalty(), Double.MIN_VALUE);
        assertEquals(0, cacheStats.getEvictionCount());
        assertEquals(KEYS + misses, cacheStats.getElementCount());
        assertEquals(Long.MAX_VALUE, cacheStats.getMaxTotalWeight());
        assertEquals(KEYS + misses, cacheStats.estimateCurrentWeight());
    }
}
