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

package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.segment.RecordCache.newRecordCache;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Random;

import com.google.common.base.Supplier;
import com.google.common.cache.CacheStats;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.Before;
import org.junit.Test;

public class RecordCacheStatsTest {
    private static final String NAME = "cache stats";
    private static final int KEYS = 100;

    private final Random rnd = new Random();
    private final MemoryStore store = new MemoryStore();

    private final RecordCache<Integer> cache = newRecordCache(KEYS);
    private final RecordCacheStats cacheStats =
            new RecordCacheStats(NAME,
                new Supplier<CacheStats>() {
                    @Override public CacheStats get() { return cache.getStats(); }
                },
                new Supplier<Long>() {
                    @Override public Long get() { return cache.size(); }
                },
                new Supplier<Long>() {
                    @Override public Long get() { return cache.estimateCurrentWeight(); }
                });

    private int hits;

    public RecordCacheStatsTest() throws IOException {}

    private RecordId newRecordId() {
        return TestUtils.newRecordId(store.getSegmentIdProvider(), rnd);
    }

    @Before
    public void setup() {
        for (int k = 0; k < KEYS; k++) {
            cache.put(k, newRecordId());
        }

        for (int k = 0; k < 100; k++) {
            if (cache.get(4 * k) != null) {
                hits++;
            }
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
        assertEquals(hits, cacheStats.getHitCount());
    }

    @Test
    public void getHitRate() {
        assertEquals(((double)hits)/KEYS, cacheStats.getHitRate(), Double.MIN_VALUE);
    }

    @Test
    public void getMissCount() {
        assertEquals(KEYS - hits, cacheStats.getMissCount());
    }

    @Test
    public void getMissRate() {
        assertEquals((KEYS - (double)hits)/KEYS, cacheStats.getMissRate(), Double.MIN_VALUE);
    }

    @Test
    public void getLoadCount() {
        assertEquals(KEYS, cacheStats.getLoadCount());
    }

    @Test
    public void getLoadSuccessCount() {
        assertEquals(KEYS, cacheStats.getLoadSuccessCount());
    }

    @Test
    public void getLoadExceptionCount() {
        assertEquals(0, cacheStats.getLoadExceptionCount());
    }

    @Test
    public void getLoadExceptionRate() {
        assertEquals(0, cacheStats.getLoadExceptionRate(), Double.MIN_VALUE);
    }

    @Test
    public void getTotalLoadTime() {
        assertEquals(0, cacheStats.getTotalLoadTime());
    }

    @Test
    public void getAverageLoadPenalty() {
        assertEquals(0, cacheStats.getAverageLoadPenalty(), Double.MIN_VALUE);
    }

    @Test
    public void getEvictionCount() {
        assertEquals(0, cacheStats.getEvictionCount());
    }

    @Test
    public void getElementCount() {
        assertEquals(KEYS, cacheStats.getElementCount());
    }

    @Test
    public void getMaxTotalWeight() {
        assertEquals(-1, cacheStats.getMaxTotalWeight());
    }

    @Test
    public void estimateCurrentWeight() {
        assertEquals(KEYS, cacheStats.estimateCurrentWeight());
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
        assertEquals(KEYS, cacheStats.getElementCount());
        assertEquals(-1, cacheStats.getMaxTotalWeight());
        assertEquals(KEYS, cacheStats.estimateCurrentWeight());
    }
}
