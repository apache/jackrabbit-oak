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
package org.apache.jackrabbit.oak.cache;

import java.util.concurrent.ExecutionException;

import org.apache.jackrabbit.oak.cache.api.CacheStatsAdapter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link AbstractCacheStats} using {@link CacheLIRS} as the backing cache.
 * These assertions intentionally avoid third-party cache types so the same
 * tests can run across cache implementation changes.
 */
public class AbstractCacheStatsTest {

    private static final String CACHE_NAME = "testCache";
    private static final long MAX_WEIGHT = 1000;

    private CacheLIRS<String, String> cache;
    private CacheStatsAdapter stats;

    @Before
    public void setUp() {
        cache = new CacheLIRS<>(null, MAX_WEIGHT, 1, 1, 0, null, null, null);
        stats = new CacheStatsAdapter(cache.asOakCache(), CACHE_NAME, null, MAX_WEIGHT);
    }

    @Test
    public void getNameReturnsConstructorValue() {
        Assert.assertEquals(CACHE_NAME, stats.getName());
    }

    @Test
    public void hitCountIncreasesOnCacheHit() throws ExecutionException {
        cache.put("k1", "v1");
        cache.get("k1", () -> "v1"); // cache hit — callable not invoked
        Assert.assertEquals(1, stats.getHitCount());
    }

    @Test
    public void missCountIncreasesOnCacheMiss() {
        cache.getIfPresent("absent");
        Assert.assertEquals(1, stats.getMissCount());
    }

    @Test
    public void requestCountIsSumOfHitsAndMisses() throws ExecutionException {
        cache.put("k1", "v1");
        cache.get("k1", () -> "v1"); // hit
        cache.getIfPresent("absent"); // miss
        Assert.assertEquals(2, stats.getRequestCount());
        Assert.assertEquals(1, stats.getHitCount());
        Assert.assertEquals(1, stats.getMissCount());
    }

    @Test
    public void loadSuccessCountIncreasesOnCallableLoad() throws ExecutionException {
        cache.get("k1", () -> "v1"); // miss + load
        Assert.assertEquals(1, stats.getLoadSuccessCount());
        Assert.assertEquals(1, stats.getLoadCount());
    }

    @Test
    public void loadExceptionCountIncreasesOnFailedLoad() {
        try {
            cache.get("k1", () -> {
                throw new RuntimeException("load failed");
            });
        } catch (ExecutionException ignored) {
        }
        Assert.assertEquals(1, stats.getLoadExceptionCount());
        Assert.assertEquals(1, stats.getLoadCount());
        Assert.assertEquals(0, stats.getLoadSuccessCount());
    }

    @Test
    public void evictionCountIncreasesAfterCapacityEviction() {
        // LIRS needs warm-up: create cache of size 5 and add 30 entries to ensure evictions
        CacheLIRS<String, String> smallCache = CacheLIRS.<String, String>newBuilder()
                .maximumSize(5)
                .build();
        CacheStatsAdapter smallStats = new CacheStatsAdapter(smallCache.asOakCache(), "small", null, 5);
        for (int i = 0; i < 30; i++) {
            smallCache.put("k" + i, "v" + i);
        }
        Assert.assertTrue("evictionCount should be positive after capacity eviction",
                smallStats.getEvictionCount() > 0);
    }

    @Test
    public void maxTotalWeightReturnsConfiguredValue() {
        Assert.assertEquals(MAX_WEIGHT, stats.getMaxTotalWeight());
    }

    @Test
    public void elementCountReflectsCachedEntries() throws ExecutionException {
        cache.get("k1", () -> "v1");
        cache.get("k2", () -> "v2");
        Assert.assertEquals(2, stats.getElementCount());
    }

    @Test
    public void estimateCurrentWeightReturnsNegativeOneWhenNoWeigher() {
        Assert.assertEquals(-1, stats.estimateCurrentWeight());
    }

    @Test
    public void resetStatsClearsCountersButNotCacheContents() throws ExecutionException {
        cache.get("k1", () -> "v1"); // miss + load
        cache.get("k1", () -> "v1"); // hit
        cache.getIfPresent("absent"); // miss

        stats.resetStats();

        Assert.assertEquals(0, stats.getRequestCount());
        Assert.assertEquals(0, stats.getHitCount());
        Assert.assertEquals(0, stats.getMissCount());
        Assert.assertEquals(0, stats.getLoadCount());
        Assert.assertEquals(0, stats.getLoadSuccessCount());
        Assert.assertEquals(0, stats.getLoadExceptionCount());
        Assert.assertEquals(0, stats.getEvictionCount());
        Assert.assertEquals(0.0, stats.getLoadExceptionRate(), Double.MIN_VALUE);
        Assert.assertEquals(0, stats.getTotalLoadTime());
        // cache contents unchanged after reset
        Assert.assertEquals(1, stats.getElementCount());
    }

    @Test
    public void hitRateIsOneWhenAllAccessesAreHits() throws ExecutionException {
        cache.put("k1", "v1");
        cache.get("k1", () -> "v1"); // hit
        Assert.assertEquals(1.0, stats.getHitRate(), Double.MIN_VALUE);
    }

    @Test
    public void hitRateIsOneWhenNoRequestsYet() {
        // by convention, hit rate is 1.0 when there are no requests
        Assert.assertEquals(1.0, stats.getHitRate(), Double.MIN_VALUE);
    }

    @Test
    public void cacheInfoAsStringContainsRequiredFields() throws ExecutionException {
        cache.get("k1", () -> "v1");
        String info = stats.cacheInfoAsString();
        Assert.assertTrue("cacheInfoAsString should contain hitCount",   info.contains("hitCount="));
        Assert.assertTrue("cacheInfoAsString should contain missCount",  info.contains("missCount="));
        Assert.assertTrue("cacheInfoAsString should contain loadCount",  info.contains("loadCount="));
        Assert.assertTrue("cacheInfoAsString should contain elementCount", info.contains("elementCount=1"));
        Assert.assertTrue("cacheInfoAsString should contain maxWeight",  info.contains("maxWeight="));
    }

    @Test
    public void timeInWordsIncludesMinAndSec() {
        String result = AbstractCacheStats.timeInWords(0);
        Assert.assertNotNull(result);
        Assert.assertTrue("timeInWords should contain 'min'", result.contains("min"));
        Assert.assertTrue("timeInWords should contain 'sec'", result.contains("sec"));
    }

    @Test
    public void timeInWordsFormatsOneMinute() {
        long oneMinuteNanos = 60L * 1_000_000_000L;
        String result = AbstractCacheStats.timeInWords(oneMinuteNanos);
        Assert.assertTrue("1-minute duration should contain '1 min'", result.contains("1 min"));
    }

    @Test
    public void loadExceptionRateAfterMixedLoads() throws ExecutionException {
        cache.get("success", () -> "v1"); // success
        try {
            cache.get("failure", () -> {
                throw new RuntimeException("boom");
            });
        } catch (ExecutionException ignored) {
        }
        // 1 success + 1 exception = 2 loads; rate = 0.5
        Assert.assertEquals(0.5, stats.getLoadExceptionRate(), 0.001);
    }

    @Test
    public void loadExceptionRateIsZeroWhenNoLoads() {
        Assert.assertEquals(0.0, stats.getLoadExceptionRate(), Double.MIN_VALUE);
    }

    @Test
    public void totalLoadTimeIsPositiveAfterMeasuredLoad() throws ExecutionException {
        cache.get("k1", () -> {
            Thread.sleep(1);
            return "v1";
        });
        Assert.assertTrue("totalLoadTime should be > 0 after a measured load", stats.getTotalLoadTime() > 0);
    }

    @Test
    public void averageLoadPenaltyIsPositiveAfterMeasuredLoad() throws ExecutionException {
        cache.get("k1", () -> {
            Thread.sleep(1);
            return "v1";
        });
        Assert.assertTrue("averageLoadPenalty should be > 0 after a measured load",
                stats.getAverageLoadPenalty() > 0.0);
    }

    @Test
    public void missRateIsOneWhenAllAccessesAreMisses() {
        cache.getIfPresent("a");
        cache.getIfPresent("b");
        Assert.assertEquals(1.0, stats.getMissRate(), Double.MIN_VALUE);
    }

    @Test
    public void statsAreAccumulatedAcrossMultipleLoads() throws ExecutionException {
        cache.get("k1", () -> "v1");
        cache.get("k2", () -> "v2");
        cache.get("k3", () -> "v3");
        Assert.assertEquals(3, stats.getLoadSuccessCount());
        Assert.assertEquals(3, stats.getLoadCount());
    }
}
