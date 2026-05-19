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
package org.apache.jackrabbit.oak.cache.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheStatsSnapshot;
import org.apache.jackrabbit.oak.cache.api.LoadingCache;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.CacheStatsAdapter;
import org.apache.jackrabbit.oak.cache.impl.caffeine.CaffeineCacheAdapter;
import org.apache.jackrabbit.oak.cache.impl.caffeine.CaffeineLoadingCacheAdapter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link CacheBuilder}.
 */
public class CacheBuilderTest {

    /** CacheBuilder always creates a Caffeine-backed manual cache. */
    @Test
    public void buildCreatesCaffeineManualCache() {
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        Assert.assertTrue(cache instanceof CaffeineCacheAdapter);
    }

    /** CacheBuilder always creates a Caffeine-backed loading cache. */
    @Test
    public void buildCreatesCaffeineLoadingCache() {
        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .build(key -> "loaded-" + key);
        Assert.assertTrue(cache instanceof CaffeineLoadingCacheAdapter);
    }

    /** build() produces a manual cache that does not expose LoadingCache at runtime. */
    @Test
    public void buildReturnsManualCacheOnly() {
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .build();

        Assert.assertFalse("manual cache must not implement LoadingCache", cache instanceof LoadingCache);
    }

    /** Weigher and evictionListener are wired correctly. */
    @Test
    public void weigherAndEvictionListenerWiring() {
        AtomicReference<EvictionCause> capturedCause = new AtomicReference<>();

        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumWeight(1000)
                .weigher((k, v) -> k.length() + v.length())
                .evictionListener((k, v, cause) -> capturedCause.set(cause))
                .build();

        cache.put("key", "value");
        cache.invalidate("key");
        cache.cleanUp();

        Assert.assertEquals(EvictionCause.EXPLICIT, capturedCause.get());
    }

    /** build(CacheLoader) wraps checked loader failures in CompletionException. */
    @Test
    public void loadingCacheCheckedLoaderFailureThrowsCompletionException() {
        Exception loaderFailure = new Exception("load failed");

        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .build(k -> { throw loaderFailure; });

        try {
            cache.get("missing");
            Assert.fail("expected CompletionException");
        } catch (CompletionException e) {
            Assert.assertSame(loaderFailure, e.getCause());
        }
    }

    /** build(CacheLoader) propagates runtime loader failures directly. */
    @Test
    public void loadingCacheRuntimeFailureThrowsRuntimeException() {
        RuntimeException loaderFailure = new RuntimeException("load failed");

        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .build(k -> { throw loaderFailure; });

        try {
            cache.get("missing");
            Assert.fail("expected RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertSame(loaderFailure, e);
        }
    }

    /** get(K, Function) propagates mapping failures directly. */
    @Test
    public void getWithFunctionFailureThrowsRuntimeException() {
        RuntimeException mappingFailure = new RuntimeException("mapping failed");

        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .build();

        try {
            cache.get("missing", key -> {
                throw mappingFailure;
            });
            Assert.fail("expected RuntimeException");
        } catch (RuntimeException e) {
            Assert.assertSame(mappingFailure, e);
        }
    }

    /** Cache.get(K, Function) passes the cache key into the mapping function. */
    @Test
    public void getWithFunctionReceivesKey() {
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .build();

        Assert.assertEquals("value-missing", cache.get("missing", key -> "value-" + key));
    }

    /** LoadingCache.refresh(K) returns a future for the refresh operation. */
    @Test
    public void refreshReturnsFuture() {
        LoadingCache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .build(key -> "loaded-" + key);

        cache.get("missing");
        CompletableFuture<String> future = cache.refresh("missing");

        Assert.assertNotNull(future);
        Assert.assertEquals("loaded-missing", future.join());
    }

    /** stats() returns non-null CacheStatsSnapshot with correct hit/miss counts. */
    @Test
    public void statsReturnsCorrectHitMissCounts() {
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();

        cache.put("k", "v");
        cache.getIfPresent("k");
        cache.getIfPresent("missing");

        CacheStatsSnapshot stats = cache.stats();
        Assert.assertNotNull(stats);
        Assert.assertEquals(1, stats.hitCount());
        Assert.assertEquals(1, stats.missCount());
    }

    /** Zero-size caches evict written entries before they are visible to later reads. */
    @Test
    public void zeroMaximumSizeEvictsImmediately() {
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumSize(0)
                .build();

        cache.put("key", "value");

        Assert.assertNull(cache.getIfPresent("key"));
        Assert.assertEquals(0, cache.estimatedSize());
    }

    /** Zero-weight caches evict written entries before they are visible to later reads. */
    @Test
    public void zeroMaximumWeightEvictsImmediately() {
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumWeight(0)
                .weigher((k, v) -> 1)
                .build();

        cache.put("key", "value");

        Assert.assertNull(cache.getIfPresent("key"));
        Assert.assertEquals(0, cache.estimatedSize());
    }

    /** Invalid builder combinations are rejected before backend-specific build logic runs. */
    @Test
    public void buildRejectsInvalidConfigurations() {
        assertInvalidBuild(
                CacheBuilder.<String, String>newBuilder(),
                "Either maximumSize or maximumWeight must be configured");
        assertInvalidBuild(
                CacheBuilder.<String, String>newBuilder().maximumSize(10).maximumWeight(20).weigher((k, v) -> 1),
                "maximumSize and maximumWeight are mutually exclusive");
        assertInvalidBuild(
                CacheBuilder.<String, String>newBuilder().maximumWeight(10),
                "maximumWeight requires weigher");
        assertInvalidBuild(
                CacheBuilder.<String, String>newBuilder().maximumSize(10).weigher((k, v) -> 1),
                "weigher requires maximumWeight");
        assertInvalidBuild(
                CacheBuilder.<String, String>newBuilder().maximumSize(10).refreshAfterWrite(Duration.ofSeconds(1)),
                "refreshAfterWrite requires build(CacheLoader)");
        try {
            CacheBuilder.<String, String>newBuilder().maximumSize(10).build(null);
            Assert.fail("expected NullPointerException");
        } catch (NullPointerException e) {
            // expected
        }
    }

    /** CacheStatsAdapter exposes stats and live weight estimates from a Cache. */
    @Test
    public void oakCacheStatsAdapterBridgesOakStats() {
        Cache<String, String> cache = CacheBuilder.<String, String>newBuilder()
                .maximumWeight(100)
                .weigher((k, v) -> k.length() + v.length())
                .recordStats()
                .build();
        CacheStatsAdapter stats = new CacheStatsAdapter(
                cache, "testCache", (k, v) -> k.length() + v.length(), 100);

        cache.put("aa", "bbb");
        cache.getIfPresent("aa");
        cache.getIfPresent("missing");

        Assert.assertEquals(1, stats.getHitCount());
        Assert.assertEquals(1, stats.getMissCount());
        Assert.assertEquals(1, stats.getElementCount());
        Assert.assertEquals(5, stats.estimateCurrentWeight());
        Assert.assertEquals(100, stats.getMaxTotalWeight());

        stats.resetStats();
        cache.getIfPresent("aa");
        Assert.assertEquals(1, stats.getHitCount());
    }

    private static void assertInvalidBuild(CacheBuilder<String, String> builder, String expectedMessagePart) {
        try {
            builder.build();
            Assert.fail("expected IllegalArgumentException containing: " + expectedMessagePart);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("unexpected message: " + e.getMessage(),
                    e.getMessage().contains(expectedMessagePart));
        }
    }
}
