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

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link OakCacheBuilder}.
 */
public class OakCacheBuilderTest {

    private String savedCacheType;

    @Before
    public void saveSystemProperty() {
        savedCacheType = System.getProperty("oak.cache.type");
    }

    @After
    public void restoreSystemProperty() {
        if (savedCacheType == null) {
            System.clearProperty("oak.cache.type");
        } else {
            System.setProperty("oak.cache.type", savedCacheType);
        }
    }

    /** LIRS backend is used when global property is set to "lirs". */
    @Test
    public void buildViaGlobalPropertyLirs() {
        System.setProperty("oak.cache.type", "lirs");
        OakCache<String, String> cache = OakCacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        Assert.assertTrue(cache instanceof LirsCacheAdapter);
    }

    /** Caffeine backend is used when global property is set to "caffeine". */
    @Test
    public void buildViaGlobalPropertyCaffeine() {
        System.setProperty("oak.cache.type", "caffeine");
        OakCache<String, String> cache = OakCacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        Assert.assertTrue(cache instanceof CaffeineCacheAdapter);
    }

    /** Per-instance CAFFEINE override wins over lirs global property. */
    @Test
    public void perInstanceCaffeineOverridesLirsGlobal() {
        System.setProperty("oak.cache.type", "lirs");
        OakCache<String, String> cache = OakCacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .implementation(CacheImplementation.CAFFEINE)
                .build();
        Assert.assertTrue(cache instanceof CaffeineCacheAdapter);
    }

    /** Per-instance LIRS override wins over caffeine global property. */
    @Test
    public void perInstanceLirsOverridesCaffeineGlobal() {
        System.setProperty("oak.cache.type", "caffeine");
        OakCache<String, String> cache = OakCacheBuilder.<String, String>newBuilder()
                .maximumSize(10)
                .implementation(CacheImplementation.LIRS)
                .build();
        Assert.assertTrue(cache instanceof LirsCacheAdapter);
    }

    /** build() produces a manual cache that does not expose OakLoadingCache at runtime. */
    @Test
    public void buildReturnsManualCacheOnly() {
        for (CacheImplementation impl : CacheImplementation.values()) {
            OakCache<String, String> cache = OakCacheBuilder.<String, String>newBuilder()
                    .maximumSize(10)
                    .implementation(impl)
                    .build();

            Assert.assertFalse("manual cache must not implement OakLoadingCache for impl " + impl,
                    cache instanceof OakLoadingCache);
        }
    }

    /** Weigher and removalListener are wired correctly for both backends. */
    @Test
    public void weigherAndRemovalListenerWiring() {
        AtomicReference<OakRemovalCause> capturedCause = new AtomicReference<>();

        for (CacheImplementation impl : CacheImplementation.values()) {
            capturedCause.set(null);
            OakCache<String, String> cache = OakCacheBuilder.<String, String>newBuilder()
                    .maximumWeight(1000)
                    .weigher((k, v) -> k.length() + v.length())
                    .removalListener((k, v, cause) -> capturedCause.set(cause))
                    .implementation(impl)
                    .build();

            cache.put("key", "value");
            cache.invalidate("key");
            // Caffeine processes removal notifications lazily; cleanUp() drains the pending queue.
            cache.cleanUp();

            Assert.assertEquals("expected EXPLICIT cause for impl " + impl,
                    OakRemovalCause.EXPLICIT, capturedCause.get());
        }
    }

    /** build(OakCacheLoader) wraps checked loader failures in ExecutionException. */
    @Test
    public void loadingCacheCheckedLoaderFailureThrowsExecutionException() {
        Exception loaderFailure = new Exception("load failed");

        for (CacheImplementation impl : CacheImplementation.values()) {
            OakLoadingCache<String, String> cache = OakCacheBuilder.<String, String>newBuilder()
                    .maximumSize(10)
                    .implementation(impl)
                    .build(k -> { throw loaderFailure; });

            try {
                cache.get("missing");
                Assert.fail("expected ExecutionException for impl " + impl);
            } catch (ExecutionException e) {
                Assert.assertSame("cause should be the loader exception for impl " + impl,
                        loaderFailure, e.getCause());
            }
        }
    }

    /** build(OakCacheLoader) preserves ExecutionException shape for runtime loader failures. */
    @Test
    public void loadingCacheRuntimeFailureThrowsExecutionException() {
        RuntimeException loaderFailure = new RuntimeException("load failed");

        for (CacheImplementation impl : CacheImplementation.values()) {
            OakLoadingCache<String, String> cache = OakCacheBuilder.<String, String>newBuilder()
                    .maximumSize(10)
                    .implementation(impl)
                    .build(k -> { throw loaderFailure; });

            try {
                cache.get("missing");
                Assert.fail("expected ExecutionException for impl " + impl);
            } catch (ExecutionException e) {
                Assert.assertSame("runtime failure cause should be preserved for impl " + impl,
                        loaderFailure, e.getCause());
            }
        }
    }

    /** get(K, Callable) preserves ExecutionException shape for mapping failures. */
    @Test
    public void getWithCallableFailureThrowsExecutionException() {
        RuntimeException mappingFailure = new RuntimeException("mapping failed");

        for (CacheImplementation impl : CacheImplementation.values()) {
            OakCache<String, String> cache = OakCacheBuilder.<String, String>newBuilder()
                    .maximumSize(10)
                    .implementation(impl)
                    .build();

            try {
                cache.get("missing", () -> {
                    throw mappingFailure;
                });
                Assert.fail("expected ExecutionException for impl " + impl);
            } catch (ExecutionException e) {
                Assert.assertSame("mapping failure cause should be preserved for impl " + impl,
                        mappingFailure, e.getCause());
            }
        }
    }

    /** stats() returns non-null OakCacheStats with correct hit/miss counts. */
    @Test
    public void statsReturnsCorrectHitMissCounts() {
        for (CacheImplementation impl : CacheImplementation.values()) {
            OakCache<String, String> cache = OakCacheBuilder.<String, String>newBuilder()
                    .maximumSize(10)
                    .recordStats()
                    .implementation(impl)
                    .build();

            cache.put("k", "v");
            cache.getIfPresent("k");        // hit
            cache.getIfPresent("missing");  // miss

            OakCacheStats stats = cache.stats();
            Assert.assertNotNull("stats must not be null for impl " + impl, stats);
            Assert.assertEquals("hit count for impl " + impl, 1, stats.hitCount());
            Assert.assertEquals("miss count for impl " + impl, 1, stats.missCount());
        }
    }

    /** Invalid builder combinations are rejected before backend-specific build logic runs. */
    @Test
    public void buildRejectsInvalidConfigurations() {
        assertInvalidBuild(
                OakCacheBuilder.<String, String>newBuilder(),
                "Either maximumSize or maximumWeight must be configured");
        assertInvalidBuild(
                OakCacheBuilder.<String, String>newBuilder().maximumSize(10).maximumWeight(20).weigher((k, v) -> 1),
                "maximumSize and maximumWeight are mutually exclusive");
        assertInvalidBuild(
                OakCacheBuilder.<String, String>newBuilder().maximumWeight(10),
                "maximumWeight requires weigher");
        assertInvalidBuild(
                OakCacheBuilder.<String, String>newBuilder().maximumSize(10).weigher((k, v) -> 1),
                "weigher requires maximumWeight");
        assertInvalidBuild(
                OakCacheBuilder.<String, String>newBuilder().maximumSize(10).refreshAfterWrite(Duration.ofSeconds(1)),
                "refreshAfterWrite requires build(OakCacheLoader)");
        assertInvalidBuild(
                OakCacheBuilder.<String, String>newBuilder().maximumWeight(10).weigher((k, v) -> 1)
                        .averageWeight((long) Integer.MAX_VALUE + 1L),
                "averageWeight must be less than or equal to Integer.MAX_VALUE");
        assertInvalidBuild(
                OakCacheBuilder.<String, String>newBuilder().maximumSize(10).averageWeight(10),
                "averageWeight requires maximumWeight");
    }

    /** OakCacheStatsAdapter exposes stats and live weight estimates from an OakCache. */
    @Test
    public void oakCacheStatsAdapterBridgesOakStats() {
        OakCache<String, String> cache = OakCacheBuilder.<String, String>newBuilder()
                .maximumWeight(100)
                .weigher((k, v) -> k.length() + v.length())
                .recordStats()
                .implementation(CacheImplementation.CAFFEINE)
                .build();
        OakCacheStatsAdapter stats = new OakCacheStatsAdapter(
                cache, "testCache", (k, v) -> k.toString().length() + v.toString().length(), 100);

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

    private static void assertInvalidBuild(OakCacheBuilder<String, String> builder, String expectedMessagePart) {
        try {
            builder.build();
            Assert.fail("expected IllegalArgumentException containing: " + expectedMessagePart);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("unexpected message: " + e.getMessage(),
                    e.getMessage().contains(expectedMessagePart));
        }
    }
}
