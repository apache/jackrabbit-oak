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

import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.SegmentCache.newSegmentCache;
import static org.apache.jackrabbit.oak.segment.SegmentStore.EMPTY_STORE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.junit.Before;
import org.junit.Test;

/**
 * {@link SegmentCache} unit tests. L1/L2 behaviour assertions that depend on admission/eviction
 * policy assume the default Oak build (Caffeine-backed {@code oak.cache} segment cache); the Guava
 * cache implementation is not covered here.
 */
public class SegmentCacheTest {

    @Before
    public void resetOak12214Toggle() {
        SegmentCache.FT_OAK_12214_PROPAGATE_L1_HITS_TO_L2_ENABLED.set(true);
    }

    private final SegmentCache cache = newSegmentCache(DEFAULT_SEGMENT_CACHE_MB);

    private final SegmentId id1 = new SegmentId(EMPTY_STORE, 0x0000000000000001L, 0xa000000000000001L, cache::recordHit);
    private final Segment segment1 = mock(Segment.class);
    private final SegmentId id2 = new SegmentId(EMPTY_STORE, 0x0000000000000002L, 0xa000000000000002L, cache::recordHit);
    private final Segment segment2 = mock(Segment.class);
    private final SegmentId id3 = new SegmentId(EMPTY_STORE, 0x0000000000000003L, 0xa000000000000003L, cache::recordHit);
    private final Segment segment3 = mock(Segment.class);

    {
        when(segment1.getSegmentId()).thenReturn(id1);
        when(segment1.estimateMemoryUsage()).thenReturn(1);
        when(segment2.getSegmentId()).thenReturn(id2);
        when(segment2.estimateMemoryUsage()).thenReturn(2);
        when(segment3.getSegmentId()).thenReturn(id3);
        when(segment3.estimateMemoryUsage()).thenReturn(DEFAULT_SEGMENT_CACHE_MB * 1024 * 1024);
    }

    @Test(expected = SegmentNotFoundException.class)
    public void snfeFromUncachedSegment() {
        id1.getSegment();
    }

    @Test
    public void putTest() throws ExecutionException {
        cache.putSegment(segment1);

        // Segment should be memoised with its id
        assertEquals(segment1, id1.getSegment());

        // Segment should be cached with the segmentId and thus not trigger a call
        // to the (empty) node store.
        assertEquals(segment1, cache.getSegment(id1, () -> failToLoad(id1)));
    }

    @Test
    public void getSegmentWrapsCheckedLoaderFailureInExecutionException() {
        Exception failure = new Exception("load failed");

        try {
            cache.getSegment(id1, () -> {
                throw failure;
            });
            fail("expected ExecutionException");
        } catch (ExecutionException e) {
            assertEquals(failure, e.getCause());
            assertEquals("load failed", e.getCause().getMessage());
        }
    }

    @Test
    public void getSegmentWrapsRuntimeLoaderFailureWithOriginalCause() throws ExecutionException {
        RepositoryNotReachableException failure = new RepositoryNotReachableException(null);

        try {
            cache.getSegment(id1, () -> {
                throw failure;
            });
            fail("expected RuntimeException");
        } catch (RuntimeException e) {
            assertSame(failure, e.getCause());
        }
    }

    @Test
    public void invalidateTests() throws ExecutionException {
        cache.putSegment(segment1);
        assertEquals(segment1, id1.getSegment());
        assertEquals(segment1, cache.getSegment(id1, () -> failToLoad(id1)));

        // Clearing the cache should cause an eviction call back for id
        cache.clear();

        // Check eviction cleared memoisation
        expect(SegmentNotFoundException.class, id1::getSegment);

        // Check that segment1 was evicted and needs reloading through the node store
        AtomicBoolean cached = new AtomicBoolean(true);
        assertEquals(segment1, cache.getSegment(id1, () -> {
            cached.set(false);
            return segment1;
        }));
        assertFalse(cached.get());

        // Assert that segment1 was loaded again
        assertEquals(segment1, id1.getSegment());
        assertEquals(segment1, cache.getSegment(id1, () -> failToLoad(id1)));
    }

    @Test
    public void evictionDuringPut() throws ExecutionException {
        cache.putSegment(segment3);

        // Check eviction cleared memoisation
        expect(SegmentNotFoundException.class, id3::getSegment);

        // Check that segment3 was evicted inside put because of its size and needs
        // reloading through the node store
        AtomicBoolean cached = new AtomicBoolean(true);
        assertEquals(segment3, cache.getSegment(id3, () -> {
            cached.set(false);
            return segment3;
        }));
        assertFalse(cached.get());
    }

    @Test
    public void evictionDuringLoad() throws ExecutionException {
        cache.getSegment(id3, () -> segment3);

        // Check eviction cleared memoisation
        expect(SegmentNotFoundException.class, id3::getSegment);

        // Check that segment3 was evicted inside put because of its size and needs
        // reloading through the node store
        AtomicBoolean cached = new AtomicBoolean(true);
        assertEquals(segment3, cache.getSegment(id3, () -> {
            cached.set(false);
            return segment3;
        }));
        assertFalse(cached.get());
    }

    /**
     * Verifies that repeated L1 hits keep a hot segment in L2 under eviction pressure.
     *
     * <p><strong>Contract asserted:</strong> after filling a 1&nbsp;MB cache and many L1 hits on
     * {@code hotId}, {@link SegmentCache#getSegment(SegmentId, Callable)} must still return the
     * memoised segment without calling the loader (no {@link SegmentNotFoundException} on L1).
     *
     * <p><strong>Implementation note:</strong> Oak's default segment cache uses Caffeine; this
     * workload relies on feeding L2 on each L1 hit so the hot entry is not chosen for eviction
     * when a sixteenth entry is added. Caffeine's internal SLRU/TinyLFU details may change across
     * versions; if this test becomes unstable, prefer tightening the scenario or asserting via
     * {@link AbstractCacheStats} rather than internal queue names.
     */
    @Test
    public void recordAccessKeepsHotSegmentInL2UnderPressure() throws ExecutionException {
        // 1 MB cache; 15 × 64 KB = 983 KB fits, 16th entry forces an eviction.
        SegmentCache smallCache = newSegmentCache(1);
        SegmentId hotId = new SegmentId(EMPTY_STORE, 0xdeadL, 0xa000000000000001L, smallCache::recordHit);
        Segment hotSeg = mock(Segment.class);
        when(hotSeg.getSegmentId()).thenReturn(hotId);
        when(hotSeg.estimateMemoryUsage()).thenReturn(64 * 1024);

        // Load hotId first — it becomes the probationary LRU (oldest, eviction candidate).
        smallCache.getSegment(hotId, () -> hotSeg);

        // Fill the rest of the cache with 14 fillers (no re-accesses), all in probationary.
        for (int i = 0; i < 14; i++) {
            SegmentId filler = new SegmentId(EMPTY_STORE, i + 10L, 0xa000000000000010L + i);
            Segment fillerSeg = mock(Segment.class);
            when(fillerSeg.getSegmentId()).thenReturn(filler);
            when(fillerSeg.estimateMemoryUsage()).thenReturn(64 * 1024);
            smallCache.getSegment(filler, () -> fillerSeg);
        }

        long hitCountBeforeL1 = smallCache.getCacheStats().getHitCount();
        // 20 L1 hits after the cache is full: each calls recordHit (and getIfPresent when toggle on).
        for (int i = 0; i < 20; i++) {
            assertEquals(hotSeg, hotId.getSegment());
        }
        assertEquals("each L1 hit should increment segment cache hit stats", 20,
                smallCache.getCacheStats().getHitCount() - hitCountBeforeL1);

        // Add a 16th entry to force eviction pressure; hotId must still be served from cache + L1.
        SegmentId trigger = new SegmentId(EMPTY_STORE, 999L, 0xa000000000000999L);
        Segment triggerSeg = mock(Segment.class);
        when(triggerSeg.getSegmentId()).thenReturn(trigger);
        when(triggerSeg.estimateMemoryUsage()).thenReturn(64 * 1024);
        smallCache.getSegment(trigger, () -> triggerSeg);

        // hotId must still be in L2 — loader must not be called.
        assertEquals(hotSeg, smallCache.getSegment(hotId, () -> failToLoad(hotId)));
        // L1 memoisation must also be intact.
        assertEquals(hotSeg, hotId.getSegment());
    }

    /**
     * With {@link SegmentCache#FT_OAK_12214_PROPAGATE_L1_HITS_TO_L2_ENABLED} disabled, L1 hits do not touch L2, so repeated
     * L1 reads do not refresh eviction policy for {@code hotId}. Under churn (each iteration loads a
     * new 64&nbsp;KB segment while the cache stays at capacity), {@code hotId} is eventually evicted
     * from L2 and must be reloaded via the loader.
     *
     * <p>The {@code finally} block restores the toggle so other tests are unaffected.
     */
    @Test
    public void hotSegmentEvictedWithoutL2Notification() throws ExecutionException {
        SegmentCache.FT_OAK_12214_PROPAGATE_L1_HITS_TO_L2_ENABLED.set(false);
        try {
            // 1 MB cache — same size as the positive test.
            SegmentCache smallCache = newSegmentCache(1);
            SegmentId hotId = new SegmentId(EMPTY_STORE, 0xdeadL, 0xa000000000000001L, smallCache::recordHit);
            Segment hotSeg = mock(Segment.class);
            when(hotSeg.getSegmentId()).thenReturn(hotId);
            when(hotSeg.estimateMemoryUsage()).thenReturn(64 * 1024);

            smallCache.getSegment(hotId, () -> hotSeg);

            for (int i = 0; i < 14; i++) {
                SegmentId filler = new SegmentId(EMPTY_STORE, i + 10L, 0xa000000000000010L + i);
                Segment fillerSeg = mock(Segment.class);
                when(fillerSeg.getSegmentId()).thenReturn(filler);
                when(fillerSeg.estimateMemoryUsage()).thenReturn(64 * 1024);
                smallCache.getSegment(filler, () -> fillerSeg);
            }

            for (int i = 0; i < 20; i++) {
                assertEquals(hotSeg, hotId.getSegment());
            }

            AtomicBoolean reloaded = new AtomicBoolean(false);
            final int maxChurnRounds = 48;
            for (int round = 0; round < maxChurnRounds && !reloaded.get(); round++) {
                SegmentId probe = new SegmentId(EMPTY_STORE, 4000L + round, 0xa0000000000e0000L + round);
                Segment probeSeg = mock(Segment.class);
                when(probeSeg.getSegmentId()).thenReturn(probe);
                when(probeSeg.estimateMemoryUsage()).thenReturn(64 * 1024);
                smallCache.getSegment(probe, () -> probeSeg);
                smallCache.getSegment(hotId, () -> {
                    reloaded.set(true);
                    return hotSeg;
                });
            }
            assertTrue("hotId should have been evicted from L2 when notification is disabled", reloaded.get());
        } finally {
            SegmentCache.FT_OAK_12214_PROPAGATE_L1_HITS_TO_L2_ENABLED.set(true);
        }
    }

    @Test
    public void nonEmptyCacheStatsTest() throws Exception {
        AbstractCacheStats stats = cache.getCacheStats();

        // empty cache
        assertEquals(0, stats.getElementCount());
        assertEquals(0, stats.getLoadCount());
        assertEquals(0, stats.estimateCurrentWeight());
        assertEquals(0, stats.getHitCount());
        assertEquals(0, stats.getMissCount());
        assertEquals(0, stats.getRequestCount());
        assertEquals(0, stats.getEvictionCount());

        // load
        cache.getSegment(id1, () -> segment1);
        assertEquals(1, stats.getElementCount());
        assertEquals(1, stats.getLoadCount());
        assertEquals(33, stats.estimateCurrentWeight());
        assertEquals(0, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
        assertEquals(1, stats.getRequestCount());
        assertEquals(0, stats.getEvictionCount());

        // cache hit
        assertEquals(segment1, id1.getSegment());
        assertEquals(1, stats.getElementCount());
        assertEquals(1, stats.getLoadCount());
        assertEquals(33, stats.estimateCurrentWeight());
        assertEquals(1, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
        assertEquals(2, stats.getRequestCount());
        assertEquals(0, stats.getEvictionCount());

        cache.clear();
        assertEquals(0, stats.getElementCount());
        assertEquals(1, stats.getLoadCount());
        assertEquals(0, stats.estimateCurrentWeight());
        assertEquals(1, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
        assertEquals(2, stats.getRequestCount());
        assertEquals(1, stats.getEvictionCount());

        stats.resetStats();
        assertEquals(0, stats.getElementCount());
        assertEquals(0, stats.getLoadCount());
        assertEquals(0, stats.estimateCurrentWeight());
        assertEquals(0, stats.getHitCount());
        assertEquals(0, stats.getMissCount());
        assertEquals(0, stats.getRequestCount());
        assertEquals(0, stats.getEvictionCount());

        // Eviction during put
        cache.getSegment(id3, () -> segment3);
        assertEquals(0, stats.getElementCount());
        assertEquals(1, stats.getLoadCount());
        assertEquals(0, stats.estimateCurrentWeight());
        assertEquals(0, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
        assertEquals(1, stats.getRequestCount());
        assertEquals(1, stats.getEvictionCount());
    }

    @Test
    public void emptyCacheStatsTest() throws Exception {
        SegmentCache cache = newSegmentCache(0);
        AbstractCacheStats stats = cache.getCacheStats();

        // empty cache
        assertEquals(0, stats.getElementCount());
        assertEquals(0, stats.getLoadCount());
        assertEquals(0, stats.estimateCurrentWeight());
        assertEquals(0, stats.getHitCount());
        assertEquals(0, stats.getMissCount());
        assertEquals(0, stats.getRequestCount());
        assertEquals(0, stats.getEvictionCount());

        // load
        cache.getSegment(id1, () -> segment1);
        assertEquals(0, stats.getElementCount());
        assertEquals(1, stats.getLoadCount());
        assertEquals(0, stats.estimateCurrentWeight());
        assertEquals(0, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
        assertEquals(1, stats.getRequestCount());
        assertEquals(0, stats.getEvictionCount());

        // No cache hit
        try {
            id1.getSegment();
            fail(id1 + " should not be in the cache");
        } catch (SegmentNotFoundException expected) {}

        cache.clear();
        assertEquals(0, stats.getElementCount());
        assertEquals(1, stats.getLoadCount());
        assertEquals(0, stats.estimateCurrentWeight());
        assertEquals(0, stats.getHitCount());
        assertEquals(1, stats.getMissCount());
        assertEquals(1, stats.getRequestCount());
        assertEquals(0, stats.getEvictionCount());

        stats.resetStats();
        assertEquals(0, stats.getElementCount());
        assertEquals(0, stats.getLoadCount());
        assertEquals(0, stats.estimateCurrentWeight());
        assertEquals(0, stats.getHitCount());
        assertEquals(0, stats.getMissCount());
        assertEquals(0, stats.getRequestCount());
        assertEquals(0, stats.getEvictionCount());
    }

    private static void expect(Class<? extends Throwable> exceptionType, Callable<?> thunk) {
        try {
            thunk.call();
        } catch (Throwable e) {
            if (!exceptionType.isAssignableFrom(e.getClass())) {
                throw new AssertionError(
                        "Unexpected exception: " + e.getClass().getSimpleName() + ". " +
                                "Expected: " + exceptionType.getSimpleName(), e);
            } else {
                return;
            }
        }
        throw new AssertionError("Expected exception " +
                exceptionType.getSimpleName() + " not thrown");
    }

    private static Segment failToLoad(SegmentId id) {
        fail("Cache should not need to load " + id);
        return null;
    }

}
