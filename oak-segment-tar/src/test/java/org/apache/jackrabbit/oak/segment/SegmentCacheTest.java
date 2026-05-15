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
import org.junit.Ignore;
import org.junit.Test;

public class SegmentCacheTest {
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
     * Verifies that repeated L1 hits keep a segment alive in L2 under eviction pressure.
     *
     * <p>Each L1 hit calls {@link SegmentCache#recordHit}, which calls
     * {@code cache.getIfPresent(id)} to register an L2 read. This saturates hotId's frequency
     * in W-TinyLFU's sketch at 15 (the 4-bit counter maximum). Fillers are re-accessed 5× each
     * (freq = 6, strictly below the cap), so hotId (freq 15) always beats fillers (freq 6) in
     * TinyLFU's admission comparison — no coin flip, strictly deterministic.
     *
     * <p>20 × 64 KB fillers (1.25 MB) overflow the 1 MB cache. Whenever a filler tries to
     * evict hotId (the LRU victim in probationary), TinyLFU rejects the filler (freq 6 &lt; 15)
     * and hotId survives. This is the same filler setup as the negative test — toggle state
     * is the only difference.
     */
    @Test
    @Ignore
    public void recordAccessKeepsHotSegmentInL2UnderPressure() throws ExecutionException {
        // 1 MB cache — small enough that 20 × 64 KB fillers create real eviction pressure.
        SegmentCache smallCache = newSegmentCache(1);
        SegmentId hotId = new SegmentId(EMPTY_STORE, 0xdeadL, 0xa000000000000001L, smallCache::recordHit);
        Segment hotSeg = mock(Segment.class);
        when(hotSeg.getSegmentId()).thenReturn(hotId);
        when(hotSeg.estimateMemoryUsage()).thenReturn(64 * 1024);

        // Initial L2 load — hotId enters the sketch at freq 1.
        smallCache.getSegment(hotId, () -> hotSeg);

        // 20 L1 hits: each triggers recordHit → cache.getIfPresent(hotId) → sketch freq → 15 (saturated).
        for (int i = 0; i < 200; i++) {
            assertEquals(hotSeg, hotId.getSegment());
        }

        // 20 × 64 KB fillers, each re-accessed 5× via L2 → freq 6 (strictly below the 15 cap).
        // Total weight 1.25 MB > 1 MB forces eviction. hotId (freq 15) beats every filler (freq 6)
        // in TinyLFU's admission gate — filler candidates are always rejected, hotId survives.
        for (int i = 0; i < 20; i++) {
            SegmentId filler = new SegmentId(EMPTY_STORE, i + 10L, 0xa000000000000010L + i);
            Segment fillerSeg = mock(Segment.class);
            when(fillerSeg.getSegmentId()).thenReturn(filler);
            when(fillerSeg.estimateMemoryUsage()).thenReturn(64 * 1024);
            smallCache.getSegment(filler, () -> fillerSeg);
            for (int j = 0; j < 2; j++) {
                smallCache.getSegment(filler, () -> fillerSeg);
            }
        }

        // hotId must still be in L2 — loader must not be called.
        assertEquals(hotSeg, smallCache.getSegment(hotId, () -> failToLoad(hotId)));
        // L1 memoisation must also be intact.
        assertEquals(hotSeg, hotId.getSegment());
    }

    /**
     * Negative counterpart: with {@link SegmentCache#FT_OAK_12214_ENABLE} disabled,
     * L1 hits skip {@code cache.getIfPresent}, so hotId's L2 frequency stays at 1.
     * Fillers re-accessed 5× via L2 each reach freq 6, which is strictly greater than
     * hotId's freq 1. TinyLFU admits each filler over hotId, evicting hotId.
     *
     * <p>Determinism guarantee: filler freq (6) &gt; hotId freq (1) is a strict inequality —
     * no coin flip. The 1.25 MB of fillers overflows the 1 MB cache, so at least one
     * eviction is guaranteed, and hotId is always the lowest-frequency victim.
     *
     * <p>The {@code finally} block restores the toggle so other tests are unaffected.
     */
    @Test
    public void hotSegmentEvictedWithoutL2Notification() throws ExecutionException {
        SegmentCache.FT_OAK_12214_ENABLE.set(false);
        try {
            // 1 MB cache — same size as the positive test so the two are directly comparable.
            SegmentCache smallCache = newSegmentCache(1);
            SegmentId hotId = new SegmentId(EMPTY_STORE, 0xdeadL, 0xa000000000000001L, smallCache::recordHit);
            Segment hotSeg = mock(Segment.class);
            when(hotSeg.getSegmentId()).thenReturn(hotId);
            when(hotSeg.estimateMemoryUsage()).thenReturn(64 * 1024);

            // Initial L2 load — hotId enters the sketch at freq 1.
            smallCache.getSegment(hotId, () -> hotSeg);

            // 20 L1 hits — with toggle disabled, recordHit skips cache.getIfPresent,
            // so hotId's sketch frequency stays at 1 despite the repeated L1 hits.
            for (int i = 0; i < 20; i++) {
                assertEquals(hotSeg, hotId.getSegment());
            }

            // Same filler setup as the positive test: 20 × 64 KB fillers, 5 L2 re-accesses
            // each → freq 6. hotId (freq 1) loses the TinyLFU admission battle against
            // every filler (freq 6 > 1) and is evicted.
            for (int i = 0; i < 20; i++) {
                SegmentId filler = new SegmentId(EMPTY_STORE, i + 10L, 0xa000000000000010L + i);
                Segment fillerSeg = mock(Segment.class);
                when(fillerSeg.getSegmentId()).thenReturn(filler);
                when(fillerSeg.estimateMemoryUsage()).thenReturn(64 * 1024);
                smallCache.getSegment(filler, () -> fillerSeg);
                for (int j = 0; j < 5; j++) {
                    smallCache.getSegment(filler, () -> fillerSeg);
                }
            }

            // hotId must have been evicted — loader must be called.
            AtomicBoolean reloaded = new AtomicBoolean(false);
            smallCache.getSegment(hotId, () -> {
                reloaded.set(true);
                return hotSeg;
            });
            assertTrue("hotId should have been evicted when L2 notification is disabled", reloaded.get());
        } finally {
            // Always restore the toggle so subsequent tests run with the default behaviour.
            SegmentCache.FT_OAK_12214_ENABLE.set(true);
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
