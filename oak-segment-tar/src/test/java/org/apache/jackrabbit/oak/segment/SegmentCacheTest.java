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
import static org.apache.jackrabbit.oak.segment.SegmentStore.EMPTY_STORE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.junit.Test;

public class SegmentCacheTest {
    private final SegmentCache cache = new SegmentCache(DEFAULT_SEGMENT_CACHE_MB);

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

    @Test
    public void statsTest() throws Exception {
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
