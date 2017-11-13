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

import static org.apache.jackrabbit.oak.segment.CacheWeights.segmentWeight;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalNotification;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.segment.CacheWeights.SegmentCacheWeigher;

/**
 * A cache for {@link SegmentId#isDataSegmentId() data} {@link Segment}
 * instances by their {@link SegmentId}. This cache ignores {@link
 * SegmentId#isBulkSegmentId() bulk} segments.
 * <p>
 * Conceptually this cache serves as a 2nd level cache for segments. The 1st
 * level cache is implemented by memoising the segment in its id (see {@link
 * SegmentId#segment}. Every time an segment is evicted from this cache the
 * memoised segment is discarded (see {@link SegmentId#onAccess}.
 */
public class SegmentCache {

    /**
     * Default maximum weight of this cache in MB
     */
    public static final int DEFAULT_SEGMENT_CACHE_MB = 256;

    /**
     * Maximum weight of the items in this cache
     */
    private final long maximumWeight;

    /**
     * Cache of recently accessed segments
     */
    @Nonnull
    private final Cache<SegmentId, Segment> cache;

    /**
     * Statistics of this cache. Do to the special access patter (see class
     * comment), we cannot rely on {@link Cache#stats()}.
     */
    @Nonnull
    private final Stats stats = new Stats("Segment Cache");

    /**
     * Create a new segment cache of the given size.
     *
     * @param cacheSizeMB size of the cache in megabytes.
     */
    public SegmentCache(long cacheSizeMB) {
        this.maximumWeight = cacheSizeMB * 1024 * 1024;
        this.cache = CacheBuilder.newBuilder()
            .concurrencyLevel(16)
            .maximumWeight(maximumWeight)
            .weigher(new SegmentCacheWeigher())
            .removalListener(this::onRemove)
            .build();
    }

    /**
     * Removal handler called whenever an item is evicted from the cache.
     */
    private void onRemove(@Nonnull RemovalNotification<SegmentId, Segment> notification) {
        stats.evictionCount.incrementAndGet();
        if (notification.getValue() != null) {
            stats.currentWeight.addAndGet(-segmentWeight(notification.getValue()));
        }
        if (notification.getKey() != null) {
            notification.getKey().unloaded();
        }
    }

    /**
     * Retrieve an segment from the cache or load it and cache it if not yet in
     * the cache.
     *
     * @param id     the id of the segment
     * @param loader the loader to load the segment if not yet in the cache
     * @return the segment identified by {@code id}
     * @throws ExecutionException when {@code loader} failed to load an segment
     */
    @Nonnull
    public Segment getSegment(@Nonnull final SegmentId id, @Nonnull final Callable<Segment> loader) throws ExecutionException {
        if (id.isDataSegmentId()) {
            return cache.get(id, () -> {
                try {
                    long t0 = System.nanoTime();
                    Segment segment = loader.call();
                    stats.loadSuccessCount.incrementAndGet();
                    stats.loadTime.addAndGet(System.nanoTime() - t0);
                    stats.missCount.incrementAndGet();
                    stats.currentWeight.addAndGet(segmentWeight(segment));
                    id.loaded(segment);
                    return segment;
                } catch (Exception e) {
                    stats.loadExceptionCount.incrementAndGet();
                    throw e;
                }
            });
        } else {
            try {
                return loader.call();
            } catch (Exception e) {
                throw new ExecutionException(e);
            }
        }
    }

    /**
     * Put a segment into the cache. This method does nothing for {@link
     * SegmentId#isBulkSegmentId() bulk} segments.
     *
     * @param segment the segment to cache
     */
    public void putSegment(@Nonnull Segment segment) {
        SegmentId id = segment.getSegmentId();

        if (id.isDataSegmentId()) {
            // Putting the segment into the cache can cause it to be evicted
            // right away again. Therefore we need to call loaded and update
            // the current weight *before* putting the segment into the cache.
            // This ensures that the eviction call back is always called
            // *after* a call to loaded and that the current weight is only
            // decremented *after* it was incremented.
            id.loaded(segment);
            stats.currentWeight.addAndGet(segmentWeight(segment));
            cache.put(id, segment);
        }
    }

    /**
     * Clear all segment from the cache
     */
    public void clear() {
        cache.invalidateAll();
    }

    /**
     * @return Statistics for this cache.
     */
    @Nonnull
    public AbstractCacheStats getCacheStats() {
        return stats;
    }

    /**
     * Record a hit in this cache's underlying statistics.
     *
     * @see SegmentId#onAccess
     */
    public void recordHit() {
        stats.hitCount.incrementAndGet();
    }

    /**
     * We cannot rely on the statistics of the underlying Guava cache as all
     * cache hits are taken by {@link SegmentId#getSegment()} and thus never
     * seen by the cache.
     */
    private class Stats extends AbstractCacheStats {

        @Nonnull
        final AtomicLong currentWeight = new AtomicLong();

        @Nonnull
        final AtomicLong loadSuccessCount = new AtomicLong();

        @Nonnull
        final AtomicInteger loadExceptionCount = new AtomicInteger();

        @Nonnull
        final AtomicLong loadTime = new AtomicLong();

        @Nonnull
        final AtomicLong evictionCount = new AtomicLong();

        @Nonnull
        final AtomicLong hitCount = new AtomicLong();

        @Nonnull
        final AtomicLong missCount = new AtomicLong();

        protected Stats(@Nonnull String name) {
            super(name);
        }

        @Override
        protected CacheStats getCurrentStats() {
            return new CacheStats(
                hitCount.get(),
                missCount.get(),
                loadSuccessCount.get(),
                loadExceptionCount.get(),
                loadTime.get(),
                evictionCount.get()
            );
        }

        @Override
        public long getElementCount() {
            return cache.size();
        }

        @Override
        public long getMaxTotalWeight() {
            return maximumWeight;
        }

        @Override
        public long estimateCurrentWeight() {
            return currentWeight.get();
        }

    }

}
