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
package org.apache.jackrabbit.oak.benchmark;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.apache.jackrabbit.guava.common.cache.CacheBuilder;
import org.apache.jackrabbit.guava.common.cache.CacheStats;
import org.apache.jackrabbit.guava.common.cache.RemovalNotification;
import org.apache.jackrabbit.guava.common.util.concurrent.UncheckedExecutionException;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.segment.CacheWeights;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link SegmentCache} backed by a Guava LRU cache, used in benchmark classes
 * to compare eviction policies against the default Caffeine W-TinyLFU implementation.
 * All Guava-specific code lives here; the production {@link SegmentCache} class stays clean.
 */
class GuavaSegmentCache extends SegmentCache {

    private static final String NAME = "Segment Cache (Guava)";

    private final org.apache.jackrabbit.guava.common.cache.Cache<SegmentId, Segment> cache;
    private final Stats stats;

    GuavaSegmentCache(long cacheSizeMb) {
        long maximumWeight = cacheSizeMb * 1024L * 1024L;
        // Build cache first so cache::size can be passed to Stats; the removal listener
        // references this.stats which is assigned below — safe because evictions only
        // fire after construction is complete (same pattern as production NonEmptyCache).
        this.cache = CacheBuilder.<SegmentId, Segment>newBuilder()
                .maximumWeight(maximumWeight)
                .weigher((SegmentId id, Segment seg) -> CacheWeights.segmentWeight(seg))
                .removalListener(this::onRemove)
                .build();
        this.stats = new Stats(NAME, maximumWeight, cache::size);
    }

    private void onRemove(RemovalNotification<SegmentId, Segment> n) {
        stats.evictionCount.incrementAndGet();
        if (n.getValue() != null) {
            stats.currentWeight.addAndGet(-CacheWeights.segmentWeight(n.getValue()));
        }
        n.getKey().unloaded();
    }

    @Override
    @NotNull
    public Segment getSegment(@NotNull SegmentId id, @NotNull Callable<Segment> loader)
            throws ExecutionException {
        if (id.isDataSegmentId()) {
            try {
                return cache.get(id, () -> {
                    long t0 = System.nanoTime();
                    try {
                        Segment segment = loader.call();
                        stats.loadSuccessCount.incrementAndGet();
                        stats.loadTime.addAndGet(System.nanoTime() - t0);
                        stats.missCount.incrementAndGet();
                        stats.currentWeight.addAndGet(CacheWeights.segmentWeight(segment));
                        id.loaded(segment);
                        return segment;
                    } catch (Exception e) {
                        stats.loadExceptionCount.incrementAndGet();
                        if (e instanceof RuntimeException re) throw re;
                        throw new LoaderException(e);
                    }
                });
            } catch (UncheckedExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof LoaderException le) {
                    throw new ExecutionException(le.getCause());
                }
                if (cause instanceof RuntimeException re) throw re;
                throw e;
            } catch (ExecutionException e) {
                throw new ExecutionException(e.getCause());
            }
        } else {
            try {
                return loader.call();
            } catch (Exception e) {
                throw new ExecutionException(e);
            }
        }
    }

    @Override
    public void putSegment(@NotNull Segment segment) {
        SegmentId id = segment.getSegmentId();
        if (id.isDataSegmentId()) {
            // Update before put() for correct ordering with eviction callback
            id.loaded(segment);
            stats.currentWeight.addAndGet(CacheWeights.segmentWeight(segment));
            cache.put(id, segment);
        }
    }

    @Override
    public void clear() {
        cache.invalidateAll();
        stats.currentWeight.set(0);
    }

    @Override
    public void cleanUp() {
        cache.cleanUp();
    }

    @Override
    @NotNull
    public AbstractCacheStats getCacheStats() {
        return stats;
    }

    @Override
    public void recordHit(@NotNull SegmentId id) {
        if (id.isDataSegmentId()) {
            if (FT_NOTIFY_L2_ON_L1_HIT.isEnabled()) {
                cache.getIfPresent(id);
            }
            stats.hitCount.incrementAndGet();
        }
    }

    private static final class LoaderException extends RuntimeException {
        LoaderException(Exception cause) {
            super(cause);
        }
    }

    private static final class Stats extends AbstractCacheStats {
        private final long maximumWeight;
        private final Supplier<Long> elementCount;

        final AtomicLong currentWeight = new AtomicLong();
        final AtomicLong loadSuccessCount = new AtomicLong();
        final AtomicInteger loadExceptionCount = new AtomicInteger();
        final AtomicLong loadTime = new AtomicLong();
        final AtomicLong evictionCount = new AtomicLong();
        final AtomicLong hitCount = new AtomicLong();
        final AtomicLong missCount = new AtomicLong();

        Stats(@NotNull String name, long maximumWeight, @NotNull Supplier<Long> elementCount) {
            super(name);
            this.maximumWeight = maximumWeight;
            this.elementCount = elementCount;
        }

        @Override
        protected CacheStats getCurrentStats() {
            return new CacheStats(
                    hitCount.get(),
                    missCount.get(),
                    loadSuccessCount.get(),
                    loadExceptionCount.get(),
                    loadTime.get(),
                    evictionCount.get());
        }

        @Override
        public long getElementCount() {
            return elementCount.get();
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
