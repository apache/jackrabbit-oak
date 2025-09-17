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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static org.apache.jackrabbit.oak.segment.CacheWeights.segmentWeight;

import java.io.Closeable;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.jackrabbit.guava.common.cache.Cache;
import org.apache.jackrabbit.guava.common.cache.CacheBuilder;
import org.apache.jackrabbit.guava.common.cache.CacheStats;
import org.apache.jackrabbit.guava.common.cache.RemovalNotification;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.segment.CacheWeights.SegmentCacheWeigher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A cache for {@link SegmentId#isDataSegmentId() data} {@link Segment}
 * instances by their {@link SegmentId}. This cache ignores {@link
 * SegmentId#isBulkSegmentId() bulk} segments.
 * <p>
 * Conceptually this cache serves as a 2nd level cache for segments. The 1st
 * level cache is implemented by memoising the segment in its id (see {@code
 * SegmentId#segment}. Every time an segment is evicted from this cache the
 * memoised segment is discarded (see {@code SegmentId#onAccess}.
 */
public abstract class SegmentCache implements Closeable {

    /**
     * Default maximum weight of this cache in MB
     */
    public static final int DEFAULT_SEGMENT_CACHE_MB = 256;

    private static final String NAME = "Segment Cache";

    /**
     * Create a new segment cache of the given size. Returns an always empty
     * cache for {@code cacheSizeMB <= 0}.
     *
     * @param cacheSizeMB     size of the cache in megabytes.
     */
    @NotNull
    public static SegmentCache newSegmentCache(int cacheSizeMB) {
        return new SegmentCache.Config()
                .withCacheSizeMB(cacheSizeMB)
                .build(null, null);
    }

    private static SegmentCache newSegmentCache(int cacheSizeMB, int prefetchThreads, int prefetchDepth, @Nullable Function<UUID, SegmentId> uuidToSegmentId, @Nullable Function<SegmentId, Segment> segmentLoader) {
        if (cacheSizeMB > 0 && prefetchThreads > 0 && prefetchDepth > 0 && uuidToSegmentId != null && segmentLoader != null) {
            return new PrefetchCache(cacheSizeMB, prefetchThreads, prefetchDepth, uuidToSegmentId, segmentLoader);
        } else if (cacheSizeMB > 0) {
            return new NonEmptyCache(cacheSizeMB);
        } else {
            return new EmptyCache();
        }
    }

    public static final class Config {

        private int cacheSizeMB = DEFAULT_SEGMENT_CACHE_MB;

        private int prefetchThreads = 0;

        private int prefetchDepth = 1;

        public Config withCacheSizeMB(int cacheSizeMB) {
            this.cacheSizeMB = cacheSizeMB;
            return this;
        }

        public Config withPrefetchThreads(int prefetchThreads) {
            this.prefetchThreads = prefetchThreads;
            return this;
        }

        public Config withPrefetchDepth(int prefetchDepth) {
            this.prefetchDepth = prefetchDepth;
            return this;
        }

        @NotNull
        public SegmentCache build(@Nullable Function<UUID, SegmentId> uuidToSegmentId, @Nullable Function<SegmentId, Segment> segmentLoader) {
            if (prefetchThreads > 0 && prefetchDepth > 0) {
                checkArgument(uuidToSegmentId != null, "uuidToSegmentId must be provided when prefetching is enabled");
                checkArgument(segmentLoader != null, "segmentLoader must be provided when prefetching is enabled");
            }
            return SegmentCache.newSegmentCache(cacheSizeMB, prefetchThreads, prefetchDepth, uuidToSegmentId, segmentLoader);
        }

        public int getSegmentCacheSize() {
            return cacheSizeMB;
        }

        @Override
        public String toString() {
            return "Config{" +
                    "cacheSizeMB=" + cacheSizeMB +
                    ", prefetchThreads=" + prefetchThreads +
                    ", prefetchDepth=" + prefetchDepth +
                    '}';
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
    @NotNull
    public abstract Segment getSegment(@NotNull SegmentId id, @NotNull Callable<Segment> loader)
    throws ExecutionException;

    /**
     * Put a segment into the cache. This method does nothing for {@link
     * SegmentId#isBulkSegmentId() bulk} segments.
     *
     * @param segment the segment to cache
     */
    public abstract void putSegment(@NotNull Segment segment);

    /**
     * Clear all segment from the cache
     */
    public abstract void clear();

    /**
     * @return Statistics for this cache.
     */
    @NotNull
    public abstract AbstractCacheStats getCacheStats();

    /**
     * Record a hit in this cache's underlying statistics.
     *
     * See {@code SegmentId#onAccess}
     */
    public abstract void recordHit();

    @Override
    public void close() {
        // nothing to do
    }

    private static class NonEmptyCache extends SegmentCache {

        /**
         * Cache of recently accessed segments
         */
        @NotNull
        private final Cache<SegmentId, Segment> cache;

        /**
         * Statistics of this cache. Do to the special access patter (see class
         * comment), we cannot rely on {@link Cache#stats()}.
         */
        @NotNull
        private final Stats stats;

        /**
         * Create a new cache of the given size.
         *
         * @param cacheSizeMB size of the cache in megabytes.
         */
        private NonEmptyCache(int cacheSizeMB) {
            long maximumWeight = cacheSizeMB * 1024 * 1024L;
            this.cache = CacheBuilder.newBuilder()
                    .concurrencyLevel(16)
                    .maximumWeight(maximumWeight)
                    .weigher(new SegmentCacheWeigher())
                    .removalListener(this::onRemove)
                    .build();
            this.stats = new Stats(NAME, maximumWeight, cache::size);
        }

        /**
         * Removal handler called whenever an item is evicted from the cache.
         */
        private void onRemove(@NotNull RemovalNotification<SegmentId, Segment> notification) {
            stats.evictionCount.incrementAndGet();
            if (notification.getValue() != null) {
                stats.currentWeight.addAndGet(-segmentWeight(notification.getValue()));
            }
            if (notification.getKey() != null) {
                notification.getKey().unloaded();
            }
        }

        @Override
        @NotNull
        public Segment getSegment(@NotNull SegmentId id, @NotNull Callable<Segment> loader) throws ExecutionException {
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

        @Override
        public void putSegment(@NotNull Segment segment) {
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

        @Override
        public void clear() {
            cache.invalidateAll();
        }

        @Override
        @NotNull
        public AbstractCacheStats getCacheStats() {
            return stats;
        }

        @Override
        public void recordHit() {
            stats.hitCount.incrementAndGet();
        }
    }


    private static class PrefetchCache extends NonEmptyCache {

        private static final Logger LOG = LoggerFactory.getLogger(PrefetchCache.class);

        private final ThreadPoolExecutor prefetchPool;

        private final int prefetchDepth;

        private final Function<UUID, SegmentId> uuidToSegmentId;

        private final Function<SegmentId, Segment> segmentLoader;

        /**
         * Create a new cache of the given size.
         *
         * @param cacheSizeMB      size of the cache in megabytes.
         * @param prefetchThreads  the number of threads to use for prefetching
         * @param prefetchDepth    the depth to prefetch
         * @param uuidToSegmentId  the function to get the SegmentId by its UUID
         */
        private PrefetchCache(int cacheSizeMB, int prefetchThreads, int prefetchDepth, @NotNull Function<UUID, SegmentId> uuidToSegmentId, @NotNull Function<SegmentId, Segment> segmentLoader) {
            super(cacheSizeMB);
            this.prefetchPool = new ThreadPoolExecutor(prefetchThreads, prefetchThreads,
                    30, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(prefetchThreads * 8),
                    r -> {
                        String threadName = String.format("segment-prefetch-%s",
                                Long.toHexString(System.nanoTime() & 0xFFFFF));
                        return new Thread(r, threadName) {
                            {
                                setUncaughtExceptionHandler((t, e) -> {
                                    if (!(e instanceof InterruptedException)) {
                                        LOG.warn("Uncaught exception in thread {}", t.getName(), e);
                                    }
                                });
                            }
                        };
                    },
                    new ThreadPoolExecutor.DiscardPolicy());
            this.prefetchPool.allowCoreThreadTimeOut(true);
            this.prefetchDepth = prefetchDepth;
            this.segmentLoader = segmentLoader;
            this.uuidToSegmentId = uuidToSegmentId;
        }

        @Override
        public @NotNull Segment getSegment(@NotNull SegmentId id, @NotNull Callable<Segment> loader) throws ExecutionException {
            return super.getSegment(id, () -> {
                Segment s = loader.call();
                if (s != null && id.isDataSegmentId()) {
                    prefetchPool.execute(new PrefetchRunnable(s, prefetchDepth));
                }
                return s;
            });
        }

        @Override
        public void close() {
            super.close();
            try {
                prefetchPool.shutdown();
                if (!prefetchPool.awaitTermination(10, TimeUnit.SECONDS)) {
                    prefetchPool.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                prefetchPool.shutdownNow();
            }
        }

        private class PrefetchRunnable implements Runnable {
            private final Segment segment;
            private final int depth;

            PrefetchRunnable(Segment segment, int depth) {
                this.segment = segment;
                this.depth = depth;
            }

            @Override
            public void run() {
                SegmentId segmentId = segment.getSegmentId();
                if (SegmentId.isDataSegmentId(segmentId.getLeastSignificantBits())) {
                    for (int i = 0; i < segment.getReferencedSegmentIdCount(); i++) {
                        UUID referencedSegmentId = segment.getReferencedSegmentId(i);
                        SegmentId id = uuidToSegmentId.apply(referencedSegmentId);
                        prefetchPool.execute(() -> {
                            try {
                                PrefetchCache.super.getSegment(id, () -> {
                                    Segment s = null;
                                    try {
                                        LOG.debug("Prefetching segment {}, referenced by segment {}", id, segmentId);
                                        return s = segmentLoader.apply(id);
                                    } catch (SegmentNotFoundException e) {
                                        LOG.warn("SegmentNotFoundException during prefetch of segment {}, referenced by segment {}", id, segmentId);
                                        throw e;
                                    } finally {
                                        if (s != null && depth > 0) {
                                            prefetchPool.execute(new PrefetchRunnable(s, depth - 1));
                                        }
                                    }
                                });
                            } catch (ExecutionException e) {
                                LOG.warn("Error prefetching segment {}", id, e);
                            }
                        });
                    }
                }
            }

            @Override
            public String toString() {
                return "PrefetchRunnable{segment=" + segment.getSegmentId() + '}';
            }
        }
    }

    /** An always empty cache */
    private static class EmptyCache extends SegmentCache {
        private final Stats stats = new Stats(NAME, 0, () -> 0L);

        @NotNull
        @Override
        public Segment getSegment(@NotNull SegmentId id, @NotNull Callable<Segment> loader)
        throws ExecutionException {
            long t0 = System.nanoTime();
            try {
                stats.missCount.incrementAndGet();
                Segment segment = loader.call();
                stats.loadSuccessCount.incrementAndGet();
                return segment;
            } catch (Exception e) {
                stats.loadExceptionCount.incrementAndGet();
                throw new ExecutionException(e);
            } finally {
                stats.loadTime.addAndGet(System.nanoTime() - t0);
            }
        }

        @Override
        public void putSegment(@NotNull Segment segment) {
            segment.getSegmentId().unloaded();
        }

        @Override
        public void clear() {}

        @NotNull
        @Override
        public AbstractCacheStats getCacheStats() {
            return stats;
        }

        @Override
        public void recordHit() {
            stats.hitCount.incrementAndGet();
        }
    }

    /**
     * We cannot rely on the statistics of the underlying Guava cache as all
     * cache hits are taken by {@link SegmentId#getSegment()} and thus never
     * seen by the cache.
     */
    private static class Stats extends AbstractCacheStats {
        private final long maximumWeight;

        @NotNull
        private final Supplier<Long> elementCount;

        @NotNull
        final AtomicLong currentWeight = new AtomicLong();

        @NotNull
        final AtomicLong loadSuccessCount = new AtomicLong();

        @NotNull
        final AtomicInteger loadExceptionCount = new AtomicInteger();

        @NotNull
        final AtomicLong loadTime = new AtomicLong();

        @NotNull
        final AtomicLong evictionCount = new AtomicLong();

        @NotNull
        final AtomicLong hitCount = new AtomicLong();

        @NotNull
        final AtomicLong missCount = new AtomicLong();

        protected Stats(@NotNull String name, long maximumWeight, @NotNull Supplier<Long> elementCount) {
            super(name);
            this.maximumWeight = maximumWeight;
            this.elementCount = requireNonNull(elementCount);
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
