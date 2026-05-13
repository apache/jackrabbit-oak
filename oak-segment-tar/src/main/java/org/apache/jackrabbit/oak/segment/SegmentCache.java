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
import static org.apache.jackrabbit.oak.segment.CacheWeights.segmentWeight;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.jackrabbit.guava.common.cache.CacheStats;
import org.apache.jackrabbit.guava.common.cache.RemovalNotification;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.CacheStatsSnapshot;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.apache.jackrabbit.oak.segment.CacheWeights.SegmentCacheWeigher;
import org.jetbrains.annotations.NotNull;

/**
 * A cache for {@link SegmentId#isDataSegmentId() data} {@link Segment}
 * instances by their {@link SegmentId}. This cache ignores {@link
 * SegmentId#isBulkSegmentId() bulk} segments.
 * <p>
 * Conceptually this cache serves as a 2nd level cache for segments. The 1st
 * level cache is implemented by memoising the segment in its id (see {@code
 * SegmentId#segment}. Every time an segment is evicted from this cache the
 * memoised segment is discarded (see {@code SegmentId#onAccess}). On an L1 hit,
 * {@link #recordHit(SegmentId)} records L1 hits in {@link #getCacheStats()} and, when enabled,
 * touches L2 so eviction policies see the access.
 */
public abstract class SegmentCache {

    /**
     * Default maximum weight of this cache in MB
     */
    public static final int DEFAULT_SEGMENT_CACHE_MB = 256;

    private static final String NAME = "Segment Cache";

    /**
     * Eviction policy used by {@link NonEmptyCache}.
     *
     * <p>The default is {@link #CAFFEINE}. {@link #LIRS} selects the
     * {@link org.apache.jackrabbit.oak.cache.CacheLIRS} implementation,
     * which was the segment-cache backend before the Caffeine migration
     * (see OAK-XXXXX). Useful for A/B testing or benchmarking.</p>
     */
    public enum SegmentCachePolicy {
        /** Caffeine W-TinyLFU — current default. */
        CAFFEINE,
        /** Oak CacheLIRS — pre-migration baseline. */
        LIRS,
        /** Guava LRU — original SegmentCache backend, before the LIRS migration. */
        GUAVA
    }

    /**
     * Create a new segment cache of the given size using the default
     * {@link SegmentCachePolicy#CAFFEINE} eviction policy.
     * Returns an always-empty cache for {@code cacheSizeMB <= 0}.
     *
     * @param cacheSizeMB size of the cache in megabytes.
     */
    @NotNull
    public static SegmentCache newSegmentCache(long cacheSizeMB) {
        return newSegmentCache(cacheSizeMB, SegmentCachePolicy.CAFFEINE);
    }

    /**
     * Create a new segment cache of the given size with the specified eviction
     * policy. Returns an always-empty cache for {@code cacheSizeMB <= 0}.
     *
     * @param cacheSizeMB size of the cache in megabytes.
     * @param policy      the eviction policy to use (must not be null).
     */
    @NotNull
    public static SegmentCache newSegmentCache(long cacheSizeMB, @NotNull SegmentCachePolicy policy) {
        if (cacheSizeMB > 0) {
            return new NonEmptyCache(cacheSizeMB, policy);
        } else {
            return new EmptyCache();
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
     * Performs any pending cache maintenance operations, including flushing
     * deferred eviction processing.  Call before reading eviction statistics
     * to ensure all pending evictions are counted.
     */
    public abstract void cleanUp();

    /**
     * @return Statistics for this cache.
     */
    @NotNull
    public abstract AbstractCacheStats getCacheStats();

    /**
     * Called on L1 memoised access ({@link SegmentId#getSegment()}): increments {@link #getCacheStats()}
     * hit counts and, for data segments with {@link #FT_OAK_12214_PROPAGATE_L1_HITS_TO_L2_ENABLED} {@code true}, touches L2
     * (e.g. {@code getIfPresent}) so eviction policy matches real reads. Name is historical
     * ({@code hit} = stats); the L2 side is access notification, not only accounting.
     * <p>
     * When the toggle is {@code true} and {@code id} is a data segment, this performs one extra map
     * lookup on the hottest read path whenever the segment is still in L2.
     *
     * @param id the segment id that was served from L1
     */
    public abstract void recordHit(@NotNull SegmentId id);

    /**
     * Feature toggle name for {@link #FT_OAK_12214_PROPAGATE_L1_HITS_TO_L2_ENABLED}: propagate L1 memoisation hits to the
     * segment L2 cache so frequency/recency used for eviction stay aligned with actual access.
     * Disable at runtime via the OSGi Whiteboard when diagnosing behavior.
     */
    public static final String FT_OAK_12214 = "FT_OAK-12214";

    /**
     * Whether L1 memoised hits are propagated to L2 so W-TinyLFU / LRU state matches actual access.
     * Defaults to {@code true} as a <strong>bug-fix</strong> toggle (L2 was blind to L1); flip via
     * the OSGi Whiteboard {@link org.apache.jackrabbit.oak.spi.toggle.FeatureToggle FeatureToggle}
     * registered under {@link #FT_OAK_12214} for diagnosis or A/B runs.
     */
    public static final AtomicBoolean FT_OAK_12214_PROPAGATE_L1_HITS_TO_L2_ENABLED = new AtomicBoolean(true);

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
         * Create a new cache of the given size using the specified eviction policy.
         *
         * @param cacheSizeMB size of the cache in megabytes.
         * @param policy      the eviction policy to use.
         */
        private NonEmptyCache(long cacheSizeMB, SegmentCachePolicy policy) {
            long maximumWeight = cacheSizeMB * 1024 * 1024;
            this.cache = buildCache(maximumWeight, policy);
            this.stats = new Stats(NAME, maximumWeight, cache::estimatedSize);
        }

        private Cache<SegmentId, Segment> buildCache(long maximumWeight, SegmentCachePolicy policy) {
            switch (policy) {
                case LIRS:
                    org.apache.jackrabbit.oak.cache.CacheLIRS.EvictionCallback<SegmentId, Segment> lirsCallback =
                            (key, value, cause) -> this.onRemove(key, value,
                                    org.apache.jackrabbit.oak.cache.CacheLIRS.toOakCause(cause));
                    org.apache.jackrabbit.oak.cache.CacheLIRS<SegmentId, Segment> lirs =
                            org.apache.jackrabbit.oak.cache.CacheLIRS
                                    .<SegmentId, Segment>newBuilder()
                                    .maximumWeight(maximumWeight)
                                    .weigher((key, value) -> segmentWeight(value))
                                    .evictionCallback(lirsCallback)
                                    .build();
                    return lirs.asManualCache();
                case GUAVA:
                    return buildGuavaCache(maximumWeight);
                case CAFFEINE:
                default:
                    return CacheBuilder.<SegmentId, Segment>newBuilder()
                            .maximumWeight(maximumWeight)
                            .weigher(new SegmentCacheWeigher())
                            .evictionListener(this::onRemove)
                            .build();
            }
        }

        @SuppressWarnings("unchecked")
        private Cache<SegmentId, Segment> buildGuavaCache(long maximumWeight) {
            org.apache.jackrabbit.guava.common.cache.Cache<SegmentId, Segment> guava =
                    org.apache.jackrabbit.guava.common.cache.CacheBuilder.newBuilder()
                            .maximumWeight(maximumWeight)
                            .weigher(new CacheWeights.SegmentCacheWeigherGuava())
                            .removalListener((RemovalNotification<SegmentId, Segment> n) ->
                                    this.onRemove(n.getKey(), n.getValue(),
                                            org.apache.jackrabbit.oak.cache.CacheLIRS.toOakCause(n.getCause())))
                            .build();
            return new GuavaCacheAdapter<>(guava);
        }

        /**
         * Removal handler called whenever an item is evicted from the cache.
         */
        private void onRemove(@NotNull SegmentId key, Segment value, @NotNull EvictionCause cause) {
            stats.evictionCount.incrementAndGet();
            if (value != null) {
                stats.currentWeight.addAndGet(-segmentWeight(value));
            }
            key.unloaded();
        }

        @Override
        @NotNull
        public Segment getSegment(@NotNull SegmentId id, @NotNull Callable<Segment> loader) throws ExecutionException {
            if (id.isDataSegmentId()) {
                try {
                    return cache.get(id, k -> {
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
                            // Preserve the former Guava cache exception shape. Letting Caffeine
                            // expose runtime loader failures directly broke FileStore RNE handling.
                            throw new SegmentCacheLoaderException(e);
                        }
                    });
                } catch (RuntimeException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof Exception && !(cause instanceof RuntimeException)) {
                        throw new ExecutionException(cause);
                    }
                    throw e;
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
            stats.hitCount.incrementAndGet();
            if (id.isDataSegmentId() && FT_OAK_12214_PROPAGATE_L1_HITS_TO_L2_ENABLED.get()) {
                cache.getIfPresent(id);
            }
        }
    }

    private static final class SegmentCacheLoaderException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private SegmentCacheLoaderException(@NotNull Exception cause) {
            super(cause);
        }
    }

    /**
     * Adapts a Guava {@link org.apache.jackrabbit.guava.common.cache.Cache} to the
     * Oak {@link Cache} interface so it can be used as the L2 backend in
     * {@link NonEmptyCache}.
     */
    private static final class GuavaCacheAdapter<K, V> implements Cache<K, V> {

        private final org.apache.jackrabbit.guava.common.cache.Cache<K, V> delegate;

        GuavaCacheAdapter(org.apache.jackrabbit.guava.common.cache.Cache<K, V> delegate) {
            this.delegate = delegate;
        }

        @Override
        public V getIfPresent(@NotNull K key) {
            return delegate.getIfPresent(key);
        }

        @Override
        public V get(@NotNull K key, @NotNull Function<? super K, ? extends V> fn) {
            try {
                return delegate.get(key, () -> fn.apply(key));
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof RuntimeException re) { throw re; }
                if (cause instanceof Error er) { throw er; }
                throw new CompletionException(cause == null ? e : cause);
            }
        }

        @Override
        public void put(@NotNull K key, @NotNull V value) {
            delegate.put(key, value);
        }

        @Override
        public void invalidate(@NotNull K key) {
            delegate.invalidate(key);
        }

        @Override
        public void invalidateAll() {
            delegate.invalidateAll();
        }

        @Override
        public void invalidateAll(@NotNull Iterable<? extends K> keys) {
            delegate.invalidateAll(keys);
        }

        @Override
        public long estimatedSize() {
            return delegate.size();
        }

        @Override
        @NotNull
        public CacheStatsSnapshot stats() {
            return new CacheStatsSnapshot(0, 0, 0, 0, 0, 0);
        }

        @Override
        @NotNull
        public ConcurrentMap<K, V> asMap() {
            return delegate.asMap();
        }

        @Override
        @NotNull
        public Map<K, V> getAllPresent(@NotNull Iterable<? extends K> keys) {
            return delegate.getAllPresent(keys);
        }

        @Override
        public void cleanUp() {
            delegate.cleanUp();
        }

        @Override
        public long getUsedWeight() {
            return -1;
        }

        @Override
        public void setMaximumWeight(long maximumWeight) {
            // Guava does not support dynamic resizing
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

        @Override
        public void cleanUp() {}

        @NotNull
        @Override
        public AbstractCacheStats getCacheStats() {
            return stats;
        }

        @Override
        public void recordHit(@NotNull SegmentId id) {
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
