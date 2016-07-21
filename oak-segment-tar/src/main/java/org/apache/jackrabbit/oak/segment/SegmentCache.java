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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;

import com.google.common.cache.RemovalCause;
import com.google.common.cache.Weigher;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheLIRS.EvictionCallback;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// FIXME OAK-4474: Finalise SegmentCache: document, add monitoring, management, tests, logging
/**
 * A cache for {@link Segment} instances by their {@link SegmentId}.
 * <p>
 * Conceptually this cache serves as a 2nd level cache for segments. The 1st level cache is
 * implemented by memoising the segment in its id (see {@link SegmentId#segment}. Every time
 * an segment is evicted from this cache the memoised segment is discarded (see
 * {@link SegmentId#unloaded()}).
 * <p>
 * As a consequence this cache is actually only queried for segments it does not contain,
 * which are then loaded through the loader passed to {@link #getSegment(SegmentId, Callable)}.
 * This behaviour is eventually reflected in the cache statistics (see {@link #getCacheStats()}),
 * which always reports a {@link CacheStats#getMissRate() miss rate} of 1.
 */
public class SegmentCache {
    private static final Logger LOG = LoggerFactory.getLogger(SegmentCache.class);

    public static final int DEFAULT_SEGMENT_CACHE_MB = 256;

    private final Weigher<SegmentId, Segment> weigher = new Weigher<SegmentId, Segment>() {
        @Override
        public int weigh(SegmentId id, Segment segment) {
            return 224 + segment.size();
        }
    };

    /**
     * Cache of recently accessed segments
     */
    @Nonnull
    private final CacheLIRS<SegmentId, Segment> cache;

    /**
     * Create a new segment cache of the given size.
     * @param cacheSizeMB  size of the cache in megabytes.
     */
    public SegmentCache(long cacheSizeMB) {
        this.cache = CacheLIRS.<SegmentId, Segment>newBuilder()
            .module("SegmentCache")
            .maximumWeight(cacheSizeMB * 1024 * 1024)
            .averageWeight(Segment.MAX_SEGMENT_SIZE / 2)
            .weigher(weigher)
            .evictionCallback(new EvictionCallback<SegmentId, Segment>() {
                @Override
                public void evicted(SegmentId id, Segment segment, RemovalCause cause) {
                    if (segment != null) {
                        id.unloaded();
                    }
                } })
            .build();
    }

    /**
     * Retrieve an segment from the cache or load it and cache it if not yet in the cache.
     * @param id        the id of the segment
     * @param loader    the loader to load the segment if not yet in the cache
     * @return          the segment identified by {@code id}
     * @throws ExecutionException  when {@code loader} failed to load an segment
     */
    @Nonnull
    public Segment getSegment(@Nonnull SegmentId id, @Nonnull Callable<Segment> loader)
    throws ExecutionException {
        return cache.get(id, loader);
    }

    /**
     * Put a segment into the cache
     * @param segment  the segment to cache
     */
    public void putSegment(@Nonnull Segment segment) {
        cache.put(segment.getSegmentId(), segment);
        segment.getSegmentId().loaded(segment);
    }

    /**
     * Clear all segment from the cache
     */
    public void clear() {
        cache.invalidateAll();
    }

    /**
     * See the class comment regarding some peculiarities of this cache's statistics
     * @return  statistics for this cache.
     */
    @Nonnull
    public CacheStats getCacheStats() {
        return new CacheStats(cache, "Segment Cache", weigher, cache.getMaxMemory());
    }
}