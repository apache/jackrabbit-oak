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

import static org.apache.jackrabbit.oak.segment.CacheWeights.OBJECT_HEADER_SIZE;

import java.util.Arrays;
import java.util.function.IntFunction;

import org.apache.jackrabbit.guava.common.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.api.Weigher;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.CacheStatsAdapter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


/**
 * A cache consisting of a fast and slow component. The fast cache for small items is based
 * on an array, and the slow one is a weight-bounded cache from the Oak Cache API.
 */
public abstract class ReaderCache<T> {
    /**
     * The fast (array-based) cache.
     */
    @NotNull
    private final FastCache<T> fastCache;

    /**
     * The slower, weight-bounded cache, from the Oak Cache API.
     * {@code null} when the configured weight is non-positive, i.e. the slow cache is disabled.
     */
    @Nullable
    private final Cache<CacheKey, T> cache;

    @NotNull
    private final AbstractCacheStats cacheStats;

    /**
     * Create a new string cache.
     *
     * @param maxWeight the maximum memory in bytes.
     * @param weigher   Needed to provide an estimation of the cache weight in memory
     */
    protected ReaderCache(long maxWeight, @NotNull String name, @NotNull Weigher<CacheKey, T> weigher) {
        fastCache = new FastCache<>();
        if (maxWeight > 0) {
            cache = CacheBuilder.<CacheKey, T>newBuilder()
                    .maximumWeight(maxWeight)
                    .weigher(weigher)
                    .recordStats()
                    .build();
            cacheStats = new CacheStatsAdapter(cache, name, weigher, maxWeight);
        } else {
            cache = null;
            cacheStats = new EmptyCacheStats(name);
        }
    }

    @NotNull
    public AbstractCacheStats getStats() {
        return cacheStats;
    }

    /**
     * Zeroed stats used when the slow cache is disabled ({@code maxWeight <= 0}).
     */
    private static final class EmptyCacheStats extends AbstractCacheStats {
        private final CacheStats stats;

        EmptyCacheStats(@NotNull String name) {
            super(name);
            this.stats = new CacheStats(0, 0, 0, 0, 0, 0);
        }

        @Override
        protected CacheStats getCurrentStats() {
            return stats;
        }

        @Override
        public long getElementCount() {
            return 0;
        }

        @Override
        public long estimateCurrentWeight() {
            return 0;
        }

        @Override
        public long getMaxTotalWeight() {
            return 0;
        }
    }

    private static int getEntryHash(long msb, long lsb, int offset) {
        int hash = (int) (msb ^ lsb) + offset;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        return (hash >>> 16) ^ hash;
    }

    /**
     * Get the value, loading it if necessary.
     *
     * @param msb the msb of the segment
     * @param lsb the lsb of the segment
     * @param offset the offset
     * @param loader the loader function
     * @return the value
     */
    @NotNull
    public T get(long msb, long lsb, int offset, IntFunction<T> loader) {
        int hash = getEntryHash(msb, lsb, offset);
        T value = fastCache.get(hash, msb, lsb, offset);
        if (value != null) {
            return value;
        }

        if (cache == null) {
            value = loader.apply(offset);
            /*
             * Admission to the fast cache depends on a slow cache hit by default.
             * If the slow cache is disabled (i.e. there will never be a hit),
             * we populate it on first access to avoid a perpetually empty fast cache.
             */
            if (isSmall(value)) {
                fastCache.put(hash, new FastCacheEntry<>(hash, msb, lsb, offset, value));
            }
            return value;
        }

        CacheKey key = new CacheKey(hash, msb, lsb, offset);
        value = cache.getIfPresent(key);
        if (value != null) {
            // slow-cache hit: promote to fast tier
            if (isSmall(value)) {
                fastCache.put(hash, new FastCacheEntry<>(hash, msb, lsb, offset, value));
            }
            return value;
        }

        value = loader.apply(offset);
        cache.put(key, value);
        return value;
    }

    /**
     * Clear the cache.
     */
    public void clear() {
        if (cache != null) {
            cache.invalidateAll();
        }
        fastCache.clear();
    }

    /**
     * Determine whether the entry is small, in which case it can be kept in the fast cache.
     */
    protected abstract boolean isSmall(T value);

    /**
     * A fast cache based on an array.
     */
    private static class FastCache<T> {

        /**
         * The number of entries in the cache. Must be a power of 2.
         */
        private static final int CACHE_SIZE = 16 * 1024;

        /**
         * The cache array.
         */
        @SuppressWarnings("unchecked")
        private final FastCacheEntry<T>[] elements = new FastCacheEntry[CACHE_SIZE];

        /**
         * Get the string if it is stored.
         *
         * @param hash the hash
         * @param msb the msb of the segment
         * @param lsb the lsb of the segment
         * @param offset the offset
         * @return the string, or null
         */
        T get(int hash, long msb, long lsb, int offset) {
            int index = hash & (CACHE_SIZE - 1);
            FastCacheEntry<T> e = elements[index];
            if (e != null && e.matches(msb, lsb, offset)) {
                return e.value;
            }
            return null;
        }

        void clear() {
            Arrays.fill(elements, null);
        }

        void put(int hash, FastCacheEntry<T> entry) {
            int index = hash & (CACHE_SIZE - 1);
            elements[index] = entry;
        }

    }

    protected static class CacheKey {
        private final int hash;
        private final long msb;
        private final long lsb;
        private final int offset;

        CacheKey(int hash, long msb, long lsb, int offset) {
            this.hash = hash;
            this.msb = msb;
            this.lsb = lsb;
            this.offset = offset;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof ReaderCache.CacheKey otherKey)) {
                return false;
            }
            return (otherKey.hash == hash) && (otherKey.msb == msb) &&
                (otherKey.lsb == lsb) && (otherKey.offset == offset);
        }

        @Override
        public String toString() {
            return Long.toHexString(msb) +
                ':' + Long.toHexString(lsb) +
                '+' + Integer.toHexString(offset);
        }

        public int estimateMemoryUsage() {
            return OBJECT_HEADER_SIZE + 32;
        }
    }

    private record FastCacheEntry<T>(int hash, long msb, long lsb, int offset, T value) {
        boolean matches(long msb, long lsb, int offset) {
            return (this.offset == offset) && (this.msb == msb) && (this.lsb == lsb);
        }
    }

}
