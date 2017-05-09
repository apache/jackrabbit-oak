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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.CacheWeights.OBJECT_HEADER_SIZE;

import java.util.Arrays;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheStats;

import com.google.common.base.Function;
import com.google.common.cache.Weigher;

/**
 * A cache consisting of a fast and slow component. The fast cache for small items is based
 * on an array, and a slow one uses a LIRS cache.
 */
public abstract class ReaderCache<T> {

    @Nonnull
    private final Weigher<CacheKey, T> weigher;

    @Nonnull
    private final String name;

    /**
     * The fast (array based) cache.
     */
    @CheckForNull
    private final FastCache<T> fastCache;

    /**
     * The slower (LIRS) cache.
     */
    @Nonnull
    private final CacheLIRS<CacheKey, T> cache;

    /**
     * Create a new string cache.
     *
     * @param maxWeight the maximum memory in bytes.
     * @param averageWeight  an estimate for the average weight of the elements in the
     *                       cache. See {@link CacheLIRS#setAverageMemory(int)}.
     * @param weigher   Needed to provide an estimation of the cache weight in memory
     */
    protected ReaderCache(long maxWeight, int averageWeight,
            @Nonnull String name, @Nonnull Weigher<CacheKey, T> weigher) {
        this.name = checkNotNull(name);
        this.weigher = checkNotNull(weigher);
        fastCache = new FastCache<>();
        cache = CacheLIRS.<CacheKey, T>newBuilder()
                .module(name)
                .maximumWeight(maxWeight)
                .averageWeight(averageWeight)
                .weigher(weigher)
                .build();
    }

    @Nonnull
    public CacheStats getStats() {
        return new CacheStats(cache, name, weigher, cache.getMaxMemory());
    }

    private static int getEntryHash(long lsb, long msb, int offset) {
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
    @Nonnull
    public T get(long msb, long lsb, int offset, Function<Integer, T> loader) {
        int hash = getEntryHash(msb, lsb, offset);
        if (fastCache == null) {
            // disabled cache
            T value = loader.apply(offset);
            assert value != null;
            return value;
        }

        T value = fastCache.get(hash, msb, lsb, offset);
        if (value != null) {
            return value;
        }
        CacheKey key = new CacheKey(hash, msb, lsb, offset);
        value = cache.getIfPresent(key);
        if (value == null) {
            value = loader.apply(offset);
            assert value != null;
            cache.put(key, value);
        }
        if (isSmall(value)) {
            fastCache.put(hash, new FastCacheEntry<>(hash, msb, lsb, offset, value));
        }
        return value;
    }

    /**
     * Clear the cache.
     */
    public void clear() {
        if (fastCache != null) {
            cache.invalidateAll();
            fastCache.clear();
        }
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
         * @param msb
         * @param lsb
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

    static class CacheKey {
        private final int hash;
        private final long msb, lsb;
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
            if (!(other instanceof ReaderCache.CacheKey)) {
                return false;
            }
            CacheKey o = (CacheKey) other;
            return o.hash == hash && o.msb == msb && o.lsb == lsb &&
                    o.offset == offset;
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

    private static class FastCacheEntry<T> {

        private final int hash;
        private final long msb, lsb;
        private final int offset;
        private final T value;

        FastCacheEntry(int hash, long msb, long lsb, int offset, T value) {
            this.hash = hash;
            this.msb = msb;
            this.lsb = lsb;
            this.offset = offset;
            this.value = value;
        }

        boolean matches(long msb, long lsb, int offset) {
            return this.offset == offset && this.msb == msb && this.lsb == lsb;
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
            if (!(other instanceof FastCacheEntry)) {
                return false;
            }
            FastCacheEntry<?> o = (FastCacheEntry<?>) other;
            return o.hash == hash && o.msb == msb && o.lsb == lsb &&
                    o.offset == offset;
        }

    }

}