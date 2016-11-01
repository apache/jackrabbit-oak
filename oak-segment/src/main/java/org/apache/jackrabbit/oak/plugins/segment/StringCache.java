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

package org.apache.jackrabbit.oak.plugins.segment;

import java.util.Arrays;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheStats;
import static org.apache.jackrabbit.oak.commons.StringUtils.estimateMemoryUsage;

/**
 * A string cache. It has two components: a fast cache for small strings, based
 * on an array, and a slow cache that uses a LIRS cache.
 */
@Deprecated
public class StringCache {

    /**
     * The fast (array based) cache.
     */
    private final FastCache fastCache;

    /**
     * The slower (LIRS) cache.
     */
    private final CacheLIRS<StringCacheKey, String> cache;

    /**
     * Create a new string cache.
     *
     * @param maxSize the maximum memory in bytes.
     */
    StringCache(long maxSize) {
        if (maxSize >= 0) {
            fastCache = new FastCache();
            cache = CacheLIRS.<StringCacheKey, String>newBuilder()
                    .module("StringCache")
                    .maximumWeight(maxSize)
                    .averageWeight(250)
                    .build();
        } else {
            fastCache = null;
            // dummy cache to prevent NPE on the getStats() call
            cache = CacheLIRS.<StringCacheKey, String> newBuilder()
                    .module("StringCache")
                    .maximumSize(1)
                    .build();
        }
    }

    @Nonnull
    @Deprecated
    public CacheStats getStats() {
        return new CacheStats(cache, "String Cache", null, -1);
    }

    /**
     * Get the string, loading it if necessary.
     *
     * @param msb the msb of the segment
     * @param lsb the lsb of the segment
     * @param offset the offset
     * @param loader the string loader function
     * @return the string (never null)
     */
    @Deprecated
    public String getString(long msb, long lsb, int offset, Function<Integer, String> loader) {
        int hash = getEntryHash(msb, lsb, offset);
        if (fastCache == null) {
            // disabled cache
            return loader.apply(offset);
        }

        String s = fastCache.getString(hash, msb, lsb, offset);
        if (s != null) {
            return s;
        }
        StringCacheKey key = new StringCacheKey(hash, msb, lsb, offset);
        s = cache.getIfPresent(key);
        if (s == null) {
            s = loader.apply(offset);
            cache.put(key, s, getMemory(s));
        }
        if (FastCache.isSmall(s)) {
            fastCache.addString(hash, new FastCacheEntry(hash, msb, lsb, offset, s));
        }
        return s;
    }

    /**
     * Clear the cache.
     */
    @Deprecated
    public void clear() {
        if (fastCache != null) {
            cache.invalidateAll();
            fastCache.clear();
        }
    }

    /**
     * Estimation includes the key's overhead, see {@link EmpiricalWeigher} for
     * an example
     */
    private static int getMemory(String s) {
        int size = 168; // overhead for each cache entry
        size += 40; // key
        size += estimateMemoryUsage(s); // value
        return size;
    }

    private static int getEntryHash(long lsb, long msb, int offset) {
        int hash = (int) (msb ^ lsb) + offset;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        return (hash >>> 16) ^ hash;
    }

    /**
     * A fast cache based on an array.
     */
    static class FastCache {

        /**
         * The maximum number of characters in string that are cached.
         */
        static final int MAX_STRING_SIZE = 128;

        /**
         * The number of entries in the cache. Must be a power of 2.
         */
        private static final int CACHE_SIZE = 16 * 1024;

        /**
         * The cache array.
         */
        private final FastCacheEntry[] cache = new FastCacheEntry[CACHE_SIZE];

        /**
         * Get the string if it is stored.
         *
         * @param hash the hash
         * @param msb
         * @param lsb
         * @param offset the offset
         * @return the string, or null
         */
        String getString(int hash, long msb, long lsb, int offset) {
            int index = hash & (CACHE_SIZE - 1);
            FastCacheEntry e = cache[index];
            if (e != null && e.matches(msb, lsb, offset)) {
                return e.string;
            }
            return null;
        }

        void clear() {
            Arrays.fill(cache, null);
        }

        /**
         * Whether the entry is small, in which case it can be kept in the fast cache.
         * 
         * @param s the string
         * @return whether the entry is small
         */
        static boolean isSmall(String s) {
            return s.length() <= MAX_STRING_SIZE;
        }

        void addString(int hash, FastCacheEntry entry) {
            int index = hash & (CACHE_SIZE - 1);
            cache[index] = entry;
        }

    }

    private static class StringCacheKey {
        private final int hash;
        private final long msb, lsb;
        private final int offset;

        StringCacheKey(int hash, long msb, long lsb, int offset) {
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
            if (!(other instanceof StringCacheKey)) {
                return false;
            }
            StringCacheKey o = (StringCacheKey) other;
            return o.hash == hash && o.msb == msb && o.lsb == lsb &&
                    o.offset == offset;
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder();
            buff.append(Long.toHexString(msb)).
                append(':').append(Long.toHexString(lsb)).
                append('+').append(Integer.toHexString(offset));
            return buff.toString();
        }

    }

    private static class FastCacheEntry {

        private final int hash;
        private final long msb, lsb;
        private final int offset;
        private final String string;

        FastCacheEntry(int hash, long msb, long lsb, int offset, String string) {
            this.hash = hash;
            this.msb = msb;
            this.lsb = lsb;
            this.offset = offset;
            this.string = string;
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
            FastCacheEntry o = (FastCacheEntry) other;
            return o.hash == hash && o.msb == msb && o.lsb == lsb &&
                    o.offset == offset;
        }

    }

}