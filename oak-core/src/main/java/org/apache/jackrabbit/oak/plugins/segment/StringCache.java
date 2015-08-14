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

/**
 * A string cache. It has two components: a fast cache for small strings, based
 * on an array, and a slow cache that uses a LIRS cache.
 */
public class StringCache {

    /**
     * The fast (array based) cache.
     */
    private final FastCache fastCache = new FastCache();

    /**
     * The slower (LIRS) cache.
     */
    private final CacheLIRS<StringCacheEntry, String> cache;

    /**
     * Create a new string cache.
     *
     * @param maxSize the maximum memory in bytes.
     */
    StringCache(int maxSize) {
        cache = CacheLIRS.<StringCacheEntry, String>newBuilder()
                .module("StringCache")
                .maximumSize(maxSize)
                .averageWeight(100)
                .build();
    }

    @Nonnull
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
    public String getString(long msb, long lsb, int offset, Function<Integer, String> loader) {
        int hash = getEntryHash(msb, lsb, offset);
        String s = fastCache.getString(hash, msb, lsb, offset);
        if (s != null) {
            return s;
        }
        StringCacheEntry key = new StringCacheEntry(hash, msb, lsb, offset, null);
        s = cache.getIfPresent(key);
        if (s == null) {
            s = loader.apply(offset);
            cache.put(key, s, getMemory(s));
        }
        if (FastCache.isSmall(s)) {
            key.setString(s);
            fastCache.addString(hash, key);
        }
        return s;
    }

    /**
     * Clear the cache.
     */
    public void clear() {
        cache.invalidateAll();
        fastCache.clear();
    }

    private static int getMemory(String s) {
        return 100 + s.length() * 2;
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
        private final StringCacheEntry[] cache = new StringCacheEntry[CACHE_SIZE];

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
            StringCacheEntry e = cache[index];
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

        void addString(int hash, StringCacheEntry entry) {
            int index = hash & (CACHE_SIZE - 1);
            cache[index] = entry;
        }

    }

    static class StringCacheEntry {
        private final int hash;
        private final long msb, lsb;
        private final int offset;
        private String string;

        StringCacheEntry(int hash, long msb, long lsb, int offset, String string) {
            this.hash = hash;
            this.msb = msb;
            this.lsb = lsb;
            this.offset = offset;
            this.string = string;
        }

        void setString(String string) {
            if (string == null) {
                throw new NullPointerException();
            }
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
            if (!(other instanceof StringCacheEntry)) {
                return false;
            }
            StringCacheEntry o = (StringCacheEntry) other;
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

}