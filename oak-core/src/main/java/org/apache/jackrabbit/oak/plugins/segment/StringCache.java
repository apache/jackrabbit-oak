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

import com.google.common.base.Function;
import org.apache.jackrabbit.oak.cache.CacheLIRS;

/**
 * A string cache. It has two components: a fast cache for small strings, based
 * on an array, and a slow cache that uses a LIRS cache.
 */
public class StringCache {

    /**
     * The slow cache.
     */
    private CacheLIRS<StringCacheEntry, String> cache;

    /**
     * Create a new string cache.
     * 
     * @param maxSize the maximum memory in bytes.
     */
    StringCache(int maxSize) {
        cache = new CacheLIRS.Builder().maximumSize(maxSize).averageWeight(100)
                .build();
    }

    /**
     * Get the string, loading it if necessary.
     * 
     * @param segment the segment
     * @param offset the offset
     * @return the string (never null)
     */
    String getString(Segment segment, int offset, Function<Integer, String> loader) {
        int hash = getEntryHash(segment, offset);
        String s = FastCache.getString(hash, segment, offset);
        if (s != null) {
            return s;
        }
        StringCacheEntry key = new StringCacheEntry(hash, segment, offset, null);
        s = cache.getIfPresent(key);
        if (s == null) {
            s = loader.apply(offset);
            cache.put(key, s, getMemory(s));
        }
        if (FastCache.isSmall(s)) {
            key.setString(s);
            FastCache.addString(hash, key);
        }
        return s;
    }

    public void clear() {
        cache.cleanUp();
    }

    private static int getMemory(String s) {
        return 100 + s.length() * 2;
    }

    private static int getEntryHash(Segment segment, int offset) {
        int hash = segment.getSegmentId().hashCode() + offset;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        return hash = (hash >>> 16) ^ hash;
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
        private static final StringCacheEntry[] CACHE = new StringCacheEntry[CACHE_SIZE];

        /**
         * Get the string if it is stored.
         * 
         * @param hash the hash
         * @param segment the segment
         * @param offset the offset
         * @return the string, or null
         */
        static String getString(int hash, Segment segment, int offset) {
            int index = hash & (CACHE_SIZE - 1);
            StringCacheEntry e = CACHE[index];
            if (e != null && e.matches(segment, offset)) {
                return e.string;
            }
            return null;
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

        static void addString(int hash, StringCacheEntry entry) {
            int index = hash & (CACHE_SIZE - 1);
            CACHE[index] = entry;
        }

    }

    static class StringCacheEntry {
        private final int hash;
        private final long msb, lsb;
        private final int offset;
        private String string;

        StringCacheEntry(int hash, Segment segment, int offset, String string) {
            this.hash = hash;
            SegmentId id = segment.getSegmentId();
            this.msb = id.getMostSignificantBits();
            this.lsb = id.getLeastSignificantBits();
            this.offset = offset;
            this.string = string;
        }

        void setString(String string) {
            if (string == null) {
                throw new NullPointerException();
            }
            this.string = string;
        }

        boolean matches(Segment segment, int offset) {
            if (this.offset != offset) {
                return false;
            }
            SegmentId id = segment.getSegmentId();
            return id.getMostSignificantBits() == msb &&
                    id.getLeastSignificantBits() == lsb;
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

    }

}
