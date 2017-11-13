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

import static org.apache.jackrabbit.oak.commons.StringUtils.estimateMemoryUsage;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.ReaderCache.CacheKey;

import com.google.common.cache.Weigher;

public final class CacheWeights {

    public static final int OBJECT_HEADER_SIZE = 12;

    /**
     * memory overhead per each item stored in the cache
     */
    private static final int LIRS_CACHE_OVERHEAD = 168;

    /**
     * memory overhead per each item stored in the cache
     */
    private static final int RECORD_CACHE_OVERHEAD = 32;

    /**
     * memory overhead per each item stored in the cache
     */
    private static final int PRIORITY_CACHE_OVERHEAD = 32;

    /**
     * memory overhead per each item stored in the cache
     */
    private static final int SEGMENT_CACHE_OVERHEAD = 32;

    private CacheWeights() {}

    public static class OneWeigher<K, V> implements Weigher<K, V> {

        @Override
        public int weigh(@Nonnull Object key, @Nonnull Object value) {
            return 1;
        }
    }

    private static final Weigher<Object, Object> NOOP_WEIGHER = new OneWeigher<>();

    @SuppressWarnings("unchecked")
    public static <K, V> Weigher<K, V> noopWeigher() {
        return (Weigher<K, V>) NOOP_WEIGHER;
    }

    static int segmentWeight(Segment segment) {
        return SEGMENT_CACHE_OVERHEAD + segment.estimateMemoryUsage();
    }

    public static class SegmentCacheWeigher implements
            Weigher<SegmentId, Segment> {
        @Override
        public int weigh(@Nonnull SegmentId id, @Nonnull Segment segment) {
            return segmentWeight(segment);
        }
    }

    public static class NodeCacheWeigher implements Weigher<String, RecordId> {

        @Override
        public int weigh(@Nonnull String key, @Nonnull RecordId value) {
            int size = PRIORITY_CACHE_OVERHEAD;
            size += estimateMemoryUsage(key);
            size += value.estimateMemoryUsage();
            return size;
        }
    }

    public static class StringCacheWeigher implements Weigher<String, RecordId> {

        @Override
        public int weigh(@Nonnull String key, @Nonnull RecordId value) {
            int size = RECORD_CACHE_OVERHEAD;
            size += estimateMemoryUsage(key);
            size += value.estimateMemoryUsage();
            return size;
        }
    }

    public static class TemplateCacheWeigher implements
            Weigher<Template, RecordId> {

        @Override
        public int weigh(@Nonnull Template key, @Nonnull RecordId value) {
            int size = RECORD_CACHE_OVERHEAD;
            size += key.estimateMemoryUsage();
            size += value.estimateMemoryUsage();
            return size;
        }
    }

    public static class ReaderTemplateCacheWeigher implements
            Weigher<CacheKey, Template> {

        @Override
        public int weigh(@Nonnull CacheKey key, @Nonnull Template value) {
            int size = LIRS_CACHE_OVERHEAD;
            size += key.estimateMemoryUsage();
            size += value.estimateMemoryUsage();
            return size;
        }
    }

    public static class ReaderStringCacheWeigher implements
            Weigher<CacheKey, String> {

        @Override
        public int weigh(@Nonnull CacheKey key, @Nonnull String value) {
            int size = LIRS_CACHE_OVERHEAD;
            size += key.estimateMemoryUsage();
            size += estimateMemoryUsage(value);
            return size;
        }
    }

}
