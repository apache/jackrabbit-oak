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

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkNotNull;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.jackrabbit.guava.common.base.Supplier;
import org.apache.jackrabbit.guava.common.cache.CacheBuilder;
import org.apache.jackrabbit.guava.common.cache.CacheStats;
import org.apache.jackrabbit.guava.common.cache.RemovalListener;
import org.apache.jackrabbit.guava.common.cache.Weigher;
import org.jetbrains.annotations.NotNull;

/**
 * Partial mapping of keys of type {@code K} to values of type {@link RecordId}. This is
 * typically used for de-duplicating values that have already been persisted and thus
 * already have a {@code RecordId}.
 * @param <K>
 */
public abstract class RecordCache<K> implements Cache<K, RecordId> {
    /**
     * @return number of mappings
     */
    public abstract long size();

    public abstract long estimateCurrentWeight();

    @Override
    public void put(@NotNull K key, @NotNull RecordId value, byte cost) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return  access statistics for this cache
     */
    @NotNull
    public abstract CacheStats getStats();

    /**
     * Factory method for creating {@code RecordCache} instances. The returned
     * instances are all thread safe. They implement a simple LRU behaviour where
     * the least recently accessed mapping would be replaced when inserting a
     * new mapping would exceed {@code size}.
     *
     * @return  A new {@code RecordCache} instance of the given {@code size}.
     */
    @NotNull
    public static <T> RecordCache<T> newRecordCache(int size) {
        if (size <= 0) {
            return new Empty<>();
        } else {
            return new Default<>(size, CacheWeights.noopWeigher());
        }
    }

    /**
     * @param size size of the cache
     * @param weigher   Needed to provide an estimation of the cache weight in memory
     * @return  A factory returning {@code RecordCache} instances of the given {@code size}
     *          when invoked.
     * @see #newRecordCache(int)
     */
    @NotNull
    public static <T> Supplier<RecordCache<T>> factory(int size, @NotNull Weigher<T, RecordId> weigher) {
        if (size <= 0) {
            return Empty.emptyFactory();
        } else {
            return Default.defaultFactory(size, checkNotNull(weigher));
        }
    }

    /**
     * @param size size of the cache
     * @return  A factory returning {@code RecordCache} instances of the given {@code size}
     *          when invoked.
     * @see #newRecordCache(int)
     */
    @NotNull
    public static <T> Supplier<RecordCache<T>> factory(int size) {
        if (size <= 0) {
            return Empty.emptyFactory();
        } else {
            return Default.defaultFactory(size, CacheWeights.noopWeigher());
        }
    }

    private static class Empty<T> extends RecordCache<T> {
        @NotNull
        private final LongAdder missCount = new LongAdder();

        static final <T> Supplier<RecordCache<T>> emptyFactory() {
            return Empty::new;
        }

        @Override
        public @NotNull CacheStats getStats() {
            return new CacheStats(0, missCount.sum(), 0, 0, 0, 0);
        }

        @Override
        public void put(@NotNull T key, @NotNull RecordId value) { }

        @Override
        public RecordId get(@NotNull T key) {
            missCount.increment();
            return null;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public long estimateCurrentWeight() {
            return -1;
        }
    }

    private static class Default<K> extends RecordCache<K> {
        @NotNull
        private final org.apache.jackrabbit.guava.common.cache.Cache<K, RecordId> cache;

        @NotNull
        private final Weigher<K, RecordId> weigher;
        @NotNull
        private final LongAdder weight = new LongAdder();
        @NotNull
        private final LongAdder loadCount = new LongAdder();

        @Override
        public @NotNull CacheStats getStats() {
            CacheStats internalStats = cache.stats();
            // any addition to the cache counts as load by our definition
            return new CacheStats(internalStats.hitCount(), internalStats.missCount(),
                    loadCount.sum(), 0, 0,  internalStats.evictionCount());
        }

        static <K> Supplier<RecordCache<K>> defaultFactory(final int size, @NotNull final Weigher<K, RecordId> weigher) {
            return () -> new Default<>(size, checkNotNull(weigher));
        }

        Default(final int size, @NotNull final Weigher<K, RecordId> weigher) {
            this.cache = CacheBuilder.newBuilder()
                    .maximumSize(size * 4L / 3)
                    .initialCapacity(size)
                    .concurrencyLevel(4)
                    .recordStats()
                    .removalListener((RemovalListener<K, RecordId>) removal -> {
                        int removedWeight = weigher.weigh(removal.getKey(), removal.getValue());
                        weight.add(-removedWeight);
                    })
                    .build();
            this.weigher = weigher;
        }

        @Override
        public void put(@NotNull K key, @NotNull RecordId value) {
            cache.put(key, value);
            loadCount.increment();
            weight.add(weigher.weigh(key, value));
        }

        @Override
        public RecordId get(@NotNull K key) {
            return cache.getIfPresent(key);
        }

        @Override
        public long size() {
            return cache.size();
        }

        @Override
        public long estimateCurrentWeight() {
            return weight.sum();
        }
    }
}
