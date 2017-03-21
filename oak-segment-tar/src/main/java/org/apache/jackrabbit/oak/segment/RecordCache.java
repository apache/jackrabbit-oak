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

import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import com.google.common.cache.CacheStats;
import com.google.common.cache.Weigher;

/**
 * Partial mapping of keys of type {@code K} to values of type {@link RecordId}. This is
 * typically used for de-duplicating values that have already been persisted and thus
 * already have a {@code RecordId}.
 * @param <K>
 */
public abstract class RecordCache<K> implements Cache<K, RecordId> {
    private long hitCount;
    private long missCount;
    private long loadCount;
    private long evictionCount;

    /**
     * @return number of mappings
     */
    public abstract long size();

    public abstract long estimateCurrentWeight();

    @Override
    public void put(@Nonnull K key, @Nonnull RecordId value, byte cost) {
        throw new UnsupportedOperationException();
    }

    /**
     * @return  access statistics for this cache
     */
    @Nonnull
    public CacheStats getStats() {
        return new CacheStats(hitCount, missCount, loadCount, 0, 0, evictionCount);
    }

    /**
     * Factory method for creating {@code RecordCache} instances. The returned
     * instances are all thread safe. They implement a simple LRU behaviour where
     * the least recently accessed mapping would be replaced when inserting a
     * new mapping would exceed {@code size}.
     *
     * @return  A new {@code RecordCache} instance of the given {@code size}.
     */
    @Nonnull
    public static <T> RecordCache<T> newRecordCache(int size) {
        if (size <= 0) {
            return new Empty<>();
        } else {
            return new Default<>(size, CacheWeights.<T, RecordId> noopWeigher());
        }
    }

    /**
     * @param size size of the cache
     * @param weigher   Needed to provide an estimation of the cache weight in memory
     * @return  A factory returning {@code RecordCache} instances of the given {@code size}
     *          when invoked.
     * @see #newRecordCache(int)
     */
    @Nonnull
    public static <T> Supplier<RecordCache<T>> factory(int size, @Nonnull Weigher<T, RecordId> weigher) {
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
    @Nonnull
    public static <T> Supplier<RecordCache<T>> factory(int size) {
        if (size <= 0) {
            return Empty.emptyFactory();
        } else {
            return Default.defaultFactory(size, CacheWeights.<T, RecordId> noopWeigher());
        }
    }

    private static class Empty<T> extends RecordCache<T> {
        static final <T> Supplier<RecordCache<T>> emptyFactory() {
            return  new Supplier<RecordCache<T>>() {
                @Override
                public RecordCache<T> get() {
                    return new Empty<>();
                }
            };
        }

        @Override
        public synchronized void put(@Nonnull T key, @Nonnull RecordId value) { }

        @Override
        public synchronized RecordId get(@Nonnull T key) {
            super.missCount++;
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

        @Nonnull
        private final Map<K, RecordId> records;

        @Nonnull
        private final Weigher<K, RecordId> weigher;

        private long weight = 0;

        static final <K> Supplier<RecordCache<K>> defaultFactory(final int size, @Nonnull final Weigher<K, RecordId> weigher) {
            return new Supplier<RecordCache<K>>() {
                @Override
                public RecordCache<K> get() {
                    return new Default<>(size, checkNotNull(weigher));
                }
            };
        }

        Default(final int size, @Nonnull final Weigher<K, RecordId> weigher) {
            this.weigher = checkNotNull(weigher);
            records = new LinkedHashMap<K, RecordId>(size * 4 / 3, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<K, RecordId> eldest) {
                    boolean remove = super.size() > size;
                    if (remove) {
                        Default.super.evictionCount++;
                        weight -= weigher.weigh(eldest.getKey(),
                                eldest.getValue());
                    }
                    return remove;
                }
            };
        }

        @Override
        public synchronized void put(@Nonnull K key, @Nonnull RecordId value) {
            super.loadCount++;
            records.put(key, value);
            weight += weigher.weigh(key, value);
        }

        @Override
        public synchronized RecordId get(@Nonnull K key) {
            RecordId value = records.get(key);
            if (value == null) {
                super.missCount++;
            } else {
                super.hitCount++;
            }
            return value;
        }

        @Override
        public synchronized long size() {
            return records.size();
        }

        @Override
        public long estimateCurrentWeight() {
            return weight;
        }
    }
}
