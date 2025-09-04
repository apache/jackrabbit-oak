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
package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.lang.Integer.bitCount;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Long.numberOfLeadingZeros;
import static java.lang.Math.max;
import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.segment.CacheWeights;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.jackrabbit.guava.common.cache.CacheStats;
import org.apache.jackrabbit.guava.common.cache.Weigher;

/**
 * {@code PriorityCache} implements a partial mapping from keys of type {@code K} to values
 * of type  {@code V}. Mappings are associates with a cost, which states how expensive it is
 * to recreate that mapping. This cache uses the cost such that mappings with a higher cost
 * have a lower chance of being evicted than mappings with a lower cost. When an item from
 * this cache is successfully looked up its cost is incremented by one, unless it has reached
 * its maximum cost of {@link Byte#MAX_VALUE} already.
 * <p>
 * Additionally, this cache tracks a generation for mappings. Mappings of later generations
 * always take precedence over mappings of earlier generations. That is, putting a mapping of
 * a later generation into the cache can cause any mapping of an earlier generation to be evicted
 * regardless of its cost.
 * <p>
 * This cache uses rehashing to resolve clashes. The number of rehashes is configurable. When
 * a clash cannot be resolved by rehashing the given number of times the put operation fails.
 * <p>
 * This cache is thread safe.
 * @param <K>  type of the keys
 * @param <V>  type of the values
 */
public class PriorityCache<K, V> {
    private final int rehash;
    private final Entry<?,?>[] entries;
    private final AtomicInteger[] costs;
    private final AtomicInteger[] evictions;

    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder loadCount = new LongAdder();
    private final LongAdder loadExceptionCount = new LongAdder();
    private final LongAdder evictionCount = new LongAdder();
    private final LongAdder size = new LongAdder();

    private static class Segment extends ReentrantLock {}

    @NotNull
    private final Segment[] segments;

    @NotNull
    private final Weigher<K, V> weigher;
    private final AtomicLong weight = new AtomicLong();

    /**
     * Static factory for creating new {@code PriorityCache} instances.
     * @param size  size of the cache. Must be a power of 2.
     * @return  a new {@code PriorityCache} instance of the given {@code size}.
     */
    public static <K, V> Supplier<PriorityCache<K, V>> factory(final int size, @NotNull final Weigher<K, V> weigher) {
        checkArgument(bitCount(size) == 1);
        requireNonNull(weigher);
        return () -> new PriorityCache<>(size, weigher);
    }

    /**
     * Static factory for creating new {@code PriorityCache} instances.
     * @param size  size of the cache. Must be a power of 2.
     * @return  a new {@code PriorityCache} instance of the given {@code size}.
     */
    public static <K, V> Supplier<PriorityCache<K, V>> factory(final int size) {
        checkArgument(bitCount(size) == 1);
        return () -> new PriorityCache<>(size);
    }

    private static class Entry<K, V> {
        static final Entry<Void, Void> NULL = new Entry<>(null, null, -1, Byte.MIN_VALUE);

        final K key;
        final V value;
        final int generation;
        byte cost;

        public Entry(K key, V value, int generation, byte cost) {
            this.key = key;
            this.value = value;
            this.generation = generation;
            this.cost = cost;
        }

        @Override
        public String toString() {
            return this == NULL
                ? "NULL"
                : "Entry{" + key + "->" + value + " @" + generation + ", $" + cost + "}";
        }
    }

    /**
     * Round {@code size} up to the next power of two or 1 for negative values.
     * @param size
     * @return the next power of two starting from {@code size}.
     */
    public static long nextPowerOfTwo(int size) {
        return 1L << (64L - numberOfLeadingZeros(max(1, size) - 1L));
    }

    /**
     * Create a new instance of the given {@code size}. {@code rehash} specifies the number
     * of rehashes to resolve a clash.
     * @param size      Size of the cache. Must be a power of {@code 2}.
     * @param rehash    Number of rehashes. Must be greater or equal to {@code 0} and
     *                  smaller than {@code 32 - numberOfTrailingZeros(size)}.
     */
    PriorityCache(int size, int rehash) {
        this(size, rehash, CacheWeights.noopWeigher());
    }

    /**
     * Create a new instance of the given {@code size}. {@code rehash} specifies the number
     * of rehashes to resolve a clash.
     * @param size      Size of the cache. Must be a power of {@code 2}.
     * @param rehash    Number of rehashes. Must be greater or equal to {@code 0} and
     *                  smaller than {@code 32 - numberOfTrailingZeros(size)}.
     * @param weigher   Needed to provide an estimation of the cache weight in memory
     */
    public PriorityCache(int size, int rehash, @NotNull Weigher<K, V> weigher) {
        this(size, rehash, weigher, 1024);
    }

    /**
     * Create a new instance of the given {@code size}. {@code rehash} specifies the number
     * of rehashes to resolve a clash.
     * @param size        Size of the cache. Must be a power of {@code 2}.
     * @param rehash      Number of rehashes. Must be greater or equal to {@code 0} and
     *                    smaller than {@code 32 - numberOfTrailingZeros(size)}.
     * @param weigher     Needed to provide an estimation of the cache weight in memory
     * @param numSegments Number of separately locked segments. The implementation assumes an equal
     *                    number of entries in each segment, requiring numSegments to divide size.
     *                    Powers of 2 are a safe choice, see @param size.
     */
    public PriorityCache(int size, int rehash, @NotNull Weigher<K, V> weigher, int numSegments) {
        checkArgument(bitCount(size) == 1);
        checkArgument(rehash >= 0);
        checkArgument(rehash < 32 - numberOfTrailingZeros(size));
        this.rehash = rehash;
        entries = new Entry<?,?>[size];
        fill(entries, Entry.NULL);
        this.weigher = requireNonNull(weigher);

        numSegments = Math.min(numSegments, size);
        checkArgument((size % numSegments) == 0,
                "Cache size is not a multiple of its segment count.");

        segments = new Segment[numSegments];
        for (int s = 0; s < numSegments; s++) {
            segments[s] = new Segment();
        }

        costs = new AtomicInteger[256];
        evictions = new AtomicInteger[256];
        for (int i = 0; i < 256; i++) {
            costs[i] = new AtomicInteger();
            evictions[i] = new AtomicInteger();
        }
    }

    /**
     * Create a new instance of the given {@code size}. The number of rehashes is
     * the maximum number allowed by the given {@code size}. ({@code 31 - numberOfTrailingZeros(size)}.
     * @param size      Size of the cache. Must be a power of {@code 2}.
     */
    public PriorityCache(int size, @NotNull Weigher<K, V> weigher) {
        this(size, 31 - numberOfTrailingZeros(size), weigher);
    }

    public PriorityCache(int size) {
        this(size, 31 - numberOfTrailingZeros(size));
    }

    private int project(int hashCode, int iteration) {
        return (hashCode >> iteration) & (entries.length - 1);
    }

    private Segment getSegment(int index) {
        int entriesPerSegment = entries.length / segments.length;
        return segments[index / entriesPerSegment];
    }

    /**
     * @return  the number of mappings in this cache.
     */
    public long size() {
        return size.sum();
    }

    /**
     * Add a mapping to the cache.
     * @param key            the key of the mapping
     * @param value          the value of the mapping
     * @param generation     the generation of the mapping
     * @param initialCost    the initial cost associated with this mapping
     * @return  {@code true} if the mapping has been added, {@code false} otherwise.
     */
    public boolean put(@NotNull K key, @NotNull V value, int generation, byte initialCost) {
        int hashCode = key.hashCode();
        byte cheapest = initialCost;
        int index = -1;
        boolean eviction = false;

        Segment lockedSegment = null;

        try {
            for (int k = 0; k <= rehash; k++) {
                int i = project(hashCode, k);
                Segment segment = getSegment(i);
                if (segment != lockedSegment) {
                    if (lockedSegment != null) {
                        lockedSegment.unlock();
                    }
                    lockedSegment = segment;
                    lockedSegment.lock();
                }

                Entry<?, ?> entry = entries[i];
                if (entry == Entry.NULL) {
                    // Empty slot -> use this index
                    index = i;
                    eviction = false;
                    break;
                } else if (entry.generation <= generation && key.equals(entry.key)) {
                    // Key exists and generation is greater or equal -> use this index and boost the cost
                    index = i;
                    initialCost = entry.cost;
                    if (initialCost < Byte.MAX_VALUE) {
                        initialCost++;
                    }
                    eviction = false;
                    break;
                } else if (entry.generation < generation) {
                    // Old generation -> use this index
                    index = i;
                    eviction = false;
                    break;
                } else if (entry.cost < cheapest) {
                    // Candidate slot, keep on searching for even cheaper slots
                    cheapest = entry.cost;
                    index = i;
                    eviction = true;
                }
            }

            if (index >= 0) {
                Entry<?, ?> oldEntry = entries[index];
                Entry<?, ?> newEntry = new Entry<>(key, value, generation, initialCost);
                entries[index] = newEntry;
                loadCount.increment();
                costs[initialCost - Byte.MIN_VALUE].incrementAndGet();

                if (oldEntry != Entry.NULL) {
                    costs[oldEntry.cost - Byte.MIN_VALUE].decrementAndGet();
                    if (eviction) {
                        evictions[oldEntry.cost - Byte.MIN_VALUE].incrementAndGet();
                        evictionCount.increment();
                    }
                    weight.addAndGet(-weighEntry(oldEntry));
                } else {
                    size.increment();
                }

                weight.addAndGet(weighEntry(newEntry));
                return true;
            }

            loadExceptionCount.increment();
            return false;
        } finally {
            if (lockedSegment != null) {
                lockedSegment.unlock();
            }
        }
    }

    /**
     * Look up a mapping from this cache by its {@code key} and {@code generation}.
     * @param key         key of the mapping to look up
     * @param generation  generation of the mapping to look up
     * @return  the mapping for {@code key} and {@code generation} or {@code null} if this
     *          cache does not contain such a mapping.
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public V get(@NotNull K key, int generation) {
        int hashCode = key.hashCode();
        for (int k = 0; k <= rehash; k++) {
            int i = project(hashCode, k);
            Segment segment = getSegment(i);
            segment.lock();

            try {
                Entry<?, ?> entry = entries[i];
                if (generation == entry.generation && key.equals(entry.key)) {
                    if (entry.cost < Byte.MAX_VALUE) {
                        costs[entry.cost - Byte.MIN_VALUE].decrementAndGet();
                        entry.cost++;
                        costs[entry.cost - Byte.MIN_VALUE].incrementAndGet();
                    }
                    hitCount.increment();
                    return (V) entry.value;
                }
            } finally {
                segment.unlock();
            }
        }
        missCount.increment();
        return null;
    }

    /**
     * Purge all keys from this cache whose entry's generation matches the
     * passed {@code purge} predicate.
     * @param purge
     */
    public void purgeGenerations(@NotNull Predicate<Integer> purge) {
        int numSegments = segments.length;
        int entriesPerSegment = entries.length / numSegments;
        for (int s = 0; s < numSegments; s++) {
            segments[s].lock();
            try {
                for (int i = 0; i < entriesPerSegment; i++) {
                    int j = i + s * entriesPerSegment;
                    Entry<?, ?> entry = entries[j];
                    if (entry != Entry.NULL && purge.test(entry.generation)) {
                        entries[j] = Entry.NULL;
                        size.decrement();
                        weight.addAndGet(-weighEntry(entry));
                    }
                }
            } finally {
                segments[s].unlock();
            }
        }
    }

    private int weighEntry(Entry<?, ?> entry) {
        return weigher.weigh((K) entry.key, (V) entry.value);
    }

    @Override
    public String toString() {
        return "PriorityCache" +
                "{ costs=" + toString(costs) +
                ", evictions=" + toString(evictions) + " }";
    }

    private static String toString(AtomicInteger[] ints) {
        StringBuilder b = new StringBuilder("[");
        String sep = "";
        for (int i = 0; i < ints.length; i++) {
            int value = ints[i].get();
            if (value > 0) {
                b.append(sep).append(i).append("->").append(value);
                sep = ",";
            }
        }
        return b.append(']').toString();
    }

    /**
     * @return  access statistics for this cache
     */
    @NotNull
    public CacheStats getStats() {
        return new CacheStats(hitCount.sum(), missCount.sum(), loadCount.sum(),
                loadExceptionCount.sum(), 0, evictionCount.sum());
    }

    public long estimateCurrentWeight() {
        return weight.get();
    }

}
