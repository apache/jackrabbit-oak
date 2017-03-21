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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Integer.bitCount;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Long.numberOfLeadingZeros;
import static java.lang.Math.max;
import static java.util.Arrays.fill;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.CacheWeights;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheStats;
import com.google.common.cache.Weigher;

/**
 * {@code PriorityCache} implements a partial mapping from keys of type {@code K} to values
 * of type  {@code V}. Mappings are associates with a cost, which states how expensive it is
 * to recreate that mapping. This cache uses the cost such that mappings with a higher cost
 * have a lower chance of being evicted than mappings with a lower cost. When an item from
 * this cache is successfully looked up its cost is incremented by one, unless it has reached
 * its maximum cost of {@link Byte#MAX_VALUE} already.
 * <p>
 * Additionally this cache tracks a generation for mappings. Mappings of later generations
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
    private final int[] costs = new int[256];
    private final int[] evictions = new int[256];

    private long hitCount;
    private long missCount;
    private long loadCount;
    private long loadExceptionCount;
    private long evictionCount;
    private long size;

    @Nonnull
    private final Weigher<K, V> weigher;
    private long weight = 0;

    /**
     * Static factory for creating new {@code PriorityCache} instances.
     * @param size  size of the cache. Must be a power of 2.
     * @return  a new {@code PriorityCache} instance of the given {@code size}.
     */
    public static <K, V> Supplier<PriorityCache<K, V>> factory(final int size, @Nonnull final Weigher<K, V> weigher) {
        checkArgument(bitCount(size) == 1);
        checkNotNull(weigher);
        return new Supplier<PriorityCache<K, V>>() {
            @Override
            public PriorityCache<K, V> get() {
                return new PriorityCache<>(size, weigher);
            }
        };
    }

    /**
     * Static factory for creating new {@code PriorityCache} instances.
     * @param size  size of the cache. Must be a power of 2.
     * @return  a new {@code PriorityCache} instance of the given {@code size}.
     */
    public static <K, V> Supplier<PriorityCache<K, V>> factory(final int size) {
        checkArgument(bitCount(size) == 1);
        return new Supplier<PriorityCache<K, V>>() {
            @Override
            public PriorityCache<K, V> get() {
                return new PriorityCache<>(size);
            }
        };
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
        return 1L << (64L - numberOfLeadingZeros((long)max(1, size) - 1L));
    }

    /**
     * Create a new instance of the given {@code size}. {@code rehash} specifies the number
     * of rehashes to resolve a clash.
     * @param size      Size of the cache. Must be a power of {@code 2}.
     * @param rehash    Number of rehashes. Must be greater or equal to {@code 0} and
     *                  smaller than {@code 32 - numberOfTrailingZeros(size)}.
     */
    PriorityCache(int size, int rehash) {
        this(size, rehash, CacheWeights.<K, V> noopWeigher());
    }

    /**
     * Create a new instance of the given {@code size}. {@code rehash} specifies the number
     * of rehashes to resolve a clash.
     * @param size      Size of the cache. Must be a power of {@code 2}.
     * @param rehash    Number of rehashes. Must be greater or equal to {@code 0} and
     *                  smaller than {@code 32 - numberOfTrailingZeros(size)}.
     * @param weigher   Needed to provide an estimation of the cache weight in memory
     */
    public PriorityCache(int size, int rehash, @Nonnull Weigher<K, V> weigher) {
        checkArgument(bitCount(size) == 1);
        checkArgument(rehash >= 0);
        checkArgument(rehash < 32 - numberOfTrailingZeros(size));
        this.rehash = rehash;
        entries = new Entry<?,?>[size];
        fill(entries, Entry.NULL);
        this.weigher = checkNotNull(weigher);
    }

    /**
     * Create a new instance of the given {@code size}. The number of rehashes is
     * the maximum number allowed by the given {@code size}. ({@code 31 - numberOfTrailingZeros(size)}.
     * @param size      Size of the cache. Must be a power of {@code 2}.
     */
    public PriorityCache(int size, @Nonnull Weigher<K, V> weigher) {
        this(size, 31 - numberOfTrailingZeros(size), weigher);
    }

    public PriorityCache(int size) {
        this(size, 31 - numberOfTrailingZeros(size));
    }

    private int project(int hashCode, int iteration) {
        return (hashCode >> iteration) & (entries.length - 1);
    }

    /**
     * @return  the number of mappings in this cache.
     */
    public long size() {
        return size;
    }

    /**
     * Add a mapping to the cache.
     * @param key            the key of the mapping
     * @param value          the value of the mapping
     * @param generation     the generation of the mapping
     * @param initialCost    the initial cost associated with this mapping
     * @return  {@code true} if the mapping has been added, {@code false} otherwise.
     */
    public synchronized boolean put(@Nonnull K key, @Nonnull V value, int generation, byte initialCost) {
        int hashCode = key.hashCode();
        byte cheapest = initialCost;
        int index = -1;
        boolean eviction = false;
        for (int k = 0; k <= rehash; k++) {
            int i = project(hashCode, k);
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
            Entry<?, ?> old = entries[index];
            Entry<?, ?> newE = new Entry<>(key, value, generation, initialCost);
            entries[index] = newE;
            loadCount++;
            costs[initialCost - Byte.MIN_VALUE]++;
            if (old != Entry.NULL) {
                costs[old.cost - Byte.MIN_VALUE]--;
                if (eviction) {
                    evictions[old.cost - Byte.MIN_VALUE]++;
                    evictionCount++;
                }
                weight -= weighEntry(old);
            } else {
                size++;
            }
            weight += weighEntry(newE);
            return true;
        } else {
            loadExceptionCount++;
            return false;
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
    @CheckForNull
    public synchronized V get(@Nonnull K key, int generation) {
        int hashCode = key.hashCode();
        for (int k = 0; k <= rehash; k++) {
            int i = project(hashCode, k);
            Entry<?, ?> entry = entries[i];
            if (generation == entry.generation && key.equals(entry.key)) {
                if (entry.cost < Byte.MAX_VALUE) {
                    costs[entry.cost - Byte.MIN_VALUE]--;
                    entry.cost++;
                    costs[entry.cost - Byte.MIN_VALUE]++;
                }
                hitCount++;
                return (V) entry.value;
            }
        }
        missCount++;
        return null;
    }

    /**
     * Purge all keys from this cache whose entry's generation matches the
     * passed {@code purge} predicate.
     * @param purge
     */
    public synchronized void purgeGenerations(@Nonnull Predicate<Integer> purge) {
        for (int i = 0; i < entries.length; i++) {
            Entry<?, ?> entry = entries[i];
            if (entry != Entry.NULL && purge.apply(entry.generation)) {
                entries[i] = Entry.NULL;
                size--;
                weight -= weighEntry(entry);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private int weighEntry(Entry<?, ?> entry) {
        return weigher.weigh((K) entry.key, (V) entry.value);
    }

    @Override
    public synchronized String toString() {
        return "PriorityCache" +
            "{ costs=" + toString(costs) +
            ", evictions=" + toString(evictions) + " }";
    }

    private static String toString(int[] ints) {
        StringBuilder b = new StringBuilder("[");
        String sep = "";
        for (int i = 0; i < ints.length; i++) {
            if (ints[i] > 0) {
                b.append(sep).append(i).append("->").append(ints[i]);
                sep = ",";
            }
        }
        return b.append(']').toString();
    }

    /**
     * @return  access statistics for this cache
     */
    @Nonnull
    public CacheStats getStats() {
        return new CacheStats(hitCount, missCount, loadCount, loadExceptionCount, 0, evictionCount);
    }

    public long estimateCurrentWeight() {
        return weight;
    }

}
