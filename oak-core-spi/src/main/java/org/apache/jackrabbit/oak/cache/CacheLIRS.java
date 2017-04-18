/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A scan resistant cache. It is meant to cache objects that are relatively
 * costly to acquire, for example file content.
 * <p>
 * This implementation is multi-threading safe and supports concurrent access.
 * Null keys or null values are not allowed. The map fill factor is at most 75%.
 * <p>
 * Each entry is assigned a distinct memory size, and the cache will try to use
 * at most the specified amount of memory. The memory unit is not relevant,
 * however it is suggested to use bytes as the unit.
 * <p>
 * This class implements an approximation of the the LIRS replacement algorithm
 * invented by Xiaodong Zhang and Song Jiang as described in
 * http://www.cse.ohio-state.edu/~zhang/lirs-sigmetrics-02.html with a few
 * smaller changes: An additional queue for non-resident entries is used, to
 * prevent unbound memory usage. The maximum size of this queue is at most the
 * size of the rest of the stack. About 6.25% of the mapped entries are cold.
 * <p>
 * Internally, the cache is split into a number of segments, and each segment is
 * an individual LIRS cache.
 * <p>
 * Accessed entries are only moved to the top of the stack if at least a number
 * of other entries have been moved to the front (1% by default). Write access
 * and moving entries to the top of the stack is synchronized per segment.
 *
 * @author Thomas Mueller
 * @param <K> the key type
 * @param <V> the value type
 */
public class CacheLIRS<K, V> implements LoadingCache<K, V> {

    static final Logger LOG = LoggerFactory.getLogger(CacheLIRS.class);
    static final ThreadLocal<Integer> CURRENTLY_LOADING = new ThreadLocal<Integer>();
    private static final AtomicInteger NEXT_CACHE_ID = new AtomicInteger();
    private static final boolean PUT_HOT = Boolean.parseBoolean(System.getProperty("oak.cacheLIRS.putHot", "true"));
    
    /**
     * Listener for items that are evicted from the cache. The listener
     * is called for both, resident and non-resident items. In the
     * latter case the passed value is {@code null}.
     * @param <K>  type of the key
     * @param <V>  type of the value
     */
    public interface EvictionCallback<K, V> {

        /**
         * Indicates eviction of an item.
         * <p>
         * <em>Note:</em> It is not safe to call any of {@code CacheLIRS}'s
         * method from withing this callback. Any such call might result in
         * undefined behaviour and Java level deadlocks.
         * <p>
         * The method may be called twice for the same key (first if the entry
         * is resident, and later if the entry is non-resident).
         * 
         * @param key the evicted item's key
         * @param value the evicted item's value or {@code null} if non-resident
         * @param cause the cause of the eviction
         */
        void evicted(@Nonnull K key, @Nullable V value, @Nonnull RemovalCause cause);
    }
    
    final int cacheId = NEXT_CACHE_ID.getAndIncrement();

    /**
     * The maximum memory this cache should use.
     */
    private long maxMemory;

    /**
     * The average memory used by one entry.
     */
    private int averageMemory;

    private final Segment<K, V>[] segments;

    private final int segmentCount;
    private final int segmentShift;
    private final int segmentMask;
    private final int stackMoveDistance;

    private final Weigher<K, V> weigher;

    private final CacheLoader<K, V> loader;

    /**
     * The eviction listener of this cache or {@code null} if none.
     */
    private final EvictionCallback<K, V> evicted;

    /**
     * A concurrent hash map of keys where loading is in progress. Key: the
     * cache key. Value: a synchronization object. The threads that wait for the
     * value to be loaded need to wait on the synchronization object. The
     * loading thread will notify all waiting threads once loading is done.
     */
    final ConcurrentHashMap<K, AtomicBoolean> loadingInProgress =
            new ConcurrentHashMap<K, AtomicBoolean>();

    /**
     * Create a new cache with the given number of entries, and the default
     * settings (an average size of 1 per entry, 16 segments, and stack move
     * distance equals to the maximum number of entries divided by 100).
     *
     * @param maxEntries the maximum number of entries
     */
    public CacheLIRS(int maxEntries) {
        this(null, maxEntries, 1, 16, maxEntries / 100, null, null, null);
    }

    /**
     * Create a new cache with the given memory size.
     *
     * @param maxMemory the maximum memory to use (1 or larger)
     * @param averageMemory the average memory (1 or larger)
     * @param segmentCount the number of cache segments (must be a power of 2)
     * @param stackMoveDistance how many other item are to be moved to the top
     *        of the stack before the current item is moved
     * @param  evicted the eviction listener of this segment or {@code null} if none.
     */
    @SuppressWarnings("unchecked")
    CacheLIRS(Weigher<K, V> weigher, long maxMemory, int averageMemory,
            int segmentCount, int stackMoveDistance, final CacheLoader<K, V> loader,
            EvictionCallback<K, V> evicted, String module) {
        LOG.debug("Init #{}, module={}, maxMemory={}, segmentCount={}, stackMoveDistance={}",
                cacheId, module, maxMemory, segmentCount, segmentCount);
        this.weigher = weigher;
        setMaxMemory(maxMemory);
        setAverageMemory(averageMemory);
        if (Integer.bitCount(segmentCount) != 1) {
            throw new IllegalArgumentException("The segment count must be a power of 2, is " + segmentCount);
        }
        this.segmentCount = segmentCount;
        this.segmentMask = segmentCount - 1;
        this.stackMoveDistance = stackMoveDistance;
        segments = new Segment[segmentCount];
        this.evicted = evicted;
        invalidateAll();
        this.segmentShift = Integer.numberOfTrailingZeros(segments[0].entries.length);
        this.loader = loader;
    }

    /**
     * Remove all entries.
     */
    @Override
    public void invalidateAll() {
        long max = Math.max(1, maxMemory / segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            Segment<K, V> old = segments[i];
            Segment<K, V> s = new Segment<K, V>(this,
                    max, averageMemory, stackMoveDistance);
            if (old != null) {
                s.hitCount = old.hitCount;
                s.missCount = old.missCount;
                s.loadSuccessCount = old.loadSuccessCount;
                s.loadExceptionCount = old.loadExceptionCount;
                s.totalLoadTime = old.totalLoadTime;
                s.evictionCount = old.evictionCount;
            }
            setSegment(i, s);
        }
    }

    private void setSegment(int index, Segment<K, V> s) {
        Segment<K, V> old = segments[index];
        segments[index] = s;
        if (evicted != null && old != null && old != s) {
            old.evictedAll(RemovalCause.EXPLICIT);
        }
    }

    void evicted(Entry<K, V> entry, RemovalCause cause) {
        if (evicted == null) {
            return;
        }
        K key = entry.key;
        if (key != null) {
            evicted.evicted(key, entry.value, cause);
        }
    }

    /**
     * Check whether there is a resident entry for the given key. This
     * method does not adjust the internal state of the cache.
     *
     * @param key the key (may not be null)
     * @return true if there is a resident entry
     */
    public boolean containsKey(Object key) {
        int hash = getHash(key);
        return getSegment(hash).containsKey(key, hash);
    }

    /**
     * Get the value for the given key if the entry is cached. This method does
     * not modify the internal state.
     *
     * @param key the key (may not be null)
     * @return the value, or null if there is no resident entry
     */
    public V peek(K key) {
        int hash = getHash(key);
        Entry<K, V> e = getSegment(hash).find(key, hash);
        return e == null ? null : e.value;
    }

    /**
     * Add an entry to the cache. This method is an explicit memory size
     * (weight), and not using the weigher even if configured. The entry may or
     * may not exist in the cache yet. This method will usually mark unknown
     * entries as cold and known entries as hot.
     * 
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @param memory the memory used for the given entry
     * @return the old value, or null if there was no resident entry
     */
    public V put(K key, V value, int memory) {
        int hash = getHash(key);
        return getSegment(hash).put(key, hash, value, memory);
    }

    /**
     * Add an entry to the cache. If a weigher is specified, it is used,
     * otherwise the average memory size is used.
     * 
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     */
    @Override
    public void put(K key, V value) {
        put(key, value, sizeOf(key, value));
    }

    @Override
    public V get(K key, Callable<? extends V> valueLoader)
            throws ExecutionException {
        int hash = getHash(key);
        return getSegment(hash).get(key, hash, valueLoader);
    }

    /**
     * Get the value, loading it if needed.
     * <p>
     * If there is an exception loading, an UncheckedExecutionException is
     * thrown.
     *
     * @param key the key
     * @return the value
     * @throws UncheckedExecutionException
     */
    @Override
    public V getUnchecked(K key) {
        try {
            return get(key);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    /**
     * Get the value, loading it if needed.
     *
     * @param key the key
     * @return the value
     * @throws ExecutionException
     */
    @Override
    public V get(K key) throws ExecutionException {
        int hash = getHash(key);
        return getSegment(hash).get(key, hash, loader);
    }

    /**
     * Re-load the value for the given key.
     * <p>
     * If there is an exception while loading, it is logged and ignored. This
     * method calls CacheLoader.reload, but synchronously replaces the old
     * value.
     *
     * @param key the key
     */
    @Override
    public void refresh(K key) {
        int hash = getHash(key);
        try {
            getSegment(hash).refresh(key, hash, loader);
        } catch (ExecutionException e) {
            LOG.warn("Could not refresh value for key " + key, e);
        }
    }

    V replace(K key, V value) {
        int hash = getHash(key);
        return getSegment(hash).replace(key, hash, value, sizeOf(key, value));
    }

    boolean replace(K key, V oldValue, V newValue) {
        int hash = getHash(key);
        return getSegment(hash).replace(key, hash, oldValue, newValue, sizeOf(key, newValue));
    }

    boolean remove(Object key, Object value) {
        int hash = getHash(key);
        return getSegment(hash).remove(key, hash, value);
    }

    protected V putIfAbsent(K key, V value) {
        int hash = getHash(key);
        return getSegment(hash).putIfAbsent(key, hash, value, sizeOf(key, value));
    }

    /**
     * Get the value for the given key if the entry is cached. This method
     * adjusts the internal state of the cache sometimes, to ensure commonly
     * used entries stay in the cache.
     *
     * @param key the key (may not be null)
     * @return the value, or null if there is no resident entry
     */
    @Override
    @Nullable
    public V getIfPresent(Object key) {
        int hash = getHash(key);
        return getSegment(hash).get(key, hash);
    }

    /**
     * Get the size of the given value. The default implementation returns the
     * average memory as configured for this cache.
     *
     * @param key the key
     * @param value the value
     * @return the size
     */
    protected int sizeOf(K key, V value) {
        if (weigher == null) {
            return averageMemory;
        }
        return weigher.weigh(key, value);
    }

    /**
     * Remove an entry. Both resident and non-resident entries can be
     * removed.
     *
     * @param key the key (may not be null)
     */
    @Override
    public void invalidate(Object key) {
        int hash = getHash(key);
        getSegment(hash).invalidate(key, hash, RemovalCause.EXPLICIT);
    }

    /**
     * Remove an entry. Both resident and non-resident entries can be
     * removed.
     *
     * @param key the key (may not be null)
     * @return the old value or null
     */
    public V remove(Object key) {
        int hash = getHash(key);
        return getSegment(hash).remove(key, hash);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void invalidateAll(Iterable<?> keys) {
        for (K k : (Iterable<K>) keys) {
            invalidate(k);
        }
    }

    /**
     * Get the memory used for the given key.
     *
     * @param key the key (may not be null)
     * @return the memory, or 0 if there is no resident entry
     */
    public int getMemory(K key) {
        int hash = getHash(key);
        return getSegment(hash).getMemory(key, hash);
    }

    private Segment<K, V> getSegment(int hash) {
        int segmentIndex = (hash >>> segmentShift) & segmentMask;
        return segments[segmentIndex];
    }

    /**
     * Get the hash code for the given key. The hash code is
     * further enhanced to spread the values more evenly.
     *
     * @param key the key
     * @return the hash code
     */
    static int getHash(Object key) {
        int hash = key.hashCode();
        // a supplemental secondary hash function
        // to protect against hash codes that don't differ much
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = ((hash >>> 16) ^ hash) * 0x45d9f3b;
        hash = (hash >>> 16) ^ hash;
        return hash;
    }

    /**
     * Get the currently used memory.
     *
     * @return the used memory
     */
    public long getUsedMemory() {
        long x = 0;
        for (Segment<K, V> s : segments) {
            x += s.usedMemory;
        }
        return x;
    }

    /**
     * Set the maximum memory this cache should use. This will not
     * immediately cause entries to get removed however; it will only change
     * the limit. To resize the internal array, call the clear method.
     *
     * @param maxMemory the maximum size (1 or larger)
     */
    public void setMaxMemory(long maxMemory) {
        if (maxMemory < 0) {
            throw new IllegalArgumentException("Max memory must not be negative");
        }
        this.maxMemory = maxMemory;
        if (segments != null) {
            long max = 1 + maxMemory / segments.length;
            for (Segment<K, V> s : segments) {
                s.setMaxMemory(max);
            }
        }
    }

    /**
     * Set the average memory used per entry. It is used to calculate the
     * length of the internal array.
     *
     * @param averageMemory the average memory used (1 or larger)
     */
    public void setAverageMemory(int averageMemory) {
        if (averageMemory <= 0) {
            throw new IllegalArgumentException("Average memory must be larger than 0");
        }
        this.averageMemory = averageMemory;
        if (segments != null) {
            for (Segment<K, V> s : segments) {
                s.setAverageMemory(averageMemory);
            }
        }
    }

    /**
     * Get the average memory used per entry.
     *
     * @return the average memory
     */
    public int getAverageMemory() {
        return averageMemory;
    }

    /**
     * Get the maximum memory to use.
     *
     * @return the maximum memory
     */
    public long getMaxMemory() {
        return maxMemory;
    }

    /**
     * Get the entry set for all resident entries.
     *
     * @return the entry set
     */
    public synchronized Set<Map.Entry<K, V>> entrySet() {
        HashMap<K, V> map = new HashMap<K, V>();
        for (K k : keySet()) {
            V v = peek(k);
            if (v != null) {
                map.put(k,  v);
            }
        }
        return map.entrySet();
    }

    protected Collection<V> values() {
        ArrayList<V> list = new ArrayList<V>();
        for (K k : keySet()) {
            V v = peek(k);
            if (v != null) {
                list.add(v);
            }
        }
        return list;
    }

    boolean containsValue(Object value) {
        for (Segment<K, V> s : segments) {
            for (K k : s.keySet()) {
                V v = peek(k);
                if (v != null && v.equals(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Get the set of keys for resident entries.
     *
     * @return the set of keys
     */
    public synchronized Set<K> keySet() {
        HashSet<K> set = new HashSet<K>();
        for (Segment<K, V> s : segments) {
            set.addAll(s.keySet());
        }
        return set;
    }

    /**
     * Get the number of non-resident entries in the cache.
     *
     * @return the number of non-resident entries
     */
    public int sizeNonResident() {
        int x = 0;
        for (Segment<K, V> s : segments) {
            x += s.queue2Size;
        }
        return x;
    }

    /**
     * Get the length of the internal map array.
     *
     * @return the size of the array
     */
    public int sizeMapArray() {
        int x = 0;
        for (Segment<K, V> s : segments) {
            x += s.entries.length;
        }
        return x;
    }

    /**
     * Get the number of hot entries in the cache.
     *
     * @return the number of hot entries
     */
    public int sizeHot() {
        int x = 0;
        for (Segment<K, V> s : segments) {
            x += s.mapSize - s.queueSize - s.queue2Size;
        }
        return x;
    }

    /**
     * Get the number of resident entries.
     *
     * @return the number of entries
     */
    @Override
    public long size() {
        int x = 0;
        for (Segment<K, V> s : segments) {
            x += s.mapSize - s.queue2Size;
        }
        return x;
    }

    void clear() {
        for (Segment<K, V> s : segments) {
            synchronized (s) {
                if (evicted != null) {
                    s.evictedAll(RemovalCause.EXPLICIT);
                }
                s.clear();
            }
        }
    }

    /**
     * Get the list of keys. This method allows to read the internal state of
     * the cache.
     *
     * @param cold if true, only keys for the cold entries are returned
     * @param nonResident true for non-resident entries
     * @return the key list
     */
    public synchronized List<K> keys(boolean cold, boolean nonResident) {
        ArrayList<K> keys = new ArrayList<K>();
        for (Segment<K, V> s : segments) {
            keys.addAll(s.keys(cold, nonResident));
        }
        return keys;
    }

    @Override
    public CacheStats stats() {
        long hitCount = 0;
        long missCount = 0;
        long loadSuccessCount = 0;
        long loadExceptionCount = 0;
        long totalLoadTime = 0;
        long evictionCount = 0;
        for (Segment<K, V> s : segments) {
            hitCount += s.hitCount;
            missCount += s.missCount;
            loadSuccessCount += s.loadSuccessCount;
            loadExceptionCount += s.loadExceptionCount;
            totalLoadTime += s.totalLoadTime;
            evictionCount += s.evictionCount;
        }
        CacheStats stats = new CacheStats(hitCount, missCount, loadSuccessCount,
                loadExceptionCount, totalLoadTime, evictionCount);
        return stats;
    }

    /**
     * A cache segment
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    static class Segment<K, V> {

        /**
         * The number of (hot, cold, and non-resident) entries in the map.
         */
        int mapSize;

        /**
         * The size of the LIRS queue for resident cold entries.
         */
        int queueSize;

        /**
         * The size of the LIRS queue for non-resident cold entries.
         */
        int queue2Size;

        /**
         * The map array. The size is always a power of 2. The bit mask that is
         * applied to the key hash code to get the index in the map array. The
         * mask is the length of the array minus one.
         */
        Entry<K, V>[] entries;

        /**
         * The currently used memory.
         */
        long usedMemory;

        long hitCount;
        long missCount;
        long loadSuccessCount;
        long loadExceptionCount;
        long totalLoadTime;
        long evictionCount;

        /**
         * The cache.
         */
        private final CacheLIRS<K, V> cache;

        /**
         * How many other item are to be moved to the top of the stack before
         * the current item is moved.
         */
        private final int stackMoveDistance;

        /**
         * The maximum memory this cache should use.
         */
        private long maxMemory;

        /**
         * The average memory used by one entry.
         */
        private int averageMemory;

        /**
         * The LIRS stack size.
         */
        private int stackSize;

        /**
         * The stack of recently referenced elements. This includes all hot
         * entries, and the recently referenced cold entries. Resident cold
         * entries that were not recently referenced, as well as non-resident
         * cold entries, are not in the stack.
         * <p>
         * There is always at least one entry: the head entry.
         */
        private Entry<K, V> stack;

        /**
         * The queue of resident cold entries.
         * <p>
         * There is always at least one entry: the head entry.
         */
        private Entry<K, V> queue;

        /**
         * The queue of non-resident cold entries.
         * <p>
         * There is always at least one entry: the head entry.
         */
        private Entry<K, V> queue2;

        /**
         * The number of times any item was moved to the top of the stack.
         */
        private int stackMoveCounter;

        /**
         * Create a new cache.
         *  @param maxMemory the maximum memory to use
         * @param averageMemory the average memory usage of an object
         * @param stackMoveDistance the number of other entries to be moved to
         *        the top of the stack before moving an entry to the top
         * @param evicted  the eviction listener of this segment or {@code null} if none.
         */
        Segment(CacheLIRS<K, V> cache, long maxMemory, int averageMemory, int stackMoveDistance) {
            this.cache = cache;
            setMaxMemory(maxMemory);
            setAverageMemory(averageMemory);
            this.stackMoveDistance = stackMoveDistance;
            clear();
        }

        public void evictedAll(RemovalCause cause) {
            for (Entry<K, V> e = stack.stackNext; e != stack; e = e.stackNext) {
                if (e.value != null) {
                    cache.evicted(e, cause);
                }
            }
            for (Entry<K, V> e = queue.queueNext; e != queue; e = e.queueNext) {
                if (e.stackNext == null) {
                    cache.evicted(e, cause);
                }
            }
            for (Entry<K, V> e = queue2.queueNext; e != queue2; e = e.queueNext) {
                cache.evicted(e, cause);
            }
        }

        synchronized void clear() {

            // calculate the size of the map array
            // assume a fill factor of at most 80%
            long maxLen = (long) (maxMemory / averageMemory / 0.75);
            // the size needs to be a power of 2
            long l = 8;
            while (l < maxLen) {
                l += l;
            }
            // the array size is at most 2^31 elements
            int len = (int) Math.min(1L << 31, l);

            // initialize the stack and queue heads
            stack = new Entry<K, V>();
            stack.stackPrev = stack.stackNext = stack;
            queue = new Entry<K, V>();
            queue.queuePrev = queue.queueNext = queue;
            queue2 = new Entry<K, V>();
            queue2.queuePrev = queue2.queueNext = queue2;

            // first set to a small array, to avoiding out of memory
            @SuppressWarnings("unchecked")
            Entry<K, V>[] small = new Entry[1];
            entries = small;
            @SuppressWarnings("unchecked")
            Entry<K, V>[] e = new Entry[len];
            entries = e;

            mapSize = 0;
            usedMemory = 0;
            stackSize = queueSize = queue2Size = 0;
        }

        /**
         * Get the memory used for the given key.
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @return the memory, or 0 if there is no resident entry
         */
        int getMemory(K key, int hash) {
            Entry<K, V> e = find(key, hash);
            return e == null ? 0 : e.memory;
        }

        /**
         * Get the value for the given key if the entry is cached. This method
         * adjusts the internal state of the cache sometimes, to ensure commonly
         * used entries stay in the cache.
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @return the value, or null if there is no resident entry
         */
        V get(Object key, int hash) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("#{} get hash {} key {}", cache.cacheId, hash, key);
            }
            Entry<K, V> e = find(key, hash);
            if (e == null) {
                // the entry was not found
                missCount++;
                return null;
            }
            V value = e.value;
            if (value == null) {
                // it was a non-resident entry
                missCount++;
                return null;
            }
            if (e.isHot()) {
                if (e != stack.stackNext) {
                    if (stackMoveDistance == 0 || stackMoveCounter - e.topMove > stackMoveDistance) {
                        access(key, hash);
                    }
                }
            } else {
                access(key, hash);
            }
            hitCount++;
            return value;
        }

        /**
         * Access an item, moving the entry to the top of the stack or front of the
         * queue if found.
         *
         * @param key the key
         */
        private synchronized void access(Object key, int hash) {
            Entry<K, V> e = find(key, hash);
            if (e == null || e.value == null) {
                return;
            }
            if (e.isHot()) {
                if (e != stack.stackNext) {
                    if (stackMoveDistance == 0 || stackMoveCounter - e.topMove > stackMoveDistance) {
                        // move a hot entry to the top of the stack
                        // unless it is already there
                        boolean wasEnd = e == stack.stackPrev;
                        removeFromStack(e);
                        if (wasEnd) {
                            // if moving the last entry, the last entry
                            // could now be cold, which is not allowed
                            pruneStack();
                        }
                        addToStack(e);
                    }
                }
            } else {
                removeFromQueue(e);
                if (e.stackNext != null) {
                    // resident cold entries become hot
                    // if they are on the stack
                    removeFromStack(e);
                    // which means a hot entry needs to become cold
                    // (this entry is cold, that means there is at least one
                    // more entry in the stack, which must be hot)
                    convertOldestHotToCold();
                } else {
                    // cold entries that are not on the stack
                    // move to the front of the queue
                    addToQueue(queue, e);
                }
                // in any case, the cold entry is moved to the top of the stack
                addToStack(e);
            }
        }

        V get(K key, int hash, Callable<? extends V> valueLoader) throws ExecutionException {
            // we can not synchronize on a per-segment object while loading,
            // because we don't want to block cache access while loading, and
            // because the value loader could access the cache (for example,
            // using put, or another get with a loader), which might result in a
            // deadlock
            // we loop here because another thread might load the value,
            // but loading might fail there, so we might need to repeat this
            while (true) {
                V value = get(key, hash);
                // the (hopefully) normal case
                if (value != null) {
                    return value;
                }
                // if we are within a loader, and are currently loading
                // an entry, then we need to avoid a possible deadlock
                // (we ensure that while loading an entry, we only load
                // entries with a higher hash code, so there is a clear order)
                Integer outer = CURRENTLY_LOADING.get();
                if (outer != null && hash <= outer) {
                    // to prevent a deadlock, we also load the value ourselves
                    return load(key, hash, valueLoader);
                }
                ConcurrentHashMap<K, AtomicBoolean> loading = cache.loadingInProgress;
                // the object we have to wait for in case another thread loads
                // this value
                AtomicBoolean alreadyLoading;
                // synchronized on this object, even before we put it in the
                // cache, so that all other threads that get this object can
                // synchronized and wait for it
                AtomicBoolean loadNow = new AtomicBoolean();
                // we synchronize a bit early here, but that's fine (we don't
                // optimize for the case where loading is extremely quick)
                synchronized (loadNow) {
                    alreadyLoading = loading.putIfAbsent(key, loadNow);
                    if (alreadyLoading == null) {
                        // we are loading ourselves
                        try {
                            CURRENTLY_LOADING.set(hash);
                            return load(key, hash, valueLoader);
                        } finally {
                            loading.remove(key);
                            if (loadNow.get()) {
                                // notify other threads, but only if
                                // they wait for this to be loaded
                                loadNow.notifyAll();
                            }
                            CURRENTLY_LOADING.remove();
                        }
                    }
                }
                // another thread is (or was) already loading
                synchronized (alreadyLoading) {
                    alreadyLoading.set(true);
                    // loading might have been finished, so check again
                    AtomicBoolean alreadyLoading2 = loading.get(key);
                    if (alreadyLoading2 != alreadyLoading) {
                        // loading has completed before we could synchronize,
                        // so we repeat
                        continue;
                    }
                    // still loading: wait
                    try {
                        // we could wait longer than 10 ms, but we are
                        // in case notify is not called for some weird reason
                        // (for example out of memory)
                        alreadyLoading.wait(10);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        }

        V load(K key, int hash, Callable<? extends V> valueLoader) throws ExecutionException {
            V value;
            long start = System.nanoTime();
            try {
                value = valueLoader.call();
                loadSuccessCount++;
            } catch (Exception e) {
                loadExceptionCount++;
                throw new ExecutionException(e);
            } finally {
                long time = System.nanoTime() - start;
                totalLoadTime += time;
            }
            put(key, hash, value, cache.sizeOf(key, value));
            return value;
        }

        V get(K key, int hash, CacheLoader<K, V> loader) throws ExecutionException {
            // avoid synchronization if it's in the cache
            V value = get(key, hash);
            if (value != null) {
                return value;
            }
            if (loader == null) {
                return null;
            }
            synchronized (this) {
                value = get(key, hash);
                if (value != null) {
                    return value;
                }
                long start = System.nanoTime();
                try {
                    value = loader.load(key);
                    loadSuccessCount++;
                } catch (Exception e) {
                    loadExceptionCount++;
                    throw new ExecutionException(e);
                } finally {
                    long time = System.nanoTime() - start;
                    totalLoadTime += time;
                }
                put(key, hash, value, cache.sizeOf(key, value));
                return value;
            }
        }

        synchronized V replace(K key, int hash, V value, int memory) {
            if (containsKey(key, hash)) {
                return put(key, hash, value, memory);
            }
            return null;
        }

        synchronized boolean replace(K key, int hash, V oldValue, V newValue, int memory) {
            V old = get(key, hash);
            if (old != null && old.equals(oldValue)) {
                put(key, hash, newValue, memory);
                return true;
            }
            return false;
        }

        synchronized boolean remove(Object key, int hash, Object value) {
            V old = get(key, hash);
            if (old != null && old.equals(value)) {
                invalidate(key, hash, RemovalCause.EXPLICIT);
                return true;
            }
            return false;
        }

        synchronized V remove(Object key, int hash) {
            V old = get(key, hash);
            // even if old is null, there might still be a cold entry
            invalidate(key, hash, RemovalCause.EXPLICIT);
            return old;
        }

        synchronized V putIfAbsent(K key, int hash, V value, int memory) {
            V old = get(key, hash);
            if (old == null) {
                put(key, hash, value, memory);
                return null;
            }
            return old;
        }

        synchronized void refresh(K key, int hash, CacheLoader<K, V> loader) throws ExecutionException {
            if (loader == null) {
                // no loader - no refresh
                return;
            }
            V value;
            V old = get(key, hash);
            long start = System.nanoTime();
            try {
                if (old == null) {
                    value = loader.load(key);
                } else {
                    ListenableFuture<V> future = loader.reload(key, old);
                    value = future.get();
                }
                loadSuccessCount++;
            } catch (Exception e) {
                loadExceptionCount++;
                throw new ExecutionException(e);
            } finally {
                long time = System.nanoTime() - start;
                totalLoadTime += time;
            }
            put(key, hash, value, cache.sizeOf(key, value));
        }

        /**
         * Add an entry to the cache. The entry may or may not exist in the
         * cache yet. This method will usually mark unknown entries as cold and
         * known entries as hot.
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @param value the value (may not be null)
         * @param memory the memory used for the given entry
         * @return the old value, or null if there was no resident entry
         */
        synchronized V put(K key, int hash, V value, int memory) {
            if (value == null) {
                throw new NullPointerException("The value may not be null");
            }
            V old;
            Entry<K, V> e = find(key, hash);
            boolean existed;
            if (e == null) {
                existed = false;
                old = null;
            } else {
                existed = true;
                old = e.value;
                invalidate(key, hash, RemovalCause.REPLACED);
            }
            e = new Entry<K, V>();
            e.key = key;
            e.value = value;
            e.memory = memory;
            Entry<K, V>[] array = entries;
            int mask = array.length - 1;
            int index = hash & mask;
            e.mapNext = array[index];
            array[index] = e;
            usedMemory += memory;
            if (usedMemory > maxMemory && mapSize > 0) {
                // an old entry needs to be removed
                evict(e);
            }
            mapSize++;
            // added entries are always added to the stack
            addToStack(e);
            if (existed) {
                // if it was there before (even non-resident), it becomes hot
                if (PUT_HOT) {
                    access(key, hash);
                }
            }
            return old;
        }

        /**
         * Remove an entry. Both resident and non-resident entries can be
         * removed.
         *
         * @param key the key (may not be null)
         * @param hash the hash
         */
        synchronized void invalidate(Object key, int hash, RemovalCause cause) {
            Entry<K, V>[] array = entries;
            int mask = array.length - 1;
            int index = hash & mask;
            Entry<K, V> e = array[index];
            if (e == null) {
                return;
            }
            if (e.key.equals(key)) {
                array[index] = e.mapNext;
            } else {
                Entry<K, V> last;
                do {
                    last = e;
                    e = e.mapNext;
                    if (e == null) {
                        return;
                    }
                } while (!e.key.equals(key));
                last.mapNext = e.mapNext;
            }
            mapSize--;
            usedMemory -= e.memory;
            if (e.stackNext != null) {
                removeFromStack(e);
            }
            if (e.isHot()) {
                // when removing a hot entry, the newest cold entry gets hot,
                // so the number of hot entries does not change
                Entry<K, V> nc = queue.queueNext;
                if (nc != queue) {
                    removeFromQueue(nc);
                    if (nc.stackNext == null) {
                        addToStackBottom(nc);
                    }
                }
            } else {
                removeFromQueue(e);
            }
            pruneStack();
            cache.evicted(e, cause);
        }

        /**
         * Evict cold entries (resident and non-resident) until the memory limit is
         * reached. The new entry is added as a cold entry, except if it is the only
         * entry.
         *
         * @param newCold a new cold entry
         */
        private void evict(Entry<K, V> newCold) {
            // ensure there are not too many hot entries: right shift of 5 is
            // division by 32, that means if there are only 1/32 (3.125%) or
            // less cold entries, a hot entry needs to become cold
            while (queueSize <= (mapSize >>> 5) && stackSize > 0) {
                convertOldestHotToCold();
            }
            if (stackSize > 0) {
                // the new cold entry is at the top of the queue
                addToQueue(queue, newCold);
            }
            // the oldest resident cold entries become non-resident
            // but at least one cold entry (the new one) must stay
            while (usedMemory > maxMemory && queueSize > 1) {
                Entry<K, V> e = queue.queuePrev;
                usedMemory -= e.memory;
                evictionCount++;
                removeFromQueue(e);
                cache.evicted(e, RemovalCause.SIZE);
                e.value = null;
                e.memory = 0;
                addToQueue(queue2, e);
                // the size of the non-resident-cold entries needs to be limited
                while (queue2Size + queue2Size > stackSize) {
                    e = queue2.queuePrev;
                    int hash = getHash(e.key);
                    invalidate(e.key, hash, RemovalCause.SIZE);
                }
            }
        }

        private void convertOldestHotToCold() {
            // the last entry of the stack is known to be hot
            Entry<K, V> last = stack.stackPrev;
            if (last == stack) {
                // never remove the stack head itself (this would mean the
                // internal structure of the cache is corrupt)
                throw new IllegalStateException();
            }
            // remove from stack - which is done anyway in the stack pruning, but we
            // can do it here as well
            removeFromStack(last);
            // adding an entry to the queue will make it cold
            addToQueue(queue, last);
            pruneStack();
        }

        /**
         * Ensure the last entry of the stack is cold.
         */
        private void pruneStack() {
            while (true) {
                Entry<K, V> last = stack.stackPrev;
                // must stop at a hot entry or the stack head,
                // but the stack head itself is also hot, so we
                // don't have to test it
                if (last.isHot()) {
                    break;
                }
                // the cold entry is still in the queue
                removeFromStack(last);
            }
        }

        /**
         * Try to find an entry in the map.
         *
         * @param key the key
         * @param hash the hash
         * @return the entry (might be a non-resident)
         */
        Entry<K, V> find(Object key, int hash) {
            Entry<K, V>[] array = entries;
            int mask = array.length - 1;
            int index = hash & mask;
            Entry<K, V> e = array[index];
            while (e != null && !e.key.equals(key)) {
                e = e.mapNext;
            }
            return e;
        }

        private void addToStack(Entry<K, V> e) {
            e.stackPrev = stack;
            e.stackNext = stack.stackNext;
            e.stackNext.stackPrev = e;
            stack.stackNext = e;
            stackSize++;
            e.topMove = stackMoveCounter++;
        }

        private void addToStackBottom(Entry<K, V> e) {
            e.stackNext = stack;
            e.stackPrev = stack.stackPrev;
            e.stackPrev.stackNext = e;
            stack.stackPrev = e;
            stackSize++;
        }

        private void removeFromStack(Entry<K, V> e) {
            e.stackPrev.stackNext = e.stackNext;
            e.stackNext.stackPrev = e.stackPrev;
            e.stackPrev = e.stackNext = null;
            stackSize--;
        }

        private void addToQueue(Entry<K, V> q, Entry<K, V> e) {
            e.queuePrev = q;
            e.queueNext = q.queueNext;
            e.queueNext.queuePrev = e;
            q.queueNext = e;
            if (e.value != null) {
                queueSize++;
            } else {
                queue2Size++;
            }
        }

        private void removeFromQueue(Entry<K, V> e) {
            e.queuePrev.queueNext = e.queueNext;
            e.queueNext.queuePrev = e.queuePrev;
            e.queuePrev = e.queueNext = null;
            if (e.value != null) {
                queueSize--;
            } else {
                queue2Size--;
            }
        }

        /**
         * Get the list of keys. This method allows to read the internal state of
         * the cache.
         *
         * @param cold if true, only keys for the cold entries are returned
         * @param nonResident true for non-resident entries
         * @return the key list
         */
        synchronized List<K> keys(boolean cold, boolean nonResident) {
            ArrayList<K> keys = new ArrayList<K>();
            if (cold) {
                Entry<K, V> start = nonResident ? queue2 : queue;
                for (Entry<K, V> e = start.queueNext; e != start; e = e.queueNext) {
                    keys.add(e.key);
                }
            } else {
                for (Entry<K, V> e = stack.stackNext; e != stack; e = e.stackNext) {
                    keys.add(e.key);
                }
            }
            return keys;
        }

        /**
         * Check whether there is a resident entry for the given key. This
         * method does not adjust the internal state of the cache.
         *
         * @param key the key (may not be null)
         * @param hash the hash
         * @return true if there is a resident entry
         */
        boolean containsKey(Object key, int hash) {
            Entry<K, V> e = find(key, hash);
            return e != null && e.value != null;
        }

        /**
         * Get the set of keys for resident entries.
         *
         * @return the set of keys
         */
        synchronized Set<K> keySet() {
            HashSet<K> set = new HashSet<K>();
            for (Entry<K, V> e = stack.stackNext; e != stack; e = e.stackNext) {
                set.add(e.key);
            }
            for (Entry<K, V> e = queue.queueNext; e != queue; e = e.queueNext) {
                set.add(e.key);
            }
            return set;
        }

        /**
         * Set the maximum memory this cache should use. This will not
         * immediately cause entries to get removed however; it will only change
         * the limit. To resize the internal array, call the clear method.
         *
         * @param maxMemory the maximum size (1 or larger)
         */
        void setMaxMemory(long maxMemory) {
            if (maxMemory <= 0) {
                throw new IllegalArgumentException("Max memory must be larger than 0");
            }
            this.maxMemory = maxMemory;
        }

        /**
         * Set the average memory used per entry. It is used to calculate the
         * length of the internal array.
         *
         * @param averageMemory the average memory used (1 or larger)
         */
        void setAverageMemory(int averageMemory) {
            if (averageMemory <= 0) {
                throw new IllegalArgumentException("Average memory must be larger than 0");
            }
            this.averageMemory = averageMemory;
        }

    }

    /**
     * A cache entry. Each entry is either hot (low inter-reference recency;
     * LIR), cold (high inter-reference recency; HIR), or non-resident-cold. Hot
     * entries are in the stack only. Cold entries are in the queue, and may be
     * in the stack. Non-resident-cold entries have their value set to null and
     * are in the stack and in the non-resident queue.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    static class Entry<K, V> {

        /**
         * The key.
         */
        K key;

        /**
         * The value. Set to null for non-resident-cold entries.
         */
        V value;

        /**
         * The estimated memory used.
         */
        int memory;

        /**
         * When the item was last moved to the top of the stack.
         */
        int topMove;

        /**
         * The next entry in the stack.
         */
        Entry<K, V> stackNext;

        /**
         * The previous entry in the stack.
         */
        Entry<K, V> stackPrev;

        /**
         * The next entry in the queue (either the resident queue or the
         * non-resident queue).
         */
        Entry<K, V> queueNext;

        /**
         * The previous entry in the queue.
         */
        Entry<K, V> queuePrev;

        /**
         * The next entry in the map
         */
        Entry<K, V> mapNext;

        /**
         * Whether this entry is hot. Cold entries are in one of the two queues.
         *
         * @return whether the entry is hot
         */
        boolean isHot() {
            return queueNext == null;
        }

    }

    /**
     * A builder for the cache.
     */
    public static class Builder<K, V> {

        private String module;
        private Weigher<K, V> weigher;
        private long maxWeight;
        private int averageWeight = 100;
        private int segmentCount = 16;
        private int stackMoveDistance = 16;
        private EvictionCallback<K, V> evicted;

        public Builder<K, V> recordStats() {
            return this;
        }
        
        public Builder<K, V> module(String module) {
            this.module = module;
            return this;
        }

        /**
         * Set the weigher which is used if memory usage of an entry is not
         * explicitly set (when adding entries).
         * 
         * @param weigher the weigher
         * @return this
         */
        public Builder<K, V> weigher(Weigher<K, V> weigher) {
            this.weigher = weigher;
            return this;
        }

        /**
         * Set the total maximum weight. If the cache is heavier, then entries
         * are evicted.
         * 
         * @param maxWeight the maximum weight
         * @return this
         */
        public Builder<K, V> maximumWeight(long maxWeight) {
            this.maxWeight = maxWeight;
            return this;
        }

        /**
         * Set the average weight of an entry. This is used, together with the
         * maximum weight, to calculate the length of the internal array of the
         * cache.
         * 
         * For higher performance, the weight should be set relatively low, at
         * the cost of some space. To save space, the average weight should be
         * set high, at the cost of some performance.
         * 
         * @param averageWeight the average weight
         * @return this
         */
        public Builder<K, V> averageWeight(int averageWeight) {
            this.averageWeight = averageWeight;
            return this;
        }

        /**
         * Set the maximum size (in number of entries). This is the same as
         * setting the average weight of an entry to 1, and the maximum weight
         * to the maximum size.
         * 
         * @param maxSize the maximum size
         * @return this
         */
        public Builder<K, V> maximumSize(long maxSize) {
            this.maxWeight = maxSize;
            this.averageWeight = 1;
            return this;
        }

        public Builder<K, V> segmentCount(int segmentCount) {
            if (Integer.bitCount(segmentCount) != 1 || segmentCount < 0 || segmentCount > 65536) {
                LOG.warn("Illegal segment count: " + segmentCount + ", using 16");
                segmentCount = 16;
            }
            this.segmentCount = segmentCount;
            return this;
        }

        /**
         * How many other item are to be moved to the top of the stack before
         * the current item is moved. The default is 16. Using higher values
         * will avoid re-ordering in many cases, so less time is spent
         * reordering. But this somewhat reduces cache hit rate, and eviction
         * will become more random. Typically, cache hit rate can be improved by
         * using smaller values, and access performance can be improved using
         * larger values. Using values larger than 128 is not recommended.
         */
        public Builder<K, V> stackMoveDistance(int stackMoveDistance) {
            if (stackMoveDistance < 0) {
                LOG.warn("Illegal stack move distance: " + stackMoveDistance + ", using 16");
                stackMoveDistance = 16;
            }
            this.stackMoveDistance = stackMoveDistance;
            return this;
        }

        public Builder<K, V> evictionCallback(EvictionCallback<K, V> evicted) {
            this.evicted = evicted;
            return this;
        }

        public CacheLIRS<K, V> build() {
            return build(null);
        }

        public CacheLIRS<K, V> build(CacheLoader<K, V> cacheLoader) {
            return new CacheLIRS<K, V>(weigher, maxWeight, averageWeight,
                    segmentCount, stackMoveDistance, cacheLoader, evicted, module);
        }
    }

    /**
     * Create a builder.
     *
     * @return the builder
     */
    public static <K, V> Builder<K, V> newBuilder() {
        return new Builder<K, V>();
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        return new ConcurrentMap<K, V>() {

            @Override
            public int size() {
                long size = CacheLIRS.this.size();
                return (int) Math.min(size, Integer.MAX_VALUE);
            }

            @Override
            public boolean isEmpty() {
                return CacheLIRS.this.size() == 0;
            }

            @Override
            public boolean containsKey(Object key) {
                return CacheLIRS.this.containsKey(key);
            }

            @Override
            public boolean containsValue(Object value) {
                return CacheLIRS.this.containsValue(value);
            }

            @SuppressWarnings("unchecked")
            @Override
            public V get(Object key) {
                return CacheLIRS.this.peek((K) key);
            }

            @Override
            public V put(K key, V value) {
                return CacheLIRS.this.put(key, value, sizeOf(key, value));
            }

            @Override
            public V remove(Object key) {
                @SuppressWarnings("unchecked")
                V old = CacheLIRS.this.getUnchecked((K) key);
                CacheLIRS.this.invalidate(key);
                return old;
            }

            @Override
            public void putAll(Map<? extends K, ? extends V> m) {
                for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
                    put(e.getKey(), e.getValue());
                }                
            }

            @Override
            public void clear() {
                CacheLIRS.this.clear();
            }

            @Override
            public Set<K> keySet() {
                return CacheLIRS.this.keySet();
            }

            @Override
            public Collection<V> values() {
                return CacheLIRS.this.values();
            }

            @Override
            public Set<java.util.Map.Entry<K, V>> entrySet() {
                return CacheLIRS.this.entrySet();
            }

            @Override
            public V putIfAbsent(K key, V value) {
                return CacheLIRS.this.putIfAbsent(key, value);
            }

            @Override
            public boolean remove(Object key, Object value) {
                return CacheLIRS.this.remove(key, value);
            }

            @Override
            public boolean replace(K key, V oldValue, V newValue) {
                return CacheLIRS.this.replace(key, oldValue, newValue);
            }

            @Override
            public V replace(K key, V value) {
                return CacheLIRS.this.replace(key, value);
            }
            
        };
    }

    @Override
    public void cleanUp() {
        // nothing to do
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public ImmutableMap<K, V> getAll(Iterable<? extends K> keys)
            throws ExecutionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public V apply(K key) {
        throw new UnsupportedOperationException();        
    }

    public boolean isEmpty() {
        return size() == 0;
    }

}
