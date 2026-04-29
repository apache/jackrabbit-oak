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
package org.apache.jackrabbit.oak.cache.api;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.jackrabbit.oak.cache.impl.caffeine.CaffeineCacheAdapter;
import org.apache.jackrabbit.oak.cache.impl.caffeine.CaffeineLoadingCacheAdapter;
import org.jetbrains.annotations.NotNull;

/**
 * Builder for Caffeine-backed {@link Cache} and {@link LoadingCache} instances.
 *
 * <p>Example:</p>
 * <pre>{@code
 * Cache<String, NodeState> cache = CacheBuilder.<String, NodeState>newBuilder()
 *         .maximumWeight(64 * 1024 * 1024)
 *         .weigher((k, v) -> v.estimateMemory())
 *         .recordStats()
 *         .build();
 * }</pre>
 *
 * @param <K> the type of cache keys
 * @param <V> the type of cache values
 */
public final class CacheBuilder<K, V> {

    private long maximumWeight = -1;
    private long maximumSize = -1;
    private int initialCapacity = -1;
    private Weigher<? super K, ? super V> weigher;
    private EvictionListener<? super K, ? super V> evictionListener;
    private boolean recordStats;
    private Duration expireAfterAccess;
    private Duration expireAfterWrite;
    private Duration refreshAfterWrite;
    private Supplier<Long> ticker;

    private CacheBuilder() {
    }

    /**
     * Creates a new builder with no pre-configured settings.
     *
     * @param <K> the type of cache keys
     * @param <V> the type of cache values
     * @return a new builder instance
     */
    @NotNull
    public static <K, V> CacheBuilder<K, V> newBuilder() {
        return new CacheBuilder<>();
    }

    /**
     * Sets the maximum total weight of entries the cache may hold.
     * Must be used together with {@link #weigher(Weigher)} and may not be
     * combined with {@link #maximumSize(long)}.
     *
     * @param maximumWeight the maximum weight (must be non-negative)
     * @return this builder
     */
    @NotNull
    public CacheBuilder<K, V> maximumWeight(long maximumWeight) {
        if (maximumWeight < 0) {
            throw new IllegalArgumentException("maximumWeight must be non-negative, got: " + maximumWeight);
        }
        this.maximumWeight = maximumWeight;
        return this;
    }

    /**
     * Sets the minimum number of entries the cache's internal hash table should be
     * pre-sized to hold. Passing this hint avoids rehashing when the cache fills
     * gradually from an empty state.
     *
     * @param initialCapacity the minimum initial capacity (must be non-negative)
     * @return this builder
     */
    @NotNull
    public CacheBuilder<K, V> initialCapacity(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("initialCapacity must be non-negative, got: " + initialCapacity);
        }
        this.initialCapacity = initialCapacity;
        return this;
    }

    /**
     * Sets the maximum number of entries the cache may hold.
     * May not be combined with {@link #maximumWeight(long)}.
     *
     * @param maximumSize the maximum entry count (must be non-negative)
     * @return this builder
     */
    @NotNull
    public CacheBuilder<K, V> maximumSize(long maximumSize) {
        if (maximumSize < 0) {
            throw new IllegalArgumentException("maximumSize must be non-negative, got: " + maximumSize);
        }
        this.maximumSize = maximumSize;
        return this;
    }

    /**
     * Sets the weigher used to determine the weight of each cache entry.
     * Requires {@link #maximumWeight(long)}.
     *
     * @param weigher the weigher (must not be null)
     * @return this builder
     */
    @NotNull
    public CacheBuilder<K, V> weigher(@NotNull Weigher<? super K, ? super V> weigher) {
        this.weigher = weigher;
        return this;
    }

    /**
     * Registers a listener to be notified when entries are removed from the cache.
     *
     * @param evictionListener the listener (must not be null)
     * @return this builder
     */
    @NotNull
    public CacheBuilder<K, V> evictionListener(@NotNull EvictionListener<? super K, ? super V> evictionListener) {
        this.evictionListener = evictionListener;
        return this;
    }

    /**
     * Enables collection of cache statistics accessible via {@link Cache#stats()}.
     *
     * @return this builder
     */
    @NotNull
    public CacheBuilder<K, V> recordStats() {
        this.recordStats = true;
        return this;
    }

    /**
     * Sets how long entries may remain in the cache after their last access.
     *
     * @param duration the maximum idle duration (must not be null)
     * @return this builder
     */
    @NotNull
    public CacheBuilder<K, V> expireAfterAccess(@NotNull Duration duration) {
        this.expireAfterAccess = duration;
        return this;
    }

    /**
     * Sets how long entries may remain in the cache after they were written.
     *
     * @param duration the maximum age after write (must not be null)
     * @return this builder
     */
    @NotNull
    public CacheBuilder<K, V> expireAfterWrite(@NotNull Duration duration) {
        this.expireAfterWrite = duration;
        return this;
    }

    /**
     * Sets how soon a loading cache should automatically refresh entries after write.
     * Requires {@link #build(CacheLoader)}.
     *
     * @param duration the refresh interval (must not be null)
     * @return this builder
     */
    @NotNull
    public CacheBuilder<K, V> refreshAfterWrite(@NotNull Duration duration) {
        this.refreshAfterWrite = duration;
        return this;
    }

    /**
     * Sets the nanosecond ticker used to measure time for expiry and refresh.
     * Intended for testing with a controllable clock.
     *
     * @param ticker a supplier returning the current time in nanoseconds (must not be null)
     * @return this builder
     */
    @NotNull
    public CacheBuilder<K, V> ticker(@NotNull Supplier<Long> ticker) {
        this.ticker = ticker;
        return this;
    }

    /**
     * Sets the clock used to measure time for expiry and refresh.
     * Convenience overload for {@link #ticker(Supplier)}; converts milliseconds to nanoseconds internally.
     *
     * @param clock the clock to use (must not be null)
     * @return this builder
     */
    @NotNull
    public CacheBuilder<K, V> ticker(@NotNull Clock clock) {
        return ticker(() -> TimeUnit.MILLISECONDS.toNanos(clock.millis()));
    }

    /**
     * Builds and returns a cache with no auto-loading behaviour.
     *
     * @return a new {@link Cache}
     */
    @NotNull
    public Cache<K, V> build() {
        validateConfiguration(false);
        return buildCaffeine();
    }

    /**
     * Builds and returns a cache that automatically loads missing entries
     * using the given loader.
     *
     * @param loader the loader invoked when a key is absent (must not be null)
     * @return a new {@link LoadingCache}
     */
    @NotNull
    public LoadingCache<K, V> build(@NotNull CacheLoader<K, V> loader) {
        validateConfiguration(true);
        return buildCaffeine(loader);
    }

    private Cache<K, V> buildCaffeine() {
        return new CaffeineCacheAdapter<>(configureCaffeineBuilder().build());
    }

    private LoadingCache<K, V> buildCaffeine(CacheLoader<K, V> loader) {
        com.github.benmanes.caffeine.cache.LoadingCache<K, V> cache =
                configureCaffeineBuilder().build(loader::load);
        return new CaffeineLoadingCacheAdapter<>(cache);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Caffeine<K, V> configureCaffeineBuilder() {
        Caffeine caffeineBuilder = Caffeine.newBuilder()
                .executor(Runnable::run);
        if (initialCapacity >= 0) {
            caffeineBuilder = caffeineBuilder.initialCapacity(initialCapacity);
        }
        if (weigher != null) {
            // validateConfiguration() guarantees maximumWeight >= 0 when weigher is set
            Weigher<? super K, ? super V> w = weigher;
            caffeineBuilder = caffeineBuilder.weigher((k, v) -> w.weigh((K) k, (V) v));
            caffeineBuilder = caffeineBuilder.maximumWeight(maximumWeight);
        } else {
            // validateConfiguration() guarantees maximumSize >= 0 when weigher is absent
            caffeineBuilder = caffeineBuilder.maximumSize(maximumSize);
        }
        if (recordStats) {
            caffeineBuilder = caffeineBuilder.recordStats();
        }
        if (evictionListener != null) {
            EvictionListener<? super K, ? super V> listener = evictionListener;
            caffeineBuilder = caffeineBuilder.removalListener(
                    (k, v, cause) -> listener.onEviction((K) k, (V) v, CaffeineCacheAdapter.toOakCause(cause)));
        }
        if (expireAfterAccess != null) {
            caffeineBuilder = caffeineBuilder.expireAfterAccess(expireAfterAccess);
        }
        if (expireAfterWrite != null) {
            caffeineBuilder = caffeineBuilder.expireAfterWrite(expireAfterWrite);
        }
        if (refreshAfterWrite != null) {
            caffeineBuilder = caffeineBuilder.refreshAfterWrite(refreshAfterWrite);
        }
        if (ticker != null) {
            Supplier<Long> t = ticker;
            caffeineBuilder = caffeineBuilder.ticker(t::get);
        }
        return (Caffeine<K, V>) caffeineBuilder;
    }

    private void validateConfiguration(boolean loadingCache) {
        if (maximumWeight < 0 && maximumSize < 0) {
            throw new IllegalArgumentException("Either maximumSize or maximumWeight must be configured");
        }
        if (maximumWeight >= 0 && maximumSize >= 0) {
            throw new IllegalArgumentException("maximumSize and maximumWeight are mutually exclusive");
        }
        if (maximumWeight >= 0 && weigher == null) {
            throw new IllegalArgumentException("maximumWeight requires weigher");
        }
        if (weigher != null && maximumWeight < 0) {
            throw new IllegalArgumentException("weigher requires maximumWeight");
        }
        if (!loadingCache && refreshAfterWrite != null) {
            throw new IllegalArgumentException("refreshAfterWrite requires build(CacheLoader)");
        }
    }
}
