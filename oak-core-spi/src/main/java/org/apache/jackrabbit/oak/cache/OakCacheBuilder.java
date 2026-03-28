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

import java.time.Duration;
import java.util.Locale;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import org.apache.jackrabbit.guava.common.cache.CacheLoader;
import org.jetbrains.annotations.NotNull;

/**
 * Builder for {@link OakCache} and {@link OakLoadingCache} instances.
 *
 * <p>The backing implementation is chosen by a two-level resolution:</p>
 * <ol>
 *   <li><strong>Per-instance override</strong> — {@link #implementation(CacheImplementation)}
 *       pins this cache to one backend, regardless of any global setting.</li>
 *   <li><strong>Global default</strong> — the system property {@code oak.cache.type}
 *       ({@code lirs} or {@code caffeine}, case-insensitive); defaults to {@code lirs}.</li>
 * </ol>
 *
 * <p>Example:</p>
 * <pre>{@code
 * OakCache<String, NodeState> cache = OakCacheBuilder.<String, NodeState>newBuilder()
 *         .module("DocumentNodeStore")
 *         .maximumWeight(64 * 1024 * 1024)
 *         .weigher((k, v) -> v.estimateMemory())
 *         .recordStats()
 *         .build();
 * }</pre>
 *
 * @param <K> the type of cache keys
 * @param <V> the type of cache values
 */
public final class OakCacheBuilder<K, V> {

    // Common fields
    private String module;
    private CacheImplementation implementation;
    private long maximumWeight = -1;
    private long maximumSize = -1;
    private OakWeigher<K, V> weigher;
    private OakRemovalListener<K, V> removalListener;
    private boolean recordStats;
    // Caffeine-only time-based expiry
    private Duration expireAfterAccess;
    private Duration expireAfterWrite;
    private Duration refreshAfterWrite;
    // LIRS-specific tuning
    private int segmentCount = -1;
    private int stackMoveDistance = -1;
    private long averageWeight = -1;

    private OakCacheBuilder() {
    }

    /**
     * Creates a new builder with no pre-configured settings.
     *
     * @param <K> the type of cache keys
     * @param <V> the type of cache values
     * @return a new builder instance
     */
    @NotNull
    public static <K, V> OakCacheBuilder<K, V> newBuilder() {
        return new OakCacheBuilder<>();
    }

    /**
     * Sets a module label used in logging and diagnostics.
     *
     * @param module the module name (must not be null)
     * @return this builder
     */
    @NotNull
    public OakCacheBuilder<K, V> module(@NotNull String module) {
        if (module == null || module.isEmpty()) {
            throw new IllegalArgumentException("module must not be null or empty");
        }
        this.module = module;
        return this;
    }

    /**
     * Pins this cache to the given implementation, overriding the global
     * {@code oak.cache.type} system property.
     *
     * @param implementation the implementation to use (must not be null)
     * @return this builder
     */
    @NotNull
    public OakCacheBuilder<K, V> implementation(@NotNull CacheImplementation implementation) {
        if (implementation == null) {
            throw new IllegalArgumentException("implementation must not be null");
        }
        this.implementation = implementation;
        return this;
    }

    /**
     * Sets the maximum total weight of entries the cache may hold.
     * Must be used together with {@link #weigher(OakWeigher)} and may not be
     * combined with {@link #maximumSize(long)}.
     *
     * @param maximumWeight the maximum weight (must be non-negative)
     * @return this builder
     */
    @NotNull
    public OakCacheBuilder<K, V> maximumWeight(long maximumWeight) {
        if (maximumWeight < 0) {
            throw new IllegalArgumentException("maximumWeight must be non-negative, got: " + maximumWeight);
        }
        this.maximumWeight = maximumWeight;
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
    public OakCacheBuilder<K, V> maximumSize(long maximumSize) {
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
    public OakCacheBuilder<K, V> weigher(@NotNull OakWeigher<K, V> weigher) {
        if (weigher == null) {
            throw new IllegalArgumentException("weigher must not be null");
        }
        this.weigher = weigher;
        return this;
    }

    /**
     * Registers a listener to be notified when entries are removed from the cache.
     *
     * @param removalListener the listener (must not be null)
     * @return this builder
     */
    @NotNull
    public OakCacheBuilder<K, V> removalListener(@NotNull OakRemovalListener<K, V> removalListener) {
        if (removalListener == null) {
            throw new IllegalArgumentException("removalListener must not be null");
        }
        this.removalListener = removalListener;
        return this;
    }

    /**
     * Enables collection of cache statistics accessible via {@link OakCache#stats()}.
     *
     * @return this builder
     */
    @NotNull
    public OakCacheBuilder<K, V> recordStats() {
        this.recordStats = true;
        return this;
    }

    /**
     * Sets how long entries may remain in the cache after their last access.
     * Applies to the Caffeine backend only; silently ignored for LIRS.
     *
     * @param duration the maximum idle duration (must not be null)
     * @return this builder
     */
    @NotNull
    public OakCacheBuilder<K, V> expireAfterAccess(@NotNull Duration duration) {
        if (duration == null) {
            throw new IllegalArgumentException("duration must not be null");
        }
        this.expireAfterAccess = duration;
        return this;
    }

    /**
     * Sets how long entries may remain in the cache after they were written.
     * Applies to the Caffeine backend only; silently ignored for LIRS.
     *
     * @param duration the maximum age after write (must not be null)
     * @return this builder
     */
    @NotNull
    public OakCacheBuilder<K, V> expireAfterWrite(@NotNull Duration duration) {
        if (duration == null) {
            throw new IllegalArgumentException("duration must not be null");
        }
        this.expireAfterWrite = duration;
        return this;
    }

    /**
     * Sets how soon a loading cache should automatically refresh entries after write.
     * Applies to the Caffeine backend only; requires {@link #build(OakCacheLoader)}
     * and is ignored for LIRS.
     *
     * @param duration the refresh interval (must not be null)
     * @return this builder
     */
    @NotNull
    public OakCacheBuilder<K, V> refreshAfterWrite(@NotNull Duration duration) {
        if (duration == null) {
            throw new IllegalArgumentException("duration must not be null");
        }
        this.refreshAfterWrite = duration;
        return this;
    }

    /**
     * Sets the number of LIRS segments. Applies to the LIRS backend only.
     *
     * @param segmentCount the number of segments (must be positive)
     * @return this builder
     */
    @NotNull
    public OakCacheBuilder<K, V> segmentCount(int segmentCount) {
        if (segmentCount <= 0) {
            throw new IllegalArgumentException("segmentCount must be positive, got: " + segmentCount);
        }
        this.segmentCount = segmentCount;
        return this;
    }

    /**
     * Sets the LIRS stack move distance. Applies to the LIRS backend only.
     *
     * @param stackMoveDistance the stack move distance (must be non-negative)
     * @return this builder
     */
    @NotNull
    public OakCacheBuilder<K, V> stackMoveDistance(int stackMoveDistance) {
        if (stackMoveDistance < 0) {
            throw new IllegalArgumentException("stackMoveDistance must be non-negative, got: " + stackMoveDistance);
        }
        this.stackMoveDistance = stackMoveDistance;
        return this;
    }

    /**
     * Sets the average expected weight per entry for LIRS sizing.
     * Applies to the LIRS backend only and requires {@link #maximumWeight(long)}.
     *
     * @param averageWeight the average entry weight (must be positive and
     *                      less than or equal to {@link Integer#MAX_VALUE})
     * @return this builder
     */
    @NotNull
    public OakCacheBuilder<K, V> averageWeight(long averageWeight) {
        if (averageWeight <= 0) {
            throw new IllegalArgumentException("averageWeight must be positive, got: " + averageWeight);
        }
        this.averageWeight = averageWeight;
        return this;
    }

    /**
     * Builds and returns a cache with no auto-loading behaviour.
     *
     * @return a new {@link OakCache}
     */
    @NotNull
    public OakCache<K, V> build() {
        validateConfiguration(false);
        return switch (resolveImplementation()) {
            case LIRS -> buildLirs();
            case CAFFEINE -> buildCaffeine();
        };
    }

    /**
     * Builds and returns a cache that automatically loads missing entries
     * using the given loader.
     *
     * @param loader the loader invoked when a key is absent (must not be null)
     * @return a new {@link OakLoadingCache}
     */
    @NotNull
    public OakLoadingCache<K, V> build(@NotNull OakCacheLoader<K, V> loader) {
        if (loader == null) {
            throw new IllegalArgumentException("loader must not be null");
        }
        validateConfiguration(true);
        return switch (resolveImplementation()) {
            case LIRS -> buildLirs(loader);
            case CAFFEINE -> buildCaffeine(loader);
        };
    }

    // ---- private helpers ----

    private CacheImplementation resolveImplementation() {
        if (implementation != null) {
            return implementation;
        }
        String prop = System.getProperty("oak.cache.type", "lirs");
        try {
            return CacheImplementation.valueOf(prop.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown oak.cache.type value '" + prop + "'; expected 'lirs' or 'caffeine'");
        }
    }

    private OakCache<K, V> buildLirs() {
        return new LirsCacheAdapter<>(configureLirsBuilder().build());
    }

    private OakLoadingCache<K, V> buildLirs(OakCacheLoader<K, V> loader) {
        CacheLIRS<K, V> cache = configureLirsBuilder().build(new CacheLoader<K, V>() {
            @Override
            public V load(K key) throws Exception {
                return loader.load(key);
            }
        });
        return new LirsLoadingCacheAdapter<>(cache);
    }

    private CacheLIRS.Builder<K, V> configureLirsBuilder() {
        CacheLIRS.Builder<K, V> b = CacheLIRS.newBuilder();
        if (module != null) {
            b = b.module(module);
        }
        if (weigher != null) {
            OakWeigher<K, V> w = weigher;
            b = b.weigher((k, v) -> w.weigh(k, v));
            if (maximumWeight >= 0) {
                b = b.maximumWeight(maximumWeight);
            }
        } else if (maximumSize >= 0) {
            b = b.maximumSize(maximumSize);
        } else if (maximumWeight >= 0) {
            b = b.maximumWeight(maximumWeight);
        }
        if (averageWeight > 0) {
            b = b.averageWeight((int) averageWeight);
        }
        if (segmentCount > 0) {
            b = b.segmentCount(segmentCount);
        }
        if (stackMoveDistance >= 0) {
            b = b.stackMoveDistance(stackMoveDistance);
        }
        if (recordStats) {
            b = b.recordStats();
        }
        if (removalListener != null) {
            OakRemovalListener<K, V> listener = removalListener;
            b = b.evictionCallback((k, v, cause) -> listener.onRemoval(k, v, LirsCacheAdapter.toOakCause(cause)));
        }
        return b;
    }

    @SuppressWarnings("unchecked")
    private OakCache<K, V> buildCaffeine() {
        return new CaffeineCacheAdapter<>((Cache<K, V>) configureCaffeineBuilder().build());
    }

    @SuppressWarnings("unchecked")
    private OakLoadingCache<K, V> buildCaffeine(OakCacheLoader<K, V> loader) {
        LoadingCache<K, V> cache = (LoadingCache<K, V>) configureCaffeineBuilder().build(key -> {
            try {
                return loader.load(key);
            } catch (Exception e) {
                throw new CacheComputationException(e);
            }
        });
        return new CaffeineLoadingCacheAdapter<>(cache);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Caffeine<K, V> configureCaffeineBuilder() {
        Caffeine caffeineBuilder = Caffeine.newBuilder();
        if (weigher != null) {
            OakWeigher<K, V> w = weigher;
            caffeineBuilder = caffeineBuilder.weigher((k, v) -> w.weigh((K) k, (V) v));
            if (maximumWeight >= 0) {
                caffeineBuilder = caffeineBuilder.maximumWeight(maximumWeight);
            }
        } else if (maximumSize >= 0) {
            caffeineBuilder = caffeineBuilder.maximumSize(maximumSize);
        } else if (maximumWeight >= 0) {
            caffeineBuilder = caffeineBuilder.maximumWeight(maximumWeight);
        }
        if (recordStats) {
            caffeineBuilder = caffeineBuilder.recordStats();
        }
        if (removalListener != null) {
            // Run maintenance (including removal callbacks) on the calling thread
            // so the listener is invoked synchronously, matching the OakCache contract.
            caffeineBuilder = caffeineBuilder.executor(Runnable::run);
            OakRemovalListener<K, V> listener = removalListener;
            caffeineBuilder = caffeineBuilder.removalListener(
                    (k, v, cause) -> listener.onRemoval((K) k, (V) v, CaffeineCacheAdapter.toOakCause(cause)));
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
            throw new IllegalArgumentException("refreshAfterWrite requires build(OakCacheLoader)");
        }
        if (averageWeight > 0 && maximumWeight < 0) {
            throw new IllegalArgumentException("averageWeight requires maximumWeight");
        }
        if (averageWeight > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "averageWeight must be less than or equal to Integer.MAX_VALUE, got: " + averageWeight);
        }
    }
}
