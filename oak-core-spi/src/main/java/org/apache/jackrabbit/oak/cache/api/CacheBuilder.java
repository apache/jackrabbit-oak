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
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

    /**
     * Feature toggle name for {@link #FT_OAK_12290_ASYNC_CACHE_MAINTENANCE_ENABLED}.
     */
    public static final String FT_OAK_12290 = "FT_OAK-12290";

    /**
     * Whether Caffeine runs cache maintenance (eviction, removal notification, buffer drains)
     * on Oak's maintenance executor instead of the calling thread. Defaults to {@code true} as a
     * <strong>bug-fix</strong> toggle: inline maintenance made request, indexer and writer
     * threads hold Caffeine's eviction lock for the duration of the maintenance work, which
     * caused lock contention (OAK-12290) and, when a lock holder died, a wedged JVM
     * (SKYOPS-149400).
     * <p>
     * The toggle is registered on the OSGi Whiteboard under {@link #FT_OAK_12290}. Its value is
     * read when a cache is built, so flipping it only affects caches built afterwards - Oak's
     * long-lived caches are built during startup and keep the setting they were built with.
     * <p>
     * Caches configured with {@link #refreshAfterWrite(Duration)} ignore the toggle and always use
     * the maintenance executor, because Caffeine shares one executor between maintenance and
     * refresh and a synchronous refresh would run the loader (potentially a remote call) on the
     * calling thread.
     */
    public static final AtomicBoolean FT_OAK_12290_ASYNC_CACHE_MAINTENANCE_ENABLED = new AtomicBoolean(true);

    private static final String MAINTENANCE_THREAD_PREFIX = "oak-cache-maintenance-";

    /**
     * Number of maintenance threads shared by all Oak caches, between 2 and 4.
     * <p>
     * Caffeine keeps at most one maintenance task per cache in flight, so the concurrency needed is
     * bounded by the number of caches draining at the same instant - roughly the dozen
     * {@code CacheBuilder} consumers - not by request throughput. Hence a small count that does not
     * scale with core count, so the pool never competes with request threads on a large machine.
     * The floor of 2 keeps a blocking {@link #refreshAfterWrite(Duration)} reload from starving
     * plain eviction work.
     */
    private static final int MAINTENANCE_THREADS =
            Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors() / 4));

    /**
     * Bound on queued maintenance tasks: deep enough to absorb a burst, shallow enough that a
     * wedged pool falls back to {@link ThreadPoolExecutor.CallerRunsPolicy} instead of queueing
     * without bound.
     */
    private static final int MAINTENANCE_QUEUE_CAPACITY = 1024;

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
     * <p>As with {@link #maximumSize(long)}, the bound is enforced asynchronously - a read that
     * immediately follows the write which exceeded the weight may still see the entry. Call
     * {@link Cache#cleanUp()} to force pending maintenance.</p>
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
     * <p>The bound is enforced by maintenance running on the cache's executor, so an entry can
     * still be served by a read that immediately follows the write which exceeded the bound. This
     * matters mostly for a bound of {@code 0}: call {@link Cache#cleanUp()} first if a test or
     * caller needs the entry to be gone. Steady-state memory use stays bounded.</p>
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
        Caffeine caffeineBuilder = Caffeine.newBuilder();
        // Caffeine uses one executor for both maintenance and refresh work. Keeping refresh
        // asynchronous outranks the escape hatch: a synchronous reload would run the loader's
        // remote call on the calling thread.
        boolean inline = !FT_OAK_12290_ASYNC_CACHE_MAINTENANCE_ENABLED.get() && refreshAfterWrite == null;
        caffeineBuilder = caffeineBuilder.executor(inline ? Runnable::run : maintenanceExecutor());
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
            // Deliberately removalListener and not evictionListener: Caffeine invokes the latter
            // inside the map's atomic removal, holding the bin lock for the key. Oak's listeners do
            // real work there - NodeCache.evicted() enqueues a persistent-cache write under its own
            // monitor - so that would reintroduce exactly the kind of lock coupling OAK-12290 is
            // about. removalListener runs on the maintenance executor instead; listeners must
            // therefore tolerate lagging behind the write that caused the removal.
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

    /**
     * The executor Caffeine runs cache maintenance on.
     * <p>
     * Oak owns this pool rather than letting Caffeine fall back to
     * {@link java.util.concurrent.ForkJoinPool#commonPool()}, for three reasons:
     * <ul>
     *   <li><em>Liveness.</em> The common pool is configurable to zero workers
     *       ({@code -Djava.util.concurrent.ForkJoinPool.common.parallelism=0}), in which case
     *       {@code execute(Runnable)} tasks are queued and never run - eviction would stop and
     *       removal listeners would never fire, silently. The bounded queue plus
     *       {@link ThreadPoolExecutor.CallerRunsPolicy} here guarantees maintenance always runs
     *       eventually, degrading to the pre-OAK-12290 inline behaviour rather than stalling.</li>
     *   <li><em>Isolation.</em> The common pool is shared with every {@code parallelStream()} in
     *       the JVM, including the hosting application's. A saturated common pool would delay
     *       segment-cache weight accounting and, once Caffeine's write buffer fills, push
     *       maintenance back onto request threads - reintroducing the very lock contention
     *       OAK-12290 is about.</li>
     *   <li><em>Diagnosability.</em> Named daemon threads make cache maintenance identifiable in a
     *       thread dump, which is how OAK-12290 and SKYOPS-149400 were diagnosed in the first
     *       place. This also matches Oak's existing convention of never using the common pool
     *       (see {@code ForkJoinUtils#submitInCustomPool}).</li>
     * </ul>
     */
    private static Executor maintenanceExecutor() {
        return MaintenanceExecutorHolder.EXECUTOR;
    }

    /**
     * Lazy holder so the pool is only created once a cache is actually built.
     */
    private static final class MaintenanceExecutorHolder {

        private static final Executor EXECUTOR = newMaintenanceExecutor();

        private static Executor newMaintenanceExecutor() {
            AtomicInteger threadCounter = new AtomicInteger();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    MAINTENANCE_THREADS, MAINTENANCE_THREADS,
                    60, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(MAINTENANCE_QUEUE_CAPACITY),
                    runnable -> {
                        Thread thread = new Thread(runnable,
                                MAINTENANCE_THREAD_PREFIX + threadCounter.incrementAndGet());
                        // Daemon: the pool is process-wide and never shut down, and no maintenance
                        // task is required to complete for a clean exit.
                        thread.setDaemon(true);
                        return thread;
                    },
                    new ThreadPoolExecutor.CallerRunsPolicy());
            executor.allowCoreThreadTimeOut(true);
            return executor;
        }
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
