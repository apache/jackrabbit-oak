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
package org.apache.jackrabbit.oak.cache.impl;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetbrains.annotations.NotNull;

/**
 * The process-wide pool Caffeine-backed Oak caches run their maintenance (eviction, removal
 * notification, buffer drains) on.
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
public final class CacheMaintenanceExecutor {

    private static final String THREAD_PREFIX = "oak-cache-maintenance-";

    /**
     * Number of maintenance threads shared by all Oak caches, between 2 and 4.
     * <p>
     * Caffeine keeps at most one maintenance task per cache in flight, so the concurrency needed is
     * bounded by the number of caches draining at the same instant - roughly the dozen
     * {@code CacheBuilder} consumers - not by request throughput. Hence a small count that does not
     * scale with core count, so the pool never competes with request threads on a large machine.
     * The floor of 2 keeps a blocking refresh reload from starving plain eviction work.
     */
    private static final int THREADS = Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors() / 4));

    /**
     * Bound on queued maintenance tasks: deep enough to absorb a burst, shallow enough that a
     * wedged pool falls back to {@link ThreadPoolExecutor.CallerRunsPolicy} instead of queueing
     * without bound.
     */
    private static final int QUEUE_CAPACITY = 1024;

    private CacheMaintenanceExecutor() {
    }

    /**
     * The shared maintenance pool. Created on first call, so no threads exist in a JVM that never
     * builds a cache.
     *
     * @return the process-wide maintenance executor
     */
    @NotNull
    public static Executor get() {
        return Holder.EXECUTOR;
    }

    /**
     * Lazy holder so the pool is only created once a cache is actually built.
     */
    private static final class Holder {

        private static final Executor EXECUTOR = newExecutor();

        private static Executor newExecutor() {
            AtomicInteger threadCounter = new AtomicInteger();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    THREADS, THREADS,
                    60, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(QUEUE_CAPACITY),
                    runnable -> {
                        Thread thread = new Thread(runnable, THREAD_PREFIX + threadCounter.incrementAndGet());
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
}
