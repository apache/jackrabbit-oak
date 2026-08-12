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
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The process-wide pool Caffeine-backed Oak caches run their maintenance (eviction, removal
 * notification, buffer drains) on, instead of {@link java.util.concurrent.ForkJoinPool#commonPool()}.
 */
public final class CacheMaintenanceExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(CacheMaintenanceExecutor.class);

    private static final Thread.UncaughtExceptionHandler UNCAUGHT_EXCEPTION_HANDLER = (t, e) ->
            LOG.warn("Uncaught exception in thread {}", t.getName(), e);

    private static final String THREAD_PREFIX = "oak-cache-maintenance-";

    /**
     * The pool is process-wide, so its workers must not inherit an OSGi request thread's context
     * class loader and keep a refreshed consumer bundle alive.
     */
    private static final ClassLoader THREAD_CONTEXT_CLASS_LOADER =
            CacheMaintenanceExecutor.class.getClassLoader();

    /**
     * Number of maintenance threads shared by all Oak caches, between 2 and 8. Caffeine keeps at
     * most one maintenance task per cache in flight, so this doesn't need to scale with core count.
     */
    private static final int THREADS = Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors() - 1));

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
                    newThreadFactory(threadCounter),
                    new LoggingCallerRunsPolicy());
            executor.allowCoreThreadTimeOut(true);
            return executor;
        }
    }

    static ThreadFactory newThreadFactory(AtomicInteger threadCounter) {
        return runnable -> {
            Thread thread = new Thread(runnable, THREAD_PREFIX + threadCounter.incrementAndGet());
            // Do not keep a consumer bundle's class loader alive through this process-wide worker.
            thread.setContextClassLoader(THREAD_CONTEXT_CLASS_LOADER);
            // Daemon: the pool is process-wide and never shut down, and no maintenance task is
            // required to complete for a clean exit.
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler(UNCAUGHT_EXCEPTION_HANDLER);
            return thread;
        };
    }

    /**
     * {@link ThreadPoolExecutor.CallerRunsPolicy} that also logs, at most once a minute, that the
     * pool is saturated.
     */
    private static final class LoggingCallerRunsPolicy implements RejectedExecutionHandler {

        private static final long LOG_INTERVAL_NANOS = TimeUnit.MINUTES.toNanos(1);

        private final AtomicLong nextLogNanos = new AtomicLong();

        @Override
        public void rejectedExecution(@NotNull Runnable task, @NotNull ThreadPoolExecutor executor) {
            long now = System.nanoTime();
            long next = nextLogNanos.get();
            if (now >= next && nextLogNanos.compareAndSet(next, now + LOG_INTERVAL_NANOS)) {
                LOG.warn("Cache maintenance pool exhausted ({} threads, {}-deep queue full); running "
                        + "maintenance inline on the calling thread instead. Expected under a burst; "
                        + "sustained occurrence means the pool is undersized for the load.",
                        THREADS, QUEUE_CAPACITY);
            }
            task.run();
        }
    }
}
