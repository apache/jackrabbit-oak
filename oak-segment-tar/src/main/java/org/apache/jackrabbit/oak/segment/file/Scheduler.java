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

import static java.lang.Thread.currentThread;
import static java.util.concurrent.Executors.defaultThreadFactory;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple scheduler for executing and scheduling tasks in the background.
 * This implementation delegates all background execution to an instance
 * of a {@link ScheduledExecutorService} with core pool size 1. The behaviour
 * of this underlying scheduler service determines the semantics of the methods
 * in this class. Namely: Execution of background tasks never overlaps and is
 * FIFO for tasks scheduled for the same time.
 * In addition all tasks scheduled through methods of this class are automatically
 * wrapped into {@link SafeRunnable} instances. The background thread executing
 * submitted tasks is a deamon thread.
 */
public class Scheduler implements Closeable {
    private static int schedulerNumber = 0;
    private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);

    @Nonnull
    private final AtomicLong executionCounter = new AtomicLong();

    @Nonnull
    private final String name;

    @Nonnull
    private final ScheduledExecutorService executor;

    /**
     * Create a new instance with the given {@code name}. The name is used to
     * derive the default name of the background thread from..
     * @param name
     */
    public Scheduler(@Nullable String name) {
        if (name == null) {
            synchronized (Scheduler.class) {
                this.name = "scheduler-" + schedulerNumber;
                schedulerNumber++;
            }
        } else {
            this.name = name;
        }
        this.executor = newScheduledThreadPool(1, new SchedulerThreadFactory(this.name));
    }

    /**
     * Immediately execute {@code task}. The background thread's name is
     * set to {@code name} during execution of {@code task}.
     * @param name
     * @param task
     * @see ScheduledExecutorService#execute(Runnable)
     */
    public void execute(@Nonnull String name, @Nonnull Runnable task) {
        executor.execute(new SafeRunnable(name, task));
    }

    /**
     * Run {@code task} once after some delay. The background thread's name is
     * set to {@code name} during execution of {@code task}.
     * @param name
     * @param delay
     * @param unit
     * @param task
     * @see ScheduledExecutorService#schedule(Runnable, long, TimeUnit)
     */
    public void scheduleOnce(
            @Nonnull String name,
            long delay,
            @Nonnull TimeUnit unit,
            @Nonnull Runnable task) {
        executor.schedule(new SafeRunnable(name, task), delay, unit);
    }

    /**
     * Run {@code task} regularly at a given interval. The background thread's name is
     * set to {@code name} during execution of {@code task}.
     * @param name
     * @param period
     * @param unit
     * @param task
     * @see ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)
     */
    public void scheduleAtFixedRate(
            @Nonnull String name,
            long period,
            @Nonnull TimeUnit unit,
            @Nonnull Runnable task) {
        executor.scheduleAtFixedRate(new SafeRunnable(name, task), period, period, unit);
    }

    /**
     * Close this scheduler.
     * @see ScheduledExecutorService#shutdown()
     */
    @Override
    public void close() {
        try {
            executor.shutdown();
            if (executor.awaitTermination(60, SECONDS)) {
                LOG.debug("The scheduler {} was successfully shut down", name);
            } else {
                LOG.warn("The scheduler {} takes too long to shut down", name);
            }
        } catch (InterruptedException e) {
            LOG.warn("Interrupt while shutting down he scheduler {}", name, e);
            currentThread().interrupt();
        }
    }

    private static class SchedulerThreadFactory implements ThreadFactory {
        private final ThreadFactory threadFactory = defaultThreadFactory();

        @Nonnull
        private final String name;

        public SchedulerThreadFactory(@Nonnull String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(@Nonnull Runnable runnable) {
            Thread thread = threadFactory.newThread(runnable);
            thread.setName(name);
            thread.setDaemon(true);
            return thread;
        }
    }

}
