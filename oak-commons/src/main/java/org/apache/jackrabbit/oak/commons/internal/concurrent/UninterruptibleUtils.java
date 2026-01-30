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
package org.apache.jackrabbit.oak.commons.internal.concurrent;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods for waiting on synchronization primitives without
 * propagating {@link InterruptedException} to callers.
 */
public class UninterruptibleUtils {

    private UninterruptibleUtils() {
        // no instance for you
    }

    /**
     * Waits uninterruptibly until the given {@link CountDownLatch} reaches zero.
     * <p>
     * This method repeatedly invokes {@link CountDownLatch#await()} and
     * ignores any {@link InterruptedException} that occurs while waiting,
     * but remembers that an interruption happened. After the latch has
     * reached zero (or the method otherwise returns), this method restores
     * the thread's interrupted status if any interruptions were detected
     * during the wait.
     *
     * @param latch the latch to wait on; must not be {@code null}
     * @throws NullPointerException if {@code latch} is {@code null}
     */
    public static void awaitUninterruptibly(CountDownLatch latch) {

        Objects.requireNonNull(latch, "latch is null");

        boolean interrupted = false;
        try {
            for (;;) {
                try {
                    latch.await();
                    return;           // completed normally
                } catch (InterruptedException e) {
                    interrupted = true; // remember and retry
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt(); // restore flag
            }
        }
    }

    /**
     * Causes the current thread to sleep for the specified duration,
     * ignoring {@link InterruptedException} until the full sleep time
     * has elapsed, then restores the interrupted status before returning.
     * <p>
     * This behaves like Guava's
     * {@code Uninterruptibles.sleepUninterruptibly(long, TimeUnit)}:
     * it repeatedly invokes {@link TimeUnit#sleep(long)} until the
     * requested time has passed, catching and recording interruptions
     * and recomputing the remaining time from a fixed deadline.
     *
     * @param sleep the time to sleep; must be non-negative
     * @param unit     the time unit of the {@code sleep} argument; must not be {@code null}
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code sleep} is negative
     */
    public static void sleepUninterruptibly(final long sleep, final TimeUnit unit) {

        Objects.requireNonNull(unit, "timeunit is null");

        if (sleep < 0L) {
            throw new IllegalArgumentException("sleep must be >= 0");
        }

        boolean interrupted = false;
        try {
            long remainingNanos = unit.toNanos(sleep);
            long end = System.nanoTime() + remainingNanos;
            for (;;) {
                try {
                    // TimeUnit.sleep() treats negative timeouts just like zero.
                    TimeUnit.NANOSECONDS.sleep(remainingNanos);
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                    remainingNanos = end - System.nanoTime();
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Invokes {@link TimeUnit#timedJoin(Thread, long)} uninterruptibly.
     * <p>
     * This method repeatedly calls {@link TimeUnit#timedJoin(Thread, long)} until the
     * specified timeout has elapsed or the target thread terminates, ignoring
     * {@link InterruptedException} but remembering that an interruption
     * occurred. When the method finally returns, it restores the current
     * thread's interrupted status if any interruptions were detected.
     *
     * @param toJoin the thread to wait for; must not be {@code null}
     * @throws NullPointerException     if {@code toJoin} or {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code timeout} is negative
     */
    public static void joinUninterruptibly(final Thread toJoin) {

        Objects.requireNonNull(toJoin, "thread to join is null");

        boolean interrupted = false;

        try {
            for(;;) {
                try {
                    toJoin.join();
                    return;
                } catch (InterruptedException var6) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
