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
     * Waits uninterruptibly until either the given {@link CountDownLatch} reaches
     * zero or the specified waiting time elapses.
     * <p>
     * This method behaves like {@link CountDownLatch#await(long, TimeUnit)},
     * except that it does not throw {@link InterruptedException}. Instead, it
     * continues waiting when interruptions occur, tracking the remaining time
     * based on a fixed deadline, and restores the thread's interrupted status
     * before returning if any interruptions were detected.
     *
     * @param latch   the latch to wait on; must not be {@code null}
     * @param timeout the maximum time to wait; must be non-negative
     * @param unit    the time unit of the {@code timeout} argument; must not be {@code null}
     * @return {@code true} if the latch reached zero before the timeout expired;
     *         {@code false} if the waiting time elapsed before the latch reached zero
     * @throws NullPointerException     if {@code latch} or {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code timeout} is negative
     */
    public static boolean awaitUninterruptibly(final CountDownLatch latch, final long timeout, final TimeUnit unit) {

        Objects.requireNonNull(latch, "latch is null");
        Objects.requireNonNull(unit, "unit is null");

        if (timeout < 0L) {
            throw new IllegalArgumentException("timeout must be >= 0");
        }

        boolean interrupted = false;
        try {
            long remainingNanos = unit.toNanos(timeout);
            long end = System.nanoTime() + remainingNanos;
            for (;;) {
                try {
                    return latch.await(remainingNanos, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                    remainingNanos = end - System.nanoTime();
                    if (remainingNanos <= 0L) {
                        // Time is up: whether we return true or false depends on latch state.
                        return latch.getCount() == 0L;
                    }
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
