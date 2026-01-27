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
}
