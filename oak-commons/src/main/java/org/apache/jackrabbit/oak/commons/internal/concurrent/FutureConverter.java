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

import org.apache.jackrabbit.guava.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utility to convert between {@link org.apache.jackrabbit.guava.common.util.concurrent.ListenableFuture}
 * and {@link java.util.concurrent.CompletableFuture}.
 */
// TODO: remove this class once we remove all Guava Concurent Packages
public class FutureConverter {
    private FutureConverter() {
        // no instances for you
    }

    /**
     * Converts a Java {@link CompletableFuture} to a Guava {@link ListenableFuture}.
     * <p>
     * The returned ListenableFuture will complete when the given CompletableFuture completes,
     * with the same result or exception. This is a one-way conversion; cancelling the ListenableFuture
     * will not cancel the original CompletableFuture.
     *
     * @param completableFuture the Java CompletableFuture to convert
     * @param <T> the type of the future result
     * @return a Guava ListenableFuture that completes when the CompletableFuture completes
     */
    public static <T> ListenableFuture<T> toListenableFuture(CompletableFuture<T> completableFuture) {
        return new ListenableFuture<T>() {
            @Override
            public void addListener(@NotNull Runnable listener, @NotNull Executor executor) {
                completableFuture.whenComplete((result, ex) -> listener.run());
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return completableFuture.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return completableFuture.isCancelled();
            }

            @Override
            public boolean isDone() {
                return completableFuture.isDone();
            }

            @Override
            public T get() throws InterruptedException, ExecutionException {
                try {
                    return completableFuture.get();
                } catch (InterruptedException e) {
                    // fix for sonar : https://sonarcloud.io/organizations/apache/rules?open=java%3AS2142&rule_key=java%3AS2142
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }

            @Override
            public T get(long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                try {
                    return completableFuture.get(timeout, unit);
                } catch (InterruptedException e) {
                    // fix for sonar : https://sonarcloud.io/organizations/apache/rules?open=java%3AS2142&rule_key=java%3AS2142
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        };
    }

}
