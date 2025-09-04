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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Utility to convert {@link org.apache.jackrabbit.guava.common.util.concurrent.ListenableFuture} to {@link java.util.concurrent.CompletableFuture}
 */
// TODO: remove this class once we remove all Guava Concurent Packages
public class FutureConverter {
    private FutureConverter() {
        // no instances for you
    }

    private static final Executor DIRECT_EXECUTOR = Runnable::run;


    public static <T> List<CompletableFuture<T>> toCompletableFuture(final List<? extends ListenableFuture<T>> listenableFutures) {
        return listenableFutures.stream()
                .map(FutureConverter::toCompletableFuture)
                .collect(Collectors.toList());
    }

    /**
     * Converts a {@link org.apache.jackrabbit.guava.common.util.concurrent.ListenableFuture}
     * to a {@link java.util.concurrent.CompletableFuture}.
     * <p>
     * The returned CompletableFuture will be completed when the ListenableFuture completes,
     * either with its result or with an exception if the ListenableFuture fails.
     *
     * @param listenableFuture the ListenableFuture to convert
     * @param <T> the result type
     * @return a CompletableFuture representing the same computation
     */
    public static <T> CompletableFuture<T> toCompletableFuture(final ListenableFuture<T> listenableFuture) {
        CompletableFuture<T> completable = new CompletableFuture<>();
        listenableFuture.addListener(() -> {
            try {
                completable.complete(listenableFuture.get());
            } catch (InterruptedException ex) {
                // fix for sonar : https://sonarcloud.io/organizations/apache/rules?open=java%3AS2142&rule_key=java%3AS2142
                Thread.currentThread().interrupt();
                completable.completeExceptionally(ex);
            } catch (Exception ex) {
                completable.completeExceptionally(ex.getCause() != null ? ex.getCause() : ex);
            }
        }, DIRECT_EXECUTOR);
        return completable;
    }
}
