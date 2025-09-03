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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Utils methods for {@link CompletableFuture}
 */
public class FutureUtils {

    private FutureUtils() {
        // no instance for you
    }

    /**
     * Returns a {@link CompletableFuture} that completes when all of the given futures complete.
     * <p>
     * The resulting list contains the results of each input future, or {@code null} for any future that completed exceptionally.
     * This mimics Guava's {@code Futures.successfulAsList} behavior.
     *
     * @param futures the list of CompletableFutures to aggregate
     * @param <T> the result type
     * @return a CompletableFuture containing a list of results (or null for failed futures)
     */
    public static <T> CompletableFuture<List<T>> successfulAsList(final List<CompletableFuture<T>> futures) {

        // need to handle each future case individually since if failed inside CompletableFuture.allOf(),
        // it would throw an exception rather than returning null
        final List<CompletableFuture<T>> handled = futures.stream()
                .map(f -> f.handle((value, ex) -> ex == null ? value : null))
                .collect(Collectors.toList());

        return CompletableFuture.allOf(handled.toArray(new CompletableFuture[0]))
                .thenApply(v ->
                        handled.stream()
                                .map(CompletableFuture::join)
                                .collect(Collectors.toList())
                );
    }

}
