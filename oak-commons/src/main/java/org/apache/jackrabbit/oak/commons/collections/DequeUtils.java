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
package org.apache.jackrabbit.oak.commons.collections;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Objects;

/**
 * Utility methods for {@link java.util.Deque} conversions.
 */
public class DequeUtils {

    private DequeUtils() {
        // no instances for you
    }

    /**
     * Convert an iterable to a {@link java.util.ArrayDeque}.
     * The returning array deque is mutable and supports all optional operations.
     *
     * @param iterable the iterable to convert
     * @param <T>      the type of the elements
     * @return the arrayDeque
     */
    public static <T> ArrayDeque<T> toArrayDeque(@NotNull Iterable<? extends T> iterable) {
        Objects.requireNonNull(iterable);
        ArrayDeque<T> arrayDeque = new ArrayDeque<>();
        iterable.forEach(arrayDeque::add);
        return arrayDeque;
    }
}
