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

import java.util.Collection;

/**
 * Utility methods for {@link Iterable} conversions.
 */
public class IterableUtils {

    private IterableUtils() {
        // no instances for you
    }

    /**
     * Checks if an iterable is empty.
     *
     * @param iterable the iterable to check, must not be {@code null}
     * @param <T> the type of elements in the iterable
     * @return {@code true} if the iterable is empty, {@code false} otherwise
     * @throws NullPointerException if the iterable is {@code null}
     */
    public static <T> boolean isEmpty(@NotNull Iterable<T> iterable) {
        if (iterable instanceof Collection) {
            return ((Collection<?>) iterable).isEmpty();
        }
        return StreamUtils.toStream(iterable).findAny().isEmpty();
    }
}
