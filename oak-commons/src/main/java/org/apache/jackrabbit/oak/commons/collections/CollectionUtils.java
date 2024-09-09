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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.jetbrains.annotations.NotNull;

/**
 * Utility methods for collections conversions.
 */
public class CollectionUtils {

    private CollectionUtils() {
        // no instances for you
    }

    /**
     * Convert an iterable to a list. The returning list is mutable and supports all optional operations.
     * @param iterable the iterable to convert
     * @return the list
     * @param <T> the type of the elements
     */
    @NotNull
    public static <T> List<T> toList(@NotNull final Iterable<T> iterable) {
        Objects.requireNonNull(iterable);
        List<T> result = new ArrayList<>();
        iterable.forEach(result::add);
        return result;
    }

    /**
     * Convert an iterator to a list. The returning list is mutable and supports all optional operations.
     * @param iterator the iterator to convert
     * @return the list
     * @param <T> the type of the elements
     */
    @NotNull
    public static <T> List<T> toList(final Iterator<T> iterator) {
        Objects.requireNonNull(iterator);
        List<T> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        return result;
    }

    /**
     * Convert an iterable to a set. The returning set is mutable and supports all optional operations.
     * @param iterable the iterable to convert
     * @return the set
     * @param <T> the type of the elements
     */
    @NotNull
    public static <T> Set<T> toSet(@NotNull  final Iterable<T> iterable) {
        Objects.requireNonNull(iterable);
        final Set<T> result = new HashSet<>();
        iterable.forEach(result::add);
        return result;
    }

    /**
     * Convert an iterator to a set. The returning set is mutable and supports all optional operations.
     * @param iterator the iterator to convert
     * @return the set
     * @param <T> the type of the elements
     */
    @NotNull
    public static <T> Set<T> toSet(@NotNull final Iterator<T> iterator) {
        Objects.requireNonNull(iterator);
        final Set<T> result = new HashSet<>();
        iterator.forEachRemaining(result::add);
        return result;
    }

    /**
     * Convert a vararg list of items to a set.  The returning set is mutable and supports all optional operations.
     * @param elements elements to convert
     * @return the set
     * @param <T> the type of the elements
     */
    @SafeVarargs
    @NotNull
    public static <T> Set<T> toSet(@NotNull final T... elements) {
        Objects.requireNonNull(elements);
        // make sure the set does not need to be resized given the initial content
        float loadFactor = (float) 0.75; // HashSet default
        int initialCapacity = 1 + (int) (elements.length / loadFactor);
        final Set<T> result = new HashSet<>(initialCapacity, loadFactor);
        for (T element : elements) {
            result.add(element);
        }
        return result;
    }

    /**
     * Convert an {@code Iterator} to an {@code Iterable}.
     * <p>
     * This method is not thread-safe
     *
     * @param iterator
     *            iterator to convert
     * @return a single-use iterable for the iterator (representing the remaining
     *         elements in the iterator)
     * @throws IllegalStateException
     *             when {@linkplain Iterable#iterator()} is called more than
     *             once
     */
    @NotNull
    public static <T> Iterable<T> toIterable(@NotNull final Iterator<T> iterator) {
        Objects.requireNonNull(iterator);

        Iterable<T> delegate = new Iterable<T>() {

            private boolean consumed = false;

            @Override
            public Iterator<T> iterator() {
                if (consumed) {
                    throw new IllegalStateException("Iterator already returned once");
                } else {
                    consumed = true;
                    return iterator;
                }
            }
        };

        return delegate;
    }

    /**
     * Generates a (non-parallel) {@linkplain Stream} for the {@linkplain Iterable}
     * @param iterable iterable to convert
     * @return the stream
     */
    @NotNull
    public static <T> Stream<T> toStream(@NotNull Iterable<T> iterable) {
        Objects.requireNonNull(iterable);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /**
     * Generates a (non-parallel) {@linkplain Stream} for the
     * {@linkplain Iterable}
     * <p>
     * This method is not thread-safe
     *
     * @param iterator
     *            iterator to convert
     * @return the stream (representing the remaining elements in the iterator)
     */
    public static <T> Stream<T> toStream(Iterator<T> iterator) {
        return StreamSupport.stream(toIterable(iterator).spliterator(), false);
    }
}