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
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
    public static <T> List<T> toList(final Iterable<T> iterable) {
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
    public static <T> List<T> toList(final Iterator<T> iterator) {
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
    public static <T> Set<T> toSet(final Iterable<T> iterable) {
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
    public static <T> Set<T> toSet(final Iterator<T> iterator) {
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
    public static <T> Set<T> toSet(final T... elements) {
        final Set<T> result = new HashSet<>();
        for (T element : elements) {
            result.add(element);
        }
        return result;
    }

    /**
     * Convert an {@code Iterator} to an {@code Iterable}.
     *
     * @param iterator
     *            iterator to convert
     * @return a single-use iterable for the iterator (representing the remaining
     *         elements in the iterator)
     * @throws IllegalStateException
     *             when {@linkplain Iterable#iterator()} is called more than
     *             once
     */
    public static <T> Iterable<T> toIterable(final Iterator<T> iterator) {
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
    public static <T> Stream<T> toStream(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /**
     * Generates a (non-parallel) {@linkplain Stream} for the
     * {@linkplain Iterable}
     *
     * @param iterator
     *            iterator to convert
     * @return the stream (representing the remaining elements in the iterator)
     */
    public static <T> Stream<T> toStream(Iterator<T> iterator) {
        return StreamSupport.stream(toIterable(iterator).spliterator(), false);
    }
}