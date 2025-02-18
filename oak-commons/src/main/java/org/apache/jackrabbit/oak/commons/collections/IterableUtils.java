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

import org.apache.commons.collections4.iterators.LazyIteratorChain;

import java.util.Iterator;
import java.util.Objects;

/**
 * Utility methods for {@link Iterable} conversions.
 */
public class IterableUtils {

    private IterableUtils() {
        // no instances for you
    }

    /**
     * Combines two iterables into a single iterable.
     * <p>
     * The returned iterable has an iterator that traverses the elements in {@code a},
     * followed by the elements in {@code b}. The source iterators are not polled until necessary.
     * <p>
     * The returned iterable's iterator supports {@code remove()} when the corresponding
     * input iterator supports it.
     *
     * @param <E> the element type
     * @param a  the first iterable, may not be null
     * @param b  the second iterable, may not be null
     * @return a new iterable, combining the provided iterables
     * @throws NullPointerException if either of the provided iterables is null
     */
    public static <E> Iterable<E> chainedIterable(final Iterable<? extends E> a,
                                                  final Iterable<? extends E> b) {
        return org.apache.commons.collections4.IterableUtils.chainedIterable(a, b);
    }

    /**
     * Combines three iterables into a single iterable.
     * <p>
     * The returned iterable has an iterator that traverses the elements in {@code a},
     * followed by the elements in {@code b} and {@code c}. The source
     * iterators are not polled until necessary.
     * <p>
     * The returned iterable's iterator supports {@code remove()} when the corresponding
     * input iterator supports it.
     *
     * @param <E> the element type
     * @param a  the first iterable, may not be null
     * @param b  the second iterable, may not be null
     * @param c  the third iterable, may not be null
     * @return a new iterable, combining the provided iterables
     * @throws NullPointerException if either of the provided iterables is null
     */
    public static <E> Iterable<E> chainedIterable(final Iterable<? extends E> a,
                                                  final Iterable<? extends E> b,
                                                  final Iterable<? extends E> c) {
        return org.apache.commons.collections4.IterableUtils.chainedIterable(a, b, c);
    }

    /**
     * Combines four iterables into a single iterable.
     * <p>
     * The returned iterable has an iterator that traverses the elements in {@code a},
     * followed by the elements in {@code b}, {@code c} and {@code d}. The source
     * iterators are not polled until necessary.
     * <p>
     * The returned iterable's iterator supports {@code remove()} when the corresponding
     * input iterator supports it.
     *
     * @param <E> the element type
     * @param a  the first iterable, may not be null
     * @param b  the second iterable, may not be null
     * @param c  the third iterable, may not be null
     * @param d  the fourth iterable, may not be null
     * @return a new iterable, combining the provided iterables
     * @throws NullPointerException if either of the provided iterables is null
     */
    public static <E> Iterable<E> chainedIterable(final Iterable<? extends E> a,
                                                  final Iterable<? extends E> b,
                                                  final Iterable<? extends E> c,
                                                  final Iterable<? extends E> d) {
        return org.apache.commons.collections4.IterableUtils.chainedIterable(a, b, c, d);
    }

    /**
     * Combines the provided iterables into a single iterable.
     * <p>
     * The returned iterable has an iterator that traverses the elements in the order
     * of the arguments, i.e. iterables[0], iterables[1], .... The source iterators
     * are not polled until necessary.
     * <p>
     * The returned iterable's iterator supports {@code remove()} when the corresponding
     * input iterator supports it.
     *
     * @param <E> the element type
     * @param iterables  the iterables to combine, may not be null
     * @return a new iterable, combining the provided iterables
     * @throws NullPointerException if either of the provided iterables is null
     */
    @SafeVarargs
    public static <E> Iterable<E> chainedIterable(final Iterable<? extends E>... iterables) {
        Objects.requireNonNull(iterables);
        return org.apache.commons.collections4.IterableUtils.chainedIterable(iterables);
    }

    /**
     * Creates an {@code Iterable} that chains multiple {@code Iterable} instances into a single {@code Iterable}.
     * <p>
     * The returned {@code Iterable} will iterate over all elements of the first {@code Iterable},
     * then all elements of the second, and so on.
     *
     * @param <E> the type of elements returned by the iterator
     * @param iterables an {@code Iterable} of {@code Iterable} instances to be chained
     * @return an {@code Iterable} that provides a single view of all elements in the input {@code Iterable} instances
     * @throws NullPointerException if the input {@code Iterable} or any of its elements are null
     */
    public static <E> Iterable<E> chainedIterable(final Iterable<? extends Iterable<? extends E>> iterables) {
        Objects.requireNonNull(iterables);
        return () -> new LazyIteratorChain<>() {
            private final Iterator<? extends Iterable<? extends E>> iterator = iterables.iterator();

            protected Iterator<? extends E> nextIterator(int count) {
                return iterator.hasNext() ? iterator.next().iterator() : null;
            }
        };
    }

    /**
     * Checks if the specified object is present in the given iterable.
     *
     * @param <E> the type of elements in the iterable
     * @param iterable the iterable to search, may not be null
     * @param object the object to find, may be null
     * @return true if the iterable contains the object, false otherwise
     * @throws NullPointerException if the iterable is null
     */
    public static <E> boolean contains(final Iterable<E> iterable, final Object object) {
        return org.apache.commons.collections4.IterableUtils.contains(iterable, object);
    }
}
