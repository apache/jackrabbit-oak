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
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    /**
     * Returns the number of elements in the specified iterable.
     *
     * @param itr the iterable to count elements in, may not be null
     * @return the number of elements in the iterable
     * @throws NullPointerException if the iterable is null
     */
    public static int size(final Iterable<?> itr) {
        return org.apache.commons.collections4.IterableUtils.size(itr);
    }

    /**
     * Checks if all elements in the specified iterable match the given predicate.
     *
     * @param <E> the type of elements in the iterable
     * @param itr the iterable to check, may not be null
     * @param predicate the predicate to apply to elements, may not be null
     * @return true if all elements match the predicate, false otherwise
     * @throws NullPointerException if the iterable or predicate is null
     */
    public static <E> boolean matchesAll(final Iterable<E> itr, final Predicate<? super E> predicate) {
        return org.apache.commons.collections4.IterableUtils.matchesAll(itr, predicate::test);
    }

    /**
     * Checks if the specified iterable is empty.
     *
     * @param itr the iterable to check, may be null
     * @return true if the iterable is empty or null, false otherwise
     */
    public static boolean isEmpty(final Iterable<?> itr) {
        return org.apache.commons.collections4.IterableUtils.isEmpty(itr);
    }

    /**
     * Converts an Iterable to an array of the specified type.
     *
     * @param <T> the type of elements in the itr
     * @param itr the itr to convert, may be null
     * @param type the class of the type of elements in the array, may not be null
     * @return an array containing the elements of the itr
     * @throws NullPointerException if the itr or type is null
     */
    @NotNull
    @SuppressWarnings("unchecked")
    public static <T> T[] toArray(final Iterable<T> itr, final Class<T> type) {

        final T[] t = (T[]) Array.newInstance(type, 0);

        final Collection<T> collection = itr instanceof Collection ? (Collection<T>) itr : ListUtils.toList(itr);
        return collection.toArray(t);
    }

    /**
     * Splits an Iterable into an Iterator of sub-iterators, each of the specified size.
     *
     * @param <T> the type of elements in the itr
     * @param itr the itr to split, may not be null
     * @param size the size of each sub-iterator, must be greater than 0
     * @return an iterator of sub-iterators, each of the specified size
     * @throws NullPointerException if the itr is null
     * @throws IllegalArgumentException if size is less than or equal to 0
     */
    public static <T> Iterable<List<T>> partition(final Iterable<T> itr, final int size) {

        Objects.requireNonNull(itr, "Iterable must not be null.");
        Validate.checkArgument(size > 0, "Size must be greater than 0.");

        return new Iterable<>() {
            @Override
            public @NotNull Iterator<List<T>> iterator() {
                return IteratorUtils.partition(itr.iterator(), size);
            }
        };
    }

    /**
     * Filters an Iterable based on a given predicate.
     *
     * @param <E> the type of elements in the iterable
     * @param itr the iterable to filter, may not be null
     * @param predicate the predicate to apply to elements, may not be null
     * @return an iterable containing only the elements that match the predicate
     * @throws NullPointerException if the iterable or predicate is null
     */
    public static <E> Iterable<E> filter(final Iterable<E> itr, final Predicate<? super E> predicate) {
        return org.apache.commons.collections4.IterableUtils.filteredIterable(itr, predicate::test);
    }

    /**
     * Filters an Iterable to include only elements of the specified class type.
     *
     * @param <E> the type of elements to include
     * @param itr the iterable to filter, may not be null
     * @param type the class type to filter by, may not be null
     * @return an iterable containing only the elements of the specified class type
     * @throws NullPointerException if the iterable or class type is null
     */
    @SuppressWarnings("unchecked")
    public static <E> Iterable<E> filter(final Iterable<?> itr, final Class<E> type) {
        return (Iterable<E>) StreamUtils.toStream(itr).filter(type::isInstance).collect(Collectors.toList());
    }

    /**
     * Transforms an Iterable by applying a given function to each element.
     *
     * @param <I> the type of input elements
     * @param <O> the type of output elements
     * @param iterable the iterable to transform, must not be null
     * @param function the function to apply to each element, must not be null
     * @return an iterable containing the transformed elements
     * @throws NullPointerException if the iterable or function is null
     */
    public static <I, O> Iterable<O> transform(final Iterable<I> iterable, final Function<? super I, ? extends O> function) {
        return org.apache.commons.collections4.IterableUtils.transformedIterable(iterable, function::apply);
    }

    /**
     * Merges multiple sorted iterables into a single sorted iterable.
     *
     * @param <T> the type of elements returned by this iterable
     * @param iterables the iterables to merge, must not be null
     * @param c the c to determine the order of elements, must not be null
     * @return an iterable that merges the input iterables in sorted order
     * @throws NullPointerException if the iterables or c are null
     */
    public static <T> Iterable<T> mergeSorted(final Iterable<? extends Iterable<? extends T>> iterables, final Comparator<? super T> c) {
        Objects.requireNonNull(iterables, "Iterables must not be null.");
        Objects.requireNonNull(c, "Comparator must not be null.");
        final Iterable<T> iterable = () -> IteratorUtils.mergeSorted(org.apache.commons.collections4.IterableUtils.transformedIterable(iterables, Iterable::iterator), c);
        return org.apache.commons.collections4.IterableUtils.unmodifiableIterable(iterable);
    }

    /**
     * Checks if two iterables have the same elements in the same order.
     * <p>
     * This method iterates through both iterables and compares each corresponding pair of elements using
     * {@link Objects#equals(Object, Object)}.
     * <p>
     * Note that both iterables will be fully traversed during the comparison.
     *
     * @param itr1 the first iterable to compare, may be null
     * @param itr2 the second iterable to compare, may be null
     * @return {@code true} if both iterables contain the same elements in the same order, {@code false} otherwise.
     *         Returns {@code true} if both iterables are null and {@code false} if only one is null.
     *
     * @see IteratorUtils#elementsEqual(Iterator, Iterator)
     */
    public static boolean elementsEqual(final Iterable<?> itr1, final Iterable<?> itr2) {

        if (itr1 == itr2) {
            // Both are null or the same instance
            return true;
        }

        if (itr1 == null || itr2 == null) {
            // returns false if one of the iterator is null
            return false;
        }

        if (itr1 instanceof Collection && itr2 instanceof Collection) {
            Collection<?> c1 = (Collection<?>) itr1;
            Collection<?> c2 = (Collection<?>) itr2;
            if (c1.size() != c2.size()) {
                return false;
            }
        }
        return IteratorUtils.elementsEqual(itr1.iterator(), itr2.iterator());
    }

    /**
     * Creates an iterable limited to the specified number of elements.
     * <p>
     * The returned iterable's iterator will stop returning elements after the specified limit
     * has been reached or when the source iterable's iterator is exhausted, whichever comes first.
     * <p>
     * The returned iterable's iterator supports {@code remove()} if the original iterator supports it.
     *
     * @param <T> the type of elements in the iterable
     * @param iterable the iterable to limit, may be null
     * @param limitSize the maximum number of elements to return
     * @return a limited iterable
     * @throws IllegalArgumentException if limitSize is negative
     */
    public static <T> Iterable<T> limit(final Iterable<T> iterable, final int limitSize) {
        return org.apache.commons.collections4.IterableUtils.boundedIterable(iterable, limitSize);
    }

    /**
     * Returns a string representation of the elements of the specified iterable.
     * <p>
     * The string representation consists of a list of the iterable's elements,
     * enclosed in square brackets ({@code "[]"}). Adjacent elements are separated
     * by the characters {@code ", "} (a comma followed by a space). Elements are
     * converted to strings as by {@code String.valueOf(Object)}.
     *
     * @param iterable  the iterable to convert to a string, may be null
     * @return a string representation of {@code iterable}
     */
    public static String toString(final Iterable<?> iterable) {
        return org.apache.commons.collections4.IterableUtils.toString(iterable);
    }

    /**
     * Returns the first element of the specified iterable, or the default value if the iterable is empty.
     * <p>
     * The iterable is only traversed enough to get the first element. If the iterable is empty,
     * the default value is returned instead.
     *
     * @param <T> the type of elements in the iterable
     * @param iterable the iterable to get the first element from, may be null
     * @param defaultValue the value to return if the iterable is empty
     * @return the first element in the iterable or the default value if the iterable is empty
     * @throws NullPointerException if the iterable is null
     */
    public static <T> T getFirst(final Iterable<T> iterable, final  T defaultValue) {
        Objects.requireNonNull(iterable, "Iterable must not be null.");
        final Iterator<T> iterator = iterable.iterator();
        return iterator.hasNext() ? iterator.next() : defaultValue;
    }

    /**
     * Returns the element at the specified index in the specified iterable.
     * <p>
     * The iterable is traversed until the specified index is reached. If the
     * position is greater than the number of elements in the iterable, an
     * IndexOutOfBoundsException is thrown.
     *
     * @param <T> the type of elements in the iterable
     * @param iterable the iterable to get the element from, must not be null
     * @param index the index of the element to retrieve, must be non-negative
     * @return the element at the specified index
     * @throws NullPointerException if the iterable is null
     * @throws IndexOutOfBoundsException if the position is negative or greater than
     *         the number of elements in the iterable
     */
    public static <T> T get(final Iterable<T> iterable, final int index) {
        Objects.requireNonNull(iterable, "Iterable must not be null.");
        return org.apache.commons.collections4.IterableUtils.get(iterable, index);
    }

    /**
     * Returns the first element in the specified iterable that matches the given predicate.
     * <p>
     * The iterable is traversed until an element is found that satisfies the predicate.
     * If no element satisfies the predicate, {@code null} is returned.
     *
     * @param <T> the type of elements in the iterable
     * @param iterable the iterable to search, must not be null
     * @param predicate the predicate to apply, must not be null
     * @return the first element that satisfies the predicate, or null if no such element exists
     * @throws NullPointerException if either the iterable or predicate is null
     */
    public static <T> T find(final Iterable<T> iterable, final Predicate<? super T> predicate) {
        Objects.requireNonNull(iterable, "Iterable must not be null.");
        Objects.requireNonNull(predicate, "Predicate must not be null.");
        return org.apache.commons.collections4.IterableUtils.find(iterable, predicate::test);
    }

    /**
     * Returns the last element of the specified iterable, or null if the iterable is empty.
     * <p>
     * The iterable must be fully traversed to find the last element.
     *
     * @param <T> the type of elements in the iterable
     * @param iterable the iterable to get the last element from, must not be null
     * @return the last element in the iterable or null if the iterable is empty
     */
    public static <T> T getLast(final Iterable<T> iterable) {

        Objects.requireNonNull(iterable, "Iterable must not be null.");

        // Optimize for Lists
        if (iterable instanceof List) {
            final List<T> list = (List<T>) iterable;
            return list.isEmpty() ? null : list.get(list.size() - 1);
        }

        // For non-List iterables
        T last = null;
        for (final T element : iterable) {
            last = element;
        }

        return last;
    }
}
