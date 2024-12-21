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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.guava.common.collect.Maps;
import org.jetbrains.annotations.NotNull;

/**
 * Utility methods for collections conversions.
 */
public class CollectionUtils {

    // Maximum capacity for a hash based collection. (used internally by JDK).
    // Also, it helps to avoid overflow errors when calculating the capacity
    private static final int MAX_CAPACITY = 1 << 30;

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
    public static <T> List<T> toList(@NotNull final Iterable<? extends T> iterable) {
        Objects.requireNonNull(iterable);
        List<T> result = new ArrayList<>();
        iterable.forEach(result::add);
        return result;
    }

    /**
     * Convert an iterable to a {@link java.util.LinkedList}. The returning LinkedList is mutable and supports all optional operations.
     * @param iterable the iterator to convert
     * @return the LinkedList
     * @param <T> the type of the elements
     */
    @NotNull
    public static <T> List<T> toLinkedList(final Iterable<T> iterable) {
        Objects.requireNonNull(iterable);
        List<T> result = new LinkedList<>();
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
    public static <T> List<T> toList(final Iterator<? extends T> iterator) {
        Objects.requireNonNull(iterator);
        List<T> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        return result;
    }

    /**
     * Split a list into partitions of a given size.
     *
     * @param list the list to partition
     * @param n the size of partitions
     * @return a list of partitions. The resulting partitions aren’t a view of the main List, so any changes happening to the main List won’t affect the partitions.
     * @param <T> the type of the elements
     */
    @NotNull
    public static <T> List<List<T>> partitionList(final List<T> list, final int n) {
        Objects.requireNonNull(list);
        return IntStream.range(0, list.size())
                .filter(i -> i % n == 0)
                .mapToObj(i -> list.subList(i, Math.min(i + n, list.size())))
                .collect(Collectors.toList());
    }

    /**
     * Convert an iterable to a set. The returning set is mutable and supports all optional operations.
     * @param iterable the iterable to convert
     * @return the set
     * @param <T> the type of the elements
     */
    @NotNull
    public static <T> Set<T> toSet(@NotNull  final Iterable<? extends T> iterable) {
        Objects.requireNonNull(iterable);
        final Set<T> result = new HashSet<>();
        iterable.forEach(result::add);
        return result;
    }

    /**
     * Convert an iterable to a {@link LinkedHashSet}. The returning set is mutable and supports all optional operations.
     * @param iterable the iterable to convert
     * @return the linkedHashSet
     * @param <T> the type of the elements
     */
    @NotNull
    public static <T> Set<T> toLinkedSet(@NotNull  final Iterable<T> iterable) {
        Objects.requireNonNull(iterable);
        final Set<T> result = new LinkedHashSet<>();
        iterable.forEach(result::add);
        return result;
    }

    /**
     * Convert an iterable to a {@link java.util.TreeSet}. The returning set is mutable and supports all optional operations.
     * @param iterable the iterable to convert
     * @return the treeSet
     * @param <T> the type of the elements
     * @throws ClassCastException if the specified object cannot be compared
     *         with the elements currently in this set
     * @throws NullPointerException if the specified element is null
     *         and this set uses natural ordering, or its comparator
     *         does not permit null elements
     */
    @NotNull
    public static <T extends Comparable> TreeSet<T> toTreeSet(@NotNull  final Iterable<? extends T> iterable) {
        Objects.requireNonNull(iterable);
        final TreeSet<T> result = new TreeSet<>();
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
        final Set<T> result = new HashSet<>(ensureCapacity(elements.length));
        result.addAll(Arrays.asList(elements));
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
    public static <T> Set<T> toLinkedSet(@NotNull final T... elements) {
        Objects.requireNonNull(elements);
        // make sure the set does not need to be resized given the initial content
        final Set<T> result = new LinkedHashSet<>(ensureCapacity(elements.length));
        result.addAll(Arrays.asList(elements));
        return result;
    }

    /**
     * Returns a new set containing the union of the two specified sets.
     * The union of two sets is a set containing all the elements of both sets.
     *
     * @param <T> the type of elements in the sets
     * @param s1 the first set, must not be null
     * @param s2 the second set, must not be null
     * @return a new set containing the union of the two specified sets
     * @throws NullPointerException if either of the sets is null
     */
    @NotNull
    public static <T> Set<T> union(@NotNull final Set<T> s1, @NotNull final Set<T> s2) {
        Objects.requireNonNull(s1);
        Objects.requireNonNull(s2);
        return Stream.concat(s1.stream(), s2.stream()).collect(Collectors.toSet());
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

    /**
     * Creates a new, empty HashSet with expected capacity.
     * <p>
     * The returned set is large enough to add expected no. of elements without resizing.
     *
     * @param capacity the expected number of elements
     * @throws IllegalArgumentException if capacity is negative
     * @see CollectionUtils#newHashMap(int)
     * @see CollectionUtils#newLinkedHashSet(int)
     */
    @NotNull
    public static <K> Set<K> newHashSet(final int capacity) {
        // make sure the set does not need to be resized given the initial content
        return new HashSet<>(ensureCapacity(capacity));
    }

    /**
     * Creates a new, empty {@link Set} which is backed by {@link ConcurrentHashMap} to allow concurrent access.
     * Returning Set doesn't allow null keys and values.
     *
     * @return a new, empty {@link Set} which is backed by {@link ConcurrentHashMap}.
     *
     * @see CollectionUtils#newHashMap(int)
     * @see CollectionUtils#newLinkedHashSet(int)
     * @see CollectionUtils#newConcurrentHashSet(Iterable) (int)
     */
    @NotNull
    public static <K> Set<K> newConcurrentHashSet() {
        return ConcurrentHashMap.newKeySet();
    }

    /**
     * Creates a new {@link Set} with given values which is backed by {@link ConcurrentHashMap} to allow concurrent access.
     * Returning Set doesn't allow null keys and values.
     *
     * @return a new, empty {@link Set} which is backed by {@link ConcurrentHashMap}.
     * @throws NullPointerException if any element of the iterable is null
     *
     * @see CollectionUtils#newHashMap(int)
     * @see CollectionUtils#newLinkedHashSet(int)
     * @see CollectionUtils#newConcurrentHashSet()
     */
    @NotNull
    public static <K> Set<K> newConcurrentHashSet(@NotNull Iterable<? extends K> elements) {
        Objects.requireNonNull(elements);
        final Set<K> set = newConcurrentHashSet();
        elements.forEach(set::add);
        return set;
    }

    /**
     * Creates a new, empty LinkedHashSet with expected capacity.
     * <p>
     * The returned set is large enough to add expected no. of elements without resizing.
     *
     * @param capacity the expected number of elements
     * @throws IllegalArgumentException if capacity is negative
     * @see CollectionUtils#newHashMap(int)
     * @see CollectionUtils#newHashSet(int)
     */
    @NotNull
    public static <K> Set<K> newLinkedHashSet(final int capacity) {
        // make sure the set does not need to be resized given the initial content
        return new LinkedHashSet<>(ensureCapacity(capacity));
    }

    /**
     * Creates a new, empty IdentityHashSet with default size.
     */
    @NotNull
    public static <E> Set<E> newIdentityHashSet() {
        return Collections.newSetFromMap(Maps.newIdentityHashMap());
    }

    /**
     * Creates a new, empty HashMap with expected capacity.
     * <p>
     * The returned map uses the default load factor of 0.75, and its capacity is
     * large enough to add expected number of elements without resizing.
     *
     * @param capacity the expected number of elements
     * @throws IllegalArgumentException if capacity is negative
     * @see CollectionUtils#newHashSet(int)
     * @see CollectionUtils#newLinkedHashSet(int)
     */
    @NotNull
    public static <K, V> Map<K, V> newHashMap(final int capacity) {
        // make sure the Map does not need to be resized given the initial content
        return new HashMap<>(ensureCapacity(capacity));
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

        return new Iterable<>() {

            private boolean consumed = false;

            @Override
            public @NotNull Iterator<T> iterator() {
                if (consumed) {
                    throw new IllegalStateException("Iterator already returned once");
                } else {
                    consumed = true;
                    return iterator;
                }
            }
        };
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
    @NotNull
    public static <T> Stream<T> toStream(@NotNull Iterator<T> iterator) {
        return StreamSupport.stream(toIterable(iterator).spliterator(), false);
    }

    /**
     * Ensure the capacity of a map or set given the expected number of elements.
     *
     * @param capacity the expected number of elements
     * @return the capacity to use to avoid rehashing & collisions
     */
    static int ensureCapacity(final int capacity) {

        if (capacity < 0) {
            throw new IllegalArgumentException("Capacity must be non-negative");
        }

        if (capacity > MAX_CAPACITY) {
            return MAX_CAPACITY;
        }

        return 1 + (int) (capacity / 0.75f);
    }
}