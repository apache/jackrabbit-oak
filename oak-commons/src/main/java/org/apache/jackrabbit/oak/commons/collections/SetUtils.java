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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility methods for {@link java.util.Set} conversions.
 */
public class SetUtils {

    private SetUtils() {
        // no instances for you
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
        final Set<T> result = new HashSet<>(CollectionUtils.ensureCapacity(elements.length));
        result.addAll(Arrays.asList(elements));
        return result;
    }

    /**
     * Creates a new, empty HashSet with expected capacity.
     * <p>
     * The returned set is large enough to add expected no. of elements without resizing.
     *
     * @param capacity the expected number of elements
     * @throws IllegalArgumentException if capacity is negative
     * @see CollectionUtils#newHashMap(int)
     * @see SetUtils#newLinkedHashSet(int)
     */
    @NotNull
    public static <K> Set<K> newHashSet(final int capacity) {
        // make sure the set does not need to be resized given the initial content
        return new HashSet<>(CollectionUtils.ensureCapacity(capacity));
    }

    /**
     * Creates a new, empty IdentityHashSet with default size.
     */
    @NotNull
    public static <E> Set<E> newIdentityHashSet() {
        return Collections.newSetFromMap(new IdentityHashMap<>());
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
        final Set<T> result = new LinkedHashSet<>(CollectionUtils.ensureCapacity(elements.length));
        result.addAll(Arrays.asList(elements));
        return result;
    }

    /**
     * Creates a new, empty LinkedHashSet with expected capacity.
     * <p>
     * The returned set is large enough to add expected no. of elements without resizing.
     *
     * @param capacity the expected number of elements
     * @throws IllegalArgumentException if capacity is negative
     * @see CollectionUtils#newHashMap(int)
     * @see SetUtils#newHashSet(int)
     */
    @NotNull
    public static <K> Set<K> newLinkedHashSet(final int capacity) {
        // make sure the set does not need to be resized given the initial content
        return new LinkedHashSet<>(CollectionUtils.ensureCapacity(capacity));
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
     * Creates a new, empty {@link Set} which is backed by {@link ConcurrentHashMap} to allow concurrent access.
     * Returning Set doesn't allow null keys and values.
     *
     * @return a new, empty {@link Set} which is backed by {@link ConcurrentHashMap}.
     *
     * @see CollectionUtils#newHashMap(int)
     * @see SetUtils#newLinkedHashSet(int)
     * @see SetUtils#newConcurrentHashSet(Iterable) (int)
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
     * @see SetUtils#newLinkedHashSet(int)
     * @see SetUtils#newConcurrentHashSet()
     */
    @NotNull
    public static <K> Set<K> newConcurrentHashSet(@NotNull Iterable<? extends K> elements) {
        Objects.requireNonNull(elements);
        final Set<K> set = newConcurrentHashSet();
        elements.forEach(set::add);
        return set;
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
     * Returns a new set containing the intersection of the two specified sets.
     * The intersection of two sets is a set containing only the elements that are present in both sets.
     *
     * @param <T> the type of elements in the sets
     * @param s1 the first set, must not be null
     * @param s2 the second set, must not be null
     * @return a new set containing the intersection of the two specified sets
     * @throws NullPointerException if either of the sets is null
     */
    @NotNull
    public static <T> Set<T> intersection(@NotNull final Set<T> s1, @NotNull final Set<T> s2) {
        Objects.requireNonNull(s1);
        Objects.requireNonNull(s2);
        return s1.stream().filter(s2::contains).collect(Collectors.toSet());
    }

    /**
     * Returns a new set containing the symmetric difference of the two specified sets.
     * The symmetric difference of two sets is a set containing elements that are in either of the sets,
     * but not in their intersection.
     *
     * @param <T> the type of elements in the sets
     * @param s1 the first set, must not be null
     * @param s2 the second set, must not be null
     * @return a new set containing the symmetric difference of the two specified sets
     * @throws NullPointerException if either of the sets is null
     */
    public static <T> Set<T> symmetricDifference(final Set<T> s1, final Set<T> s2) {
        Objects.requireNonNull(s1);
        Objects.requireNonNull(s2);
        final Set<T> result = new HashSet<>(s1);
        s2.stream().filter(Predicate.not(result::add)).forEach(result::remove);
        return result;
    }

    /**
     * Returns a new set containing the difference of the two specified sets.
     * The difference of two sets is a set containing elements that are in the first set
     * but not in the second set.
     *
     * @param <T> the type of elements in the sets
     * @param s1 the first set, must not be null
     * @param s2 the second set, must not be null
     * @return a new set containing the difference of the two specified sets
     * @throws NullPointerException if either of the sets is null
     */
    public static <T> Set<T> difference(final Set<T> s1, final Set<T> s2) {
        Objects.requireNonNull(s1);
        Objects.requireNonNull(s2);
        return s1.stream().filter(e -> !s2.contains(e)).collect(Collectors.toSet());
    }
}
