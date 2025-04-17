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

import org.apache.commons.collections4.iterators.PeekingIterator;
import org.jetbrains.annotations.NotNull;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Utility methods for {@link Iterator} conversions.
 */
public class IteratorUtils {

    private IteratorUtils() {
        // no instances for you
    }
    /**
     * Convert an {@code Iterator} to an {@code Iterable}.
     * <p>
     * This method is not thread-safe
     *
     * @param iterator iterator to convert
     * @return a single-use iterable for the iterator (representing the remaining
     * elements in the iterator)
     * @throws IllegalStateException when {@linkplain Iterable#iterator()} is called more than
     *                               once
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
     * Merges multiple sorted iterators into a single sorted iterator. Equivalent entries will not be de-duplicated.
     * <p>
     * This method assumes that the input iterators are sorted in increasing order.
     *
     * @param <T> the type of elements returned by this iterator
     * @param itrs the iterators to merge, must not be null
     * @param c the comparator to determine the order of elements, must not be null
     * @return an iterator that merges the input iterators in sorted order
     * @throws NullPointerException if the iterators or comparator are null
     */
    public static <T> Iterator<T> mergeSorted(final Iterable<? extends Iterator<? extends T>> itrs, final Comparator<? super T> c) {
        Objects.requireNonNull(itrs, "Iterators must not be null");
        Objects.requireNonNull(c, "Comparator must not be null");
        return org.apache.commons.collections4.IteratorUtils.unmodifiableIterator(new MergingIterator<>(itrs, c));
    }

    /**
     * An iterator that merges multiple sorted iterators into a single sorted iterator.
     * <p>This iterator assumes that the input iterators are sorted in increasing order.
     * Equivalent entries will not be de-duplicated.
     *
     * @param <T> the type of elements returned by this iterator
     */
    private static class MergingIterator<T> implements Iterator<T>  {
        final Queue<PeekingIterator<T>> queue;

        /**
         * Constructs a new MergingIterator.
         *
         * @param itrs the iterators to merge, must not be null
         * @param c the comparator to determine the order of elements, must not be null
         * @throws NullPointerException if the iterators or comparator are null
         */
        public MergingIterator(final Iterable<? extends Iterator<? extends T>> itrs, final Comparator<? super T> c) {
            Comparator<PeekingIterator<T>> heapComparator = Comparator.comparing(PeekingIterator::peek, c);

            this.queue = new PriorityQueue<>(heapComparator);

            for (Iterator<? extends T> itr : itrs) {
                if (itr.hasNext()) {
                    // only add those iterator which have elements
                    this.queue.add(PeekingIterator.peekingIterator(itr));
                }
            }
        }

        /**
         * Returns {@code true} if the iteration has more elements.
         *
         * @return {@code true} if the iteration has more elements
         */
        @Override
        public boolean hasNext() {
            return !this.queue.isEmpty();
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements available");
            }

            final PeekingIterator<T> nextItr = this.queue.remove();
            T next = nextItr.next();
            if (nextItr.hasNext()) {
                this.queue.add(nextItr);
            }
            return next;
        }
    }

    /**
     * Compares two iterators to determine if they contain the same elements in the same order.
     * <p>
     * This method iterates through both iterators and compares each corresponding pair of elements.
     * <p>
     * Note that this method consumes both iterators.
     *
     * @param iterator1 the first iterator to compare, may be null
     * @param iterator2 the second iterator to compare, may be null
     * @return {@code true} if both iterators contain the same number of elements and all corresponding elements
     *         are equal, {@code false} otherwise.
     */
    public static boolean elementsEqual(final Iterator<?> iterator1, final Iterator<?> iterator2) {
        if (iterator1 == iterator2) {
            // returns true if both point to same object or both are null
            return true;
        }

        if (iterator1 == null || iterator2 == null) {
            // returns false if one of the iterator is null
            return false;
        }

        while (iterator1.hasNext() && iterator2.hasNext()) {
            if (!Objects.equals(iterator1.next(), iterator2.next())) {
                return false;
            }
        }

        // return true if both the iterators have same number of elements
        return !iterator1.hasNext() && !iterator2.hasNext();
    }

    /**
     * Returns the number of elements in the given iterator.
     * <p>
     * This method consumes the iterator to count the elements.
     * A null or empty iterator returns 0.
     *
     * @param iterator the iterator whose size is to be determined
     * @return the number of elements in the iterator
     */
    public static int size(Iterator<?> iterator) {
        return org.apache.commons.collections4.IteratorUtils.size(iterator);
    }

    /**
     * Returns the element at the specified position in the iterator.
     * <p>
     * This method will consume the iterator up to the specified position.
     * <p>
     * @param <T> the type of elements in the iterator
     * @param iterator the iterator to get the element from, must not be null
     * @param index the position of the element to return, zero-based
     * @return the element at the specified position
     * @throws NullPointerException if the iterator is null
     * @throws IndexOutOfBoundsException if the iterator is empty or index is negative or greater than the number
     * of elements in the iterator
     */
    public static <T> T get(Iterator<T> iterator, int index) {
        Objects.requireNonNull(iterator, "Iterator must not be null");
        return org.apache.commons.collections4.IteratorUtils.get(iterator, index);
    }

    /**
     * Returns the last element in the given iterator.
     * <p>
     * This method consumes the entire iterator to find the last element.
     * <p>
     *
     * @param <T>      the type of elements in the iterator
     * @param iterator the iterator to get the last element from, must not be null
     * @return the last element in the iterator
     * @throws NullPointerException   if the iterator is null
     * @throws NoSuchElementException if the iterator is empty
     */
    public static <T> T getLast(Iterator<T> iterator) {
        Objects.requireNonNull(iterator, "Iterator must not be null");
        while (true) {
            T currentElement = iterator.next();
            if (!iterator.hasNext()) {
                return currentElement;
            }
        }
    }

    /**
     * Checks if the given iterator contains the specified element.
     * <p>
     * This method iterates through the iterator, checking each element for equality with
     * the specified object using {@link Objects#equals(Object, Object)}. The iteration stops
     * once a match is found or the iterator is exhausted.
     * <p>
     * Note that this method will consume the iterator.
     *
     * @param <?> the type of objects in the iterator
     * @param iterator the iterator to check, must not be null
     * @param element the element to find, may be null
     * @return {@code true} if the iterator contains the element, {@code false} otherwise
     * @throws NullPointerException if the iterator is null
     */
    public static boolean contains(Iterator<?> iterator, Object element) {
        Objects.requireNonNull(iterator, "Iterator must not be null");
        return org.apache.commons.collections4.IteratorUtils.contains(iterator, element);
    }

    /**
     * Converts an iterator to an array of a specific type.
     * <p>
     * This method consumes the iterator and returns an array containing all of its elements.
     * The type of the array is determined by the provided {@code type} parameter.
     *
     * @param <T>      the component type of the resulting array
     * @param iterator the iterator to convert, must not be null
     * @param type     the {@link Class} object representing the component type of the array, must not be null
     * @return an array containing all the elements from the iterator
     * @throws NullPointerException if the iterator or type is null
     * @since 4.1
     */
    public static <T> T[] toArray(Iterator<? extends T> iterator, Class<T> type) {
        return org.apache.commons.collections4.IteratorUtils.toArray(iterator, type);
    }
}
