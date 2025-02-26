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
}
