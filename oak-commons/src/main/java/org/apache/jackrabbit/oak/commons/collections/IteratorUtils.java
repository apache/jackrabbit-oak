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

import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.apache.commons.collections4.iterators.UnmodifiableIterator;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.Predicate;

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
     */
    public static <T> T[] toArray(Iterator<? extends T> iterator, Class<T> type) {
        return org.apache.commons.collections4.IteratorUtils.toArray(iterator, type);
    }

    /**
     * Converts an iterator to an enumeration.
     * <p>
     * This method creates an {@link Enumeration} that will use the provided {@link Iterator}
     * as its source of elements. The enumeration will iterate through the same elements
     * as the iterator in the same order.
     * <p>
     * The enumeration's {@code hasMoreElements()} and {@code nextElement()} methods
     * delegate to the iterator's {@code hasNext()} and {@code next()} methods respectively.
     *
     * @param <T> the type of elements in the iterator and enumeration
     * @param iterator the iterator to convert to an enumeration, must not be null
     * @return an enumeration that uses the provided iterator as its source
     * @throws NullPointerException if the iterator is null
     */
    public static <T> Enumeration<T> asEnumeration(final Iterator<T> iterator) {
        return org.apache.commons.collections4.IteratorUtils.asEnumeration(iterator);
    }

    /**
     * Returns an iterator that iterates through two iterators in sequence.
     * <p>
     * This method creates a new iterator that will first iterate through the elements
     * in the first iterator and then, when the first iterator is exhausted, will iterate
     * through the elements in the second iterator.
     * <p>
     * The returned iterator supports {@link Iterator#remove()} if the provided iterators
     * support it.
     *
     * @param <E> the element type
     * @param iterator1 the first iterator to chain, may be null
     * @param iterator2 the second iterator to chain, may be null
     * @return an iterator that chains the specified iterators together
     * @throws NullPointerException if any of the iterator is null
     */
    public static <E> Iterator<E> chainedIterator(final Iterator<? extends E> iterator1,
                                                  final Iterator<? extends E> iterator2) {
        return org.apache.commons.collections4.IteratorUtils.chainedIterator(iterator1, iterator2);
    }

    /**
     * Returns an iterator that iterates through varargs of iterators in sequence.
     * <p>
     * This method creates a new iterator that will first iterate through the elements
     * in the first iterator and then, when the first iterator is exhausted, will iterate
     * through the elements in the second iterator and so on...
     * <p>
     * The returned iterator supports {@link Iterator#remove()} if the underlying iterator
     * support it.
     *
     * @param <E> the element type
     * @param iterators array of iterators to chain must not be null
     * @return an iterator that chains the specified iterators together
     * @throws NullPointerException if iterators array is null or contains a null iterator
     */
    @SafeVarargs
    public static <E> Iterator<E> chainedIterator(final Iterator<? extends E>... iterators) {
        return org.apache.commons.collections4.IteratorUtils.chainedIterator(iterators);
    }

    /**
     * Returns an iterator that iterates through a collection of iterators in sequence.
     * <p>
     * This method creates a new iterator that will first iterate through the elements
     * in the first iterator and then, when the first iterator is exhausted, will iterate
     * through the elements in the second iterator and so on...
     * <p>
     * The returned iterator supports {@link Iterator#remove()} if the underlying iterator
     * support it.
     *
     * @param <E> the element type
     * @param iterators collection of iterators to chain must not be null
     * @return an iterator that chains the specified iterators together
     * @throws NullPointerException if an iterators collection is null or contains a null iterator
     */
    public static <E> Iterator<E> chainedIterator(final Collection<Iterator<? extends E>> iterators) {
        return org.apache.commons.collections4.IteratorUtils.chainedIterator(iterators);
    }

    /**
     * Returns an iterator that iterates through an iterator of iterators in sequence.
     * <p>
     * This method creates a new iterator that will first iterate through the elements
     * in the first iterator and then, when the first iterator is exhausted, will iterate
     * through the elements in the second iterator and so on...
     * <p>
     * The returned iterator supports {@link Iterator#remove()} if the underlying iterator
     * support it.
     *
     * @param <E> the element type
     * @param iterators an iterator of iterators to chain must not be null
     * @return an iterator that chains the specified iterators together
     * @throws NullPointerException if an iterators collection is null or contains a null iterator
     */
    public static <E> Iterator<E> chainedIterator(final Iterator<? extends Iterator<? extends E>> iterators) {
        final IteratorChain<E> eIteratorChain = new IteratorChain<>();
        iterators.forEachRemaining(eIteratorChain::addIterator);
        return eIteratorChain;
    }

    /**
     * Returns an iterator containing only the elements that match the given predicate.
     * <p>
     * This method creates a new iterator that will iterate through elements from the
     * source iterator but only return elements that satisfy the specified predicate.
     * The filtering occurs during iteration and the method doesn't consume the source iterator
     * until the returned iterator is advanced.
     * <p>
     * Example usage:
     * <pre>
     * Iterator&lt;Integer&gt; numbers = Arrays.asList(1, 2, 3, 4, 5).iterator();
     * Predicate&lt;Integer&gt; isEven = n -> n % 2 == 0;
     * Iterator&lt;Integer&gt; evenNumbers = IteratorUtils.filter(numbers, isEven);
     * // evenNumbers will iterate through 2, 4
     * </pre>
     * <p>
     * The returned iterator supports {@link Iterator#remove()} if the source iterator supports it.
     *
     * @param <T> the type of objects in the iterator
     * @param itr the source iterator, must not be null
     * @param predicate the predicate to apply to each element, must not be null
     * @return a filtered iterator
     * @throws NullPointerException if either the iterator or predicate is null
     */
    public static <T> Iterator<T> filter(final Iterator<? extends T> itr, final Predicate<? super T> predicate) {
        return org.apache.commons.collections4.IteratorUtils.filteredIterator(itr, predicate::test);
    }

    /**
     * Returns an iterator that transforms the elements of another iterator.
     * <p>
     * This method creates a new iterator that will apply the given transformation
     * function to each element of the source iterator as the new iterator is traversed.
     * Transformations occur lazily during iteration and the source iterator is not
     * consumed until the returned iterator is advanced.
     * <p>
     * Example usage:
     * <pre>
     * Iterator&lt;Integer&gt; numbers = Arrays.asList(1, 2, 3).iterator();
     * Function&lt;Integer, String&gt; toString = n -> "Number: " + n;
     * Iterator&lt;String&gt; stringNumbers = IteratorUtils.transform(numbers, toString);
     * // stringNumbers will iterate through "Number: 1", "Number: 2", "Number: 3"
     * </pre>
     * <p>
     * The returned iterator supports {@link Iterator#remove()} if the source iterator
     * supports it.
     *
     * @param <F> the type of elements in the source iterator
     * @param <T> the type of elements in the transformed iterator
     * @param itr the source iterator to transform, must not be null
     * @param transform the function to transform the elements of the source iterator, must not be null
     * @return an iterator that transforms the elements of the source iterator
     * @throws NullPointerException if either the iterator or the transform function is null
     */
    public static <F, T> Iterator<T> transform(Iterator<? extends F> itr, final Function<? super F, ? extends T> transform) {
        return org.apache.commons.collections4.IteratorUtils.transformedIterator(itr, transform::apply);
    }

    /**
     * Creates an iterator that cycles indefinitely over the provided elements.
     * <p>
     * The returned iterator will continuously loop through the given elements in the same order.
     * If no elements are provided, the iterator will be empty.
     * <p>
     * Example usage:
     * <pre>
     * Iterator&lt;String&gt; cyclingIterator = IteratorUtils.cycle("a", "b", "c");
     * // Iterates: "a", "b", "c", "a", "b", "c", ...
     * </pre>
     *
     * @param <E> the type of elements in the iterator
     * @param elements the elements to cycle through, must not be null
     * @return an iterator that cycles indefinitely over the provided elements
     * @throws NullPointerException if the elements array is null
     */
    @SafeVarargs
    public static <E> Iterator<E> cycle(final E... elements) {
        Objects.requireNonNull(elements, "elements must not be null");
        return IteratorUtils.cycle(SetUtils.toLinkedSet(elements));
    }

    /**
     * Creates an iterator that cycles indefinitely over the elements of the given iterable.
     * <p>
     * The returned iterator will continuously loop through the elements of the iterable in the same order.
     * If the iterable is empty, the iterator will also be empty.
     * <p>
     * Example usage:
     * <pre>
     * List&lt;String&gt; list = Arrays.asList("a", "b", "c");
     * Iterator&lt;String&gt; cyclingIterator = IteratorUtils.cycle(list);
     * // Iterates: "a", "b", "c", "a", "b", "c", ...
     * </pre>
     *
     * @param <E> the type of elements in the iterable
     * @param iterable the iterable to cycle through, must not be null
     * @return an iterator that cycles indefinitely over the elements of the iterable
     * @throws NullPointerException if the iterable is null
     */
    public static <E> Iterator<E> cycle(final Iterable<E> iterable) {
        return org.apache.commons.collections4.IteratorUtils.loopingIterator(CollectionUtils.toCollection(iterable));
    }

    /**
     * Returns an iterator that partitions the elements of another iterator into fixed-size lists.
     * <p>
     * This method creates a new iterator that will group elements from the source iterator
     * into lists of the specified size. The final list may be smaller than the requested size
     * if there are not enough elements remaining in the source iterator.
     * <p>
     * The returned lists are unmodifiable. The source iterator is consumed only as the
     * returned iterator is advanced.
     * <p>
     * Example usage:
     * <pre>
     * Iterator&lt;Integer&gt; numbers = Arrays.asList(1, 2, 3, 4, 5).iterator();
     * Iterator&lt;List&lt;Integer&gt;&gt; partitioned = IteratorUtils.partition(numbers, 2);
     * // partitioned will iterate through [1, 2], [3, 4], [5]
     * </pre>
     *
     * @param <T> the type of elements in the source iterator
     * @param iterator the source iterator to partition, must not be null
     * @param size the size of each partition, must be greater than 0
     * @return an iterator of fixed-size lists containing the elements of the source iterator
     * @throws NullPointerException if the iterator is null
     * @throws IllegalArgumentException if size is less than or equal to 0
     */
    public static <T> Iterator<List<T>> partition(final Iterator<T> iterator, final int size) {

        Objects.requireNonNull(iterator, "Iterator must not be null.");
        Validate.checkArgument(size > 0, "Size must be greater than 0.");

        return UnmodifiableIterator.unmodifiableIterator(new Iterator<>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public List<T> next() {
                // check if there are elements left, throw an exception if not
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                final List<T> currentPartition = new ArrayList<>(size);
                for (int i = 0; i < size && iterator.hasNext(); i++) {
                    currentPartition.add(iterator.next());
                }
                return Collections.unmodifiableList(currentPartition);
            }
        });
    }

    /**
     * Returns an iterator that will only provide at most the first N elements from given iterator.
     * <p>
     * This method returns an iterator that will stop after returning the
     * specified number of elements or when the source iterator is exhausted,
     * whichever comes first.
     * <p>
     * Example usage:
     * <pre>
     * Iterator&lt;String&gt; names = Arrays.asList("Alice", "Bob", "Charlie", "David").iterator();
     * Iterator&lt;String&gt; firstTwo = IteratorUtils.limit(names, 2);
     * // firstTwo will iterate through "Alice", "Bob" only
     * </pre>
     *
     * @param <T> the type of elements in the iterator
     * @param iterator the source iterator to limit, must not be null
     * @param limit the maximum number of elements to return, must not be negative
     * @return an iterator limited to the specified number of elements
     * @throws NullPointerException if the iterator is null
     * @throws IllegalArgumentException if limit is negative
     */
    public static <T> Iterator<T> limit(final Iterator<T> iterator, final int limit) {
        Objects.requireNonNull(iterator);
        Validate.checkArgument(limit >= 0, "limit is negative");
        return org.apache.commons.collections4.IteratorUtils.boundedIterator(iterator, limit);
    }
}

