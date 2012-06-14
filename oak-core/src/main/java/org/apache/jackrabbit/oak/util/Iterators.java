/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.jackrabbit.oak.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;

import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.commons.collections.iterators.FilterIterator;
import org.apache.commons.collections.iterators.IteratorChain;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.commons.collections.iterators.TransformIterator;

/**
 * Utility class containing type safe adapters for some of the iterators of
 * commons-collections.
 */
public final class Iterators {

    private Iterators() { }

    /**
     * Returns an iterator containing the single element {@code element} of
     * type {@code T}.
     *
     * @param <T>
     * @param element
     * @return
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> Iterator<T> singleton(T element) {
        return new SingletonIterator(element);
    }

    /**
     * Returns an empty iterator of type {@code T}.
     *
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> Iterator<T> empty() {
        return EmptyIterator.INSTANCE;
    }

    /**
     * Returns an iterator for the concatenation of {@code iterator1} and
     * {@code iterator2}.
     *
     * @param <T>
     * @param iterator1
     * @param iterator2
     * @return
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> Iterator<T> chain(Iterator<? extends T> iterator1, Iterator<? extends T> iterator2) {
        return new IteratorChain(iterator1, iterator2);
    }

    /**
     * Returns an iterator for the concatenation of all the given {@code iterators}.
     *
     * @param <T>
     * @param iterators
     * @return
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> Iterator<T> chain(Iterator<? extends T>[] iterators) {
        return new IteratorChain(iterators);
    }

    /**
     * Returns an iterator for the concatenation of all the given {@code iterators}.
     *
     * @param <T>
     * @param iterators
     * @return
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> Iterator<T> chain(Collection<? extends T> iterators) {
        return new IteratorChain(iterators);
    }

    /**
     * Returns an iterator for elements of an array of {@code values}.
     *
     * @param <T>
     * @param values  the array to iterate over.
     * @param from  the index to start iterating at.
     * @param to  the index to finish iterating at.
     * @return
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> Iterator<T> arrayIterator(T[] values, int from, int to) {
        return new ArrayIterator(values, from, to);
    }

    /**
     * Returns an iterator with elements from an original {@code iterator} where the
     * given {@code predicate} matches removed.
     *
     * @param <T>
     * @param iterator
     * @param predicate
     * @return
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T> Iterator<T> filter(Iterator<? extends T> iterator,
            final Predicate<? super T> predicate) {

        return new FilterIterator(iterator, new org.apache.commons.collections.Predicate() {
            @Override
            public boolean evaluate(Object object) {
                return predicate.evaluate((T) object);
            }
        });
    }

    /**
     * Returns an iterator with elements of an original  {@code iterator} mapped by
     * a {@code f}.
     *
     * @param <T>
     * @param <R>
     * @param <S>
     * @param iterator
     * @param f
     * @return
     */
    @SuppressWarnings("unchecked")
    @Nonnull
    public static <T, R, S extends T> Iterator<R> map(Iterator<? extends T> iterator,
            final Function1<S, ? super R> f) {

        return new TransformIterator(iterator, new org.apache.commons.collections.Transformer() {
            @Override
            public Object transform(Object input) {
                return f.apply((S) input);
            }
        });
    }

    /**
     * Spool an iterator into an iterable
     * @param iterator
     * @param <T>
     * @return iterable containing the values from {@code iterator}
     */
    @Nonnull
    public static <T> Iterable<T> toIterable(final Iterator<T> iterator) {
        return new Iterable<T>() {
            private List<T> copy;

            @Override
            public Iterator<T> iterator() {
                if (copy == null) {
                    copy = spool(iterator);
                }
                return copy.iterator();
            }

            private List<T> spool(Iterator<T> iterator) {
                List<T> list = new ArrayList<T>();
                while (iterator.hasNext()) {
                    list.add(iterator.next());
                }
                return list;
            }
        };
    }

    /**
     * Flattens an iterator of iterators into a single iterator.
     * @param iterators
     * @param <T>
     * @return
     */
    @Nonnull
    public static <T> Iterator<T> flatten(final Iterator<Iterator<? extends T>> iterators) {
        return new Iterator<T>() {
            private Iterator<? extends T> current;

            @Override
            public boolean hasNext() {
                if (current != null && current.hasNext()) {
                    return true;
                }
                else if (!iterators.hasNext()) {
                    return false;
                }
                else {
                    do {
                        current = iterators.next();
                    } while (!current.hasNext() && iterators.hasNext());
                    return current.hasNext();
                }
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                return current.next();
            }

            @Override
            public void remove() {
                if (current == null) {
                    throw new IllegalStateException();
                }

                current.remove();
            }
        };
    }

    /**
     * Spools the values of an iterator into a list.
     * @param values  the values to spool
     * @param list  the target list to receive the values
     * @param <T>
     * @return  {@code list}
     */
    @Nonnull
    public static <T> List<T> toList(Iterable<? extends T> values, List<T> list) {
        for (T value : values) {
            list.add(value);
        }
        return list;
    }

}
