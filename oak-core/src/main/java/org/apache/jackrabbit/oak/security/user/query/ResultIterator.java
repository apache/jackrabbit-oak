/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.security.user.query;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implements a query result iterator which only returns a maximum number of
 * element from an underlying iterator starting at a given offset.
 *
 * @param <T> element type of the query results
 */
public final class ResultIterator<T> implements Iterator<T> {

    public static final int OFFSET_NONE = 0;
    public static final int MAX_ALL = -1;

    private final Iterator<T> iterator;
    private final long offset;
    private final long max;
    private int pos;
    private T next;

    /**
     * Create a new {@code ResultIterator} with a given offset and maximum
     *
     * @param offset Offset to start iteration at. Must be non negative
     * @param max Maximum elements this iterator should return.
     * Set to {@link #MAX_ALL} for all results.
     * @param iterator the underlying iterator
     * @throws IllegalArgumentException if offset is negative
     */
    private ResultIterator(long offset, long max, Iterator<T> iterator) {
        if (offset < OFFSET_NONE) {
            throw new IllegalArgumentException("Offset must not be negative");
        }
        this.iterator = iterator;
        this.offset = offset;
        this.max = max;
    }

    /**
     * Returns an iterator respecting the specified {@code offset} and {@code max}.
     *
     * @param offset   offset to start iteration at. Must be non negative
     * @param max      maximum elements this iterator should return. Set to
     * {@link #MAX_ALL} for all
     * @param iterator the underlying iterator
     * @param <T>      element type
     * @return an iterator which only returns the elements in the given bounds
     */
    public static <T> Iterator<T> create(long offset, long max, Iterator<T> iterator) {
        if (offset == OFFSET_NONE && max == MAX_ALL) {
            // no constraints on offset nor max -> return the original iterator.
            return iterator;
        } else {
            return new ResultIterator<T>(offset, max, iterator);
        }
    }

    //-----------------------------------------------------------< Iterator >---
    @Override
    public boolean hasNext() {
        if (next == null) {
            fetchNext();
        }
        return next != null;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return consumeNext();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    //------------------------------------------------------------< private >---

    private void fetchNext() {
        for (; pos < offset && iterator.hasNext(); pos++) {
            next = iterator.next();
        }

        if (pos < offset || !iterator.hasNext() || max >= 0 && pos - offset + 1 > max) {
            next = null;
        } else {
            next = iterator.next();
            pos++;
        }
    }

    private T consumeNext() {
        T element = next;
        next = null;
        return element;
    }
}