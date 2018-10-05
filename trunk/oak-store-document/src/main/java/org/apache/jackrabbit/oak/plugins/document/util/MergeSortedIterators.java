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
package org.apache.jackrabbit.oak.plugins.document.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 * <code>MergeSortedIterators</code> is a specialized implementation of a
 * merge sort of already sorted iterators of some type of comparable elements.
 * The input iterators must return the elements in sorted order according to
 * the provided Comparator. In addition the sequence of iterators must also
 * be sorted in a way that the first element of the next iterator is greater
 * than the first element of the previous iterator.
 *
 * @param <T> the entry type
 */
public abstract class MergeSortedIterators<T> implements Iterator<T> {

    private final List<PeekingIterator<T>> iterators = new ArrayList<PeekingIterator<T>>();
    private final Comparator<T> comparator;
    private T lastPeek;

    public MergeSortedIterators(final Comparator<T> comparator) {
        this.comparator = comparator;
        fetchNextIterator();
    }

    /**
     * @return the next {@link Iterator} or <code>null</code> if there is none.
     */
    public abstract Iterator<T> nextIterator();

    /**
     * Provides details about this iterator
     */
    public String description() {
        return "";
    }

    //----------------------------< Iterator >----------------------------------

    @Override
    public boolean hasNext() {
        return !iterators.isEmpty();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        PeekingIterator<T> it = iterators.get(0);
        T next = it.next();
        // more elements?
        if (it.hasNext()) {
            adjustFirst();
        } else {
            // remove from list of iterators
            iterators.remove(0);
        }
        // fetch next iterator?
        if (comparator.compare(next, lastPeek) >= 0) {
            fetchNextIterator();
        }
        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    //----------------------------< internal >----------------------------------

    private void fetchNextIterator() {
        Iterator<T> it = nextIterator();
        if (it != null && it.hasNext()) {
            PeekingIterator<T> pIt = Iterators.peekingIterator(it);
            if (!iterators.isEmpty()
                    &&  comparator.compare(pIt.peek(), lastPeek) < 0) {
                throw new IllegalStateException(description() + 
                        " First element of next iterator (" + pIt.peek() + ")" +
                        " must be after previous iterator (" + lastPeek + ")");
            }
            lastPeek = pIt.peek();
            iterators.add(pIt);
            adjustLast();
        }
    }

    private void adjustFirst() {
        // shift first iterator until peeked elements are sorted again
        int i = 0;
        while (i + 1 < iterators.size()) {
            if (comparator.compare(iterators.get(i).peek(), iterators.get(i + 1).peek()) > 0) {
                Collections.swap(iterators, i, i + 1);
                i++;
            } else {
                break;
            }
        }
    }

    private void adjustLast() {
        // shift last until sorted again
        int i = iterators.size() - 1;
        while (i - 1 >= 0) {
            if (comparator.compare(iterators.get(i - 1).peek(), iterators.get(i).peek()) > 0) {
                Collections.swap(iterators, i, i - 1);
                i--;
            } else {
                break;
            }
        }
    }
}
