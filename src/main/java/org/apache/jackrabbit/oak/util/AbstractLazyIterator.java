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
package org.apache.jackrabbit.oak.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * {@code AbstractLazyIterator} provides basic iteration methods for a lazy loading iterator that does not support
 * remove. Implementing classes only need to implement the {@link #getNext()} method which must return the next item
 * in the iteration or {@code null} if the iteration as reached its end.
 */
public abstract class AbstractLazyIterator<T> implements Iterator<T> {

    private boolean fetchNext = true;

    private T next;

    protected AbstractLazyIterator() {
    }

    @Override
    public boolean hasNext() {
        if (fetchNext) {
            next = getNext();
            fetchNext = false;
        }
        return next != null;
    }

    @Override
    public T next() {
        if (fetchNext) {
            next = getNext();
        } else {
            fetchNext = true;
        }
        if (next == null) {
            throw new NoSuchElementException();
        }
        return next;
    }

    /**
     * Returns the next element of this iteration or {@code null} if the iteration has finished.
     * @return the next element.
     */
    abstract protected T getNext();

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}