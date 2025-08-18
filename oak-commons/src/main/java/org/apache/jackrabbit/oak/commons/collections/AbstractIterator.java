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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An abstract base class for implementing iterators with custom logic.
 * Subclasses should implement {@link #computeNext()} to provide elements and
 * must call {@link #endOfData()} to signal no more element.
 * <p>
 * The iteration ends when {@link #endOfData()} is called from {@code computeNext()}.
 *
 * @param <T> the type of elements returned by this iterator
 */
public abstract class AbstractIterator<T> implements Iterator<T> {

    protected AbstractIterator() {}

    private enum State { READY, NOT_READY, DONE, FAILED }
    private State state = State.NOT_READY;
    private T next;


    /**
     * Computes the next element in the iteration.
     * Subclasses must implement this method and call {@link #endOfData()} when no more elements are available.
     *
     * @return the next element, or {@link #endOfData()} if iteration is over
     */
    protected abstract T computeNext();

    /**
     * Signals that the iteration has no more elements.
     * Should be called from {@link #computeNext()} to end iteration.
     *
     * @return null, indicating end of data
     */
    protected final T endOfData() {
        state = State.DONE;
        return null;
    }

    @Override
    public boolean hasNext() {
        switch (state) {
            case READY:
                return true;
            case DONE:
                return false;
            case NOT_READY:
                return tryToComputeNext();
            default:
                throw new IllegalStateException("Iterator in failed state");
        }
    }

    @Override
    public T next() {
        if (!hasNext()) throw new NoSuchElementException();
        state = State.NOT_READY;
        T result = next;
        next = null; // Release reference for GC
        return result;
    }

    private boolean tryToComputeNext() {
        state = State.FAILED;
        next = computeNext();
        if (state == State.DONE) {
            return false;
        } else {
            state = State.READY;
            return true;
        }
    }
}
