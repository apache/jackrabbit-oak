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
package org.apache.jackrabbit.oak.commons.collections;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Reuses the active leaf and a lazily allocated path through nested chains.
 * Child access invalidates its owner's path; ownership generations preserve
 * removal targets without updating every ancestor after each element.
 */
final class ConcatenatingIterator<E> implements Iterator<E> {
    private final Iterator<? extends Iterator<? extends E>> inputs;
    private Deque<ConcatenatingIterator<E>> path;
    // Borrowed children have no ready cache, so direct access detaches them first.
    private Iterator<? extends E> ready;
    private Iterator<? extends E> current = Collections.emptyIterator();
    private Iterator<? extends E> lastReturned;
    private ConcatenatingIterator<E> owner;
    private long ownerGeneration;
    private long generation;
    private boolean invalidated;

    ConcatenatingIterator(Iterator<? extends Iterator<? extends E>> inputs) {
        this.inputs = Objects.requireNonNull(inputs);
    }

    @Override
    public boolean hasNext() {
        return nextIterator() != null;
    }

    @Override
    public E next() {
        Iterator<? extends E> iterator = nextIterator();
        if (iterator == null) {
            throw new NoSuchElementException();
        }
        E value = iterator.next();
        lastReturned = iterator;
        generation++;
        return value;
    }

    @Override
    public void remove() {
        detach();
        if (lastReturned == null) {
            throw new IllegalStateException("next() must precede remove()");
        }
        lastReturned.remove();
        lastReturned = null;
    }

    private void detach() {
        if (owner != null) {
            owner.invalidated = true;
            owner.ready = null;
            release();
        }
    }

    private void release() {
        if (owner.generation != ownerGeneration) {
            lastReturned = owner.lastReturned;
        }
        owner = null;
    }

    private void clearPath() {
        ready = null;
        if (path != null) {
            while (!path.isEmpty()) {
                ConcatenatingIterator<E> child = path.removeLast();
                if (child.owner == this) {
                    child.release();
                }
            }
        }
    }

    private Iterator<? extends E> nextIterator() {
        Iterator<? extends E> iterator = ready;
        if (iterator != null) {
            if (iterator.hasNext()) {
                return iterator;
            }
            if (ready == iterator) {
                ConcatenatingIterator<E> chain = path == null || path.isEmpty() ? this : path.peekLast();
                chain.current = Collections.emptyIterator();
            }
        }
        return findNext();
    }

    private Iterator<? extends E> findNext() {
        ready = null;
        detach();
        if (invalidated) {
            clearPath();
            invalidated = false;
        }
        ConcatenatingIterator<E> chain = path == null || path.isEmpty() ? this : path.peekLast();
        while (true) {
            if (chain.current instanceof ConcatenatingIterator) {
                @SuppressWarnings("unchecked")
                ConcatenatingIterator<E> nested = (ConcatenatingIterator<E>) chain.current;
                nested.detach();
                nested.clearPath();
                nested.invalidated = false;
                nested.owner = this;
                nested.ownerGeneration = generation;
                if (path == null) {
                    path = new ArrayDeque<>();
                }
                if (path.isEmpty()) {
                    path.addLast(this);
                }
                path.addLast(nested);
                chain = nested;
            } else if (chain.current.hasNext()) {
                if (!invalidated) {
                    ready = chain.current;
                }
                return chain.current;
            } else if (chain.inputs.hasNext()) {
                chain.current = Objects.requireNonNull(chain.inputs.next());
            } else if (chain == this) {
                return null;
            } else {
                path.removeLast();
                if (chain.owner == this) {
                    // Preserve removal targets on leaving a child, rather than
                    // updating every ancestor after each returned element.
                    chain.release();
                }
                chain = path.peekLast();
                chain.current = Collections.emptyIterator();
            }
        }
    }
}
