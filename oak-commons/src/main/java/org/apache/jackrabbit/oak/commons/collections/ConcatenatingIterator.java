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

import org.apache.commons.collections4.iterators.LazyIteratorChain;
import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;

/**
 * Traverses nested Oak concatenations without recursive iterator calls.
 * Set {@code -DFT_OAK-12558=false} at startup to use the previous implementation.
 */
final class ConcatenatingIterator<E> implements Iterator<E> {
    private static final boolean ENABLED = SystemPropertySupplier.create("FT_OAK-12558", true).get();

    private final Iterator<? extends Iterator<? extends E>> inputs;
    private final Deque<ConcatenatingIterator<E>> path = new ArrayDeque<>();
    private Iterator<? extends E> current = Collections.emptyIterator();
    private Iterator<? extends E> lastReturned;

    private ConcatenatingIterator(Iterator<? extends Iterator<? extends E>> inputs) {
        this.inputs = inputs;
    }

    static <E> Iterator<E> create(Iterator<? extends Iterator<? extends E>> inputs) {
        Objects.requireNonNull(inputs);
        return ENABLED ? new ConcatenatingIterator<>(inputs) : legacyIterator(inputs);
    }

    static <E> Iterator<E> legacyIterator(Iterator<? extends Iterator<? extends E>> inputs) {
        return new LazyIteratorChain<>() {
            @Override
            protected Iterator<? extends E> nextIterator(int count) {
                return inputs.hasNext() ? inputs.next() : null;
            }
        };
    }

    @Override
    public boolean hasNext() {
        try {
            return findNext() != null;
        } finally {
            path.clear();
        }
    }

    @Override
    public E next() {
        try {
            Iterator<? extends E> iterator = findNext();
            if (iterator == null) {
                throw new NoSuchElementException();
            }
            E value = iterator.next();
            for (ConcatenatingIterator<E> chain : path) {
                chain.lastReturned = iterator;
            }
            return value;
        } finally {
            path.clear();
        }
    }

    @Override
    public void remove() {
        if (lastReturned == null) {
            throw new IllegalStateException("next() must precede remove()");
        }
        lastReturned.remove();
        lastReturned = null;
    }

    private Iterator<? extends E> findNext() {
        path.addLast(this);
        while (!path.isEmpty()) {
            ConcatenatingIterator<E> chain = path.peekLast();
            if (chain.current instanceof ConcatenatingIterator) {
                @SuppressWarnings("unchecked")
                ConcatenatingIterator<E> nested = (ConcatenatingIterator<E>) chain.current;
                // Walk the child state in place: draining its pending inputs would
                // discard elements when the caller continues using that child.
                path.addLast(nested);
            } else if (chain.current.hasNext()) {
                return chain.current;
            } else if (chain.inputs.hasNext()) {
                chain.current = Objects.requireNonNull(chain.inputs.next());
            } else {
                path.removeLast();
                if (!path.isEmpty()) {
                    path.peekLast().current = Collections.emptyIterator();
                }
            }
        }
        return null;
    }
}
