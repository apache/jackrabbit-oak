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
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/** Opens nested Oak iterable views directly, without creating intermediate chains. */
final class ConcatenatingIterable<E> implements Iterable<E> {
    private final Iterable<? extends Iterable<? extends E>> sources;

    ConcatenatingIterable(Iterable<? extends Iterable<? extends E>> sources) {
        this.sources = Objects.requireNonNull(sources);
    }

    @Override
    public Iterator<E> iterator() {
        return new ConcatenatingIterator<>(new SourceIterator<>(sources.iterator()));
    }

    private static final class SourceIterator<E> implements Iterator<Iterator<? extends E>> {
        private Iterator<? extends Iterable<? extends E>> current;
        private Deque<Iterator<? extends Iterable<? extends E>>> parents;
        private Iterable<? extends E> next;

        private SourceIterator(Iterator<? extends Iterable<? extends E>> current) {
            this.current = current;
        }

        @Override
        public boolean hasNext() {
            while (next == null) {
                if (current.hasNext()) {
                    Iterable<? extends E> iterable = Objects.requireNonNull(current.next());
                    if (iterable instanceof ConcatenatingIterable) {
                        @SuppressWarnings("unchecked")
                        ConcatenatingIterable<E> nested = (ConcatenatingIterable<E>) iterable;
                        if (parents == null) {
                            parents = new ArrayDeque<>();
                        }
                        parents.addLast(current);
                        current = nested.sources.iterator();
                    } else {
                        next = iterable;
                    }
                } else if (parents != null && !parents.isEmpty()) {
                    current = parents.removeLast();
                } else {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Iterator<? extends E> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Iterable<? extends E> iterable = next;
            next = null;
            return iterable.iterator();
        }
    }
}
