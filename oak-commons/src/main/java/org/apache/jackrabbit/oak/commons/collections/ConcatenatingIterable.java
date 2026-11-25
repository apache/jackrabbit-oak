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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/** Lazy concatenation views with direct traversal of two-input chains. */
final class ConcatenatingIterable {
    private ConcatenatingIterable() {
    }

    static <E> Iterable<E> of(Iterable<? extends Iterable<? extends E>> sources) {
        return new Sources<>(sources);
    }

    static <E> Iterable<E> ofList(List<? extends Iterable<? extends E>> sources) {
        return new Sources<E>(sources) {
            @Override
            public String toString() {
                return IterableUtils.toString(this);
            }
        };
    }

    static <E> Iterable<E> pair(Iterable<? extends E> first, Iterable<? extends E> second) {
        return new Pair<>(first, second);
    }

    private static class Sources<E> implements Iterable<E> {
        private final Iterable<? extends Iterable<? extends E>> sources;

        private Sources(Iterable<? extends Iterable<? extends E>> sources) {
            this.sources = Objects.requireNonNull(sources);
        }

        @Override
        public Iterator<E> iterator() {
            return new SourceIterator<>(sources.iterator());
        }
    }

    private static final class Pair<E> implements Iterable<E> {
        private final Iterable<? extends E> first;
        private final Iterable<? extends E> second;

        private Pair(Iterable<? extends E> first, Iterable<? extends E> second) {
            this.first = Objects.requireNonNull(first);
            this.second = Objects.requireNonNull(second);
        }

        @Override
        public Iterator<E> iterator() {
            return new ConcatenatingIterator<>(new PairIterator<>(first, second));
        }

        @Override
        public String toString() {
            return IterableUtils.toString(this);
        }
    }

    private static final class PairIterator<E> implements Iterator<Iterator<? extends E>> {
        private Iterable<? extends E> current;
        private Iterable<? extends E> tail;
        private Deque<Iterable<? extends E>> parents;

        private PairIterator(Iterable<? extends E> first, Iterable<? extends E> second) {
            current = first;
            tail = second;
        }

        @Override
        public boolean hasNext() {
            while (current instanceof Pair) {
                @SuppressWarnings("unchecked")
                Pair<E> nested = (Pair<E>) current;
                if (tail != null) {
                    if (parents == null) {
                        parents = new ArrayDeque<>();
                    }
                    parents.addLast(tail);
                }
                current = nested.first;
                tail = nested.second;
            }
            return current != null;
        }

        @Override
        public Iterator<? extends E> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Iterable<? extends E> iterable = current;
            current = tail;
            tail = parents == null || parents.isEmpty() ? null : parents.removeLast();
            return iterable.iterator();
        }
    }

    private static final class SourceIterator<E> extends ConcatenatingIterator<E> {
        private Iterator<? extends Iterable<? extends E>> source;
        private Deque<Iterator<? extends Iterable<? extends E>>> parents;

        private SourceIterator(Iterator<? extends Iterable<? extends E>> source) {
            this.source = source;
        }

        @Override
        Iterator<? extends E> nextInput() {
            while (source != null) {
                if (source.hasNext()) {
                    Iterable<? extends E> iterable = Objects.requireNonNull(source.next());
                    if (iterable instanceof Sources) {
                        @SuppressWarnings("unchecked")
                        Sources<E> nested = (Sources<E>) iterable;
                        // Open the child before saving the parent so a failed open leaves the path intact.
                        Iterator<? extends Iterable<? extends E>> child = Objects.requireNonNull(nested.sources.iterator());
                        if (parents == null) {
                            parents = new ArrayDeque<>();
                        }
                        parents.addLast(source);
                        source = child;
                    } else {
                        return Objects.requireNonNull(iterable.iterator());
                    }
                } else {
                    source = parents == null || parents.isEmpty() ? null : parents.removeLast();
                }
            }
            return null;
        }
    }
}
