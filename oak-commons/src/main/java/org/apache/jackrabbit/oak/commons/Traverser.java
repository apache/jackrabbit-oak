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
package org.apache.jackrabbit.oak.commons;

import org.apache.commons.collections4.FluentIterable;
import org.apache.commons.collections4.iterators.UnmodifiableIterator;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

public class Traverser {

    private Traverser() {
        // no instances for you
    }

    /**
     * Returns an iterator that traverses a tree structure in pre-order. Null nodes are strictly forbidden.
     * <p>
     * In pre-order traversal, the current node is visited first, followed by its children
     * from left to right. This method creates an iterator that produces tree nodes in this order.
     *
     * @param <T> the type of value in the tree nodes
     * @param root the root node of the tree, may be null
     * @param childExtractor function to extract children from a node, must not be null
     * @return an iterator that traverses the tree in pre-order
     * @throws NullPointerException if childExtractor or any child is null
     */
    @NotNull
    public static <T> FluentIterable<T> preOrderTraversal(final T root, final @NotNull Function<T, Iterable<? extends T>> childExtractor) {

        Objects.requireNonNull(childExtractor, "Children extractor function must not be null");

        if (root == null) {
            return FluentIterable.empty();
        }

        return FluentIterable.of(new Iterable<>() {
            @Override
            public @NotNull Iterator<T> iterator() {
                return UnmodifiableIterator.unmodifiableIterator(new PreOrderIterator<>(root, childExtractor));
            }
        });
    }

    private static final class PreOrderIterator<T> implements Iterator<T> {

        private final Deque<Iterator<? extends T>> stack;
        private final Function<T, Iterable<? extends T>> childExtractor;

        public PreOrderIterator(final T root, final Function<T, Iterable<? extends T>> childExtractor) {
            this.childExtractor = childExtractor;
            this.stack = new ArrayDeque<>();
            // add first element during initialization
            stack.addLast(Collections.singletonList(root).iterator());
        }

        @Override
        public boolean hasNext() {
            // Remove any empty iterators from the top of the stack
            while (!stack.isEmpty() && !stack.peek().hasNext()) {
                stack.pop();
            }
            return !stack.isEmpty();
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more nodes in the tree");
            }

            // Get next element from the current iterator
            T current = stack.peek().next();

            // Push the iterator for children onto the stack
            // Children added later will be processed first in pre-order traversal
            Iterator<? extends T> childIter = childExtractor.apply(current).iterator();
            if (childIter.hasNext()) {
                stack.push(childIter);
            }
            return current;
        }
    }

    /**
     * Returns an iterator that traverses a tree structure in breadth-first order.
     * Null nodes are strictly forbidden.
     * <p>
     * In breadth-first traversal, all sibling nodes at a given level are visited before any nodes
     * at the next level. This creates a level-by-level traversal pattern, starting from the root
     * and moving downward through the tree.
     *
     * @param <T> the type of value in the tree nodes
     * @param root the root node of the tree, may be null
     * @param childExtractor function to extract children from a node, must not be null
     * @return a fluent iterable that traverses the tree in breadth-first order
     * @throws NullPointerException if childExtractor or any child is null
     */
    @NotNull
    public static <T> FluentIterable<T> breadthFirstTraversal(final T root, final @NotNull Function<T, Iterable<? extends T>> childExtractor) {
        Objects.requireNonNull(childExtractor, "Children extractor function must not be null");

        if (root == null) {
            return FluentIterable.empty();
        }

        return FluentIterable.of(new Iterable<>() {
            @Override
            public @NotNull Iterator<T> iterator() {
                return UnmodifiableIterator.unmodifiableIterator(new BreadthFirstIterator<>(root, childExtractor));
            }
        });
    }

    private static final class BreadthFirstIterator<T> implements Iterator<T> {

        private final Deque<T> queue;
        private final Function<T, Iterable<? extends T>> childExtractor;

        public BreadthFirstIterator(final T root, final Function<T, Iterable<? extends T>> childExtractor) {
            this.queue = new ArrayDeque<>();
            this.childExtractor = childExtractor;
            this.queue.add(root);
        }

        @Override
        public boolean hasNext() {
            return !queue.isEmpty();
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more nodes in the tree");
            }

            final T current = queue.removeFirst();

            // Add all children to the queue (in order)
            for (T child : childExtractor.apply(current)) {
                // would throw NPE if the child is null
                queue.addLast(child);
            }
            return current;
        }
    }
}

