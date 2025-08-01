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
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;

public class TreeTraverser {

    private TreeTraverser() {
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
    public static <T> FluentIterable<T> preOrderTraversal(final T root, final @NotNull Function<T, Iterable<T>> childExtractor) {

        Objects.requireNonNull(childExtractor, "Children extractor function must not be null");

        if (root == null) {
            return FluentIterable.empty();
        }

        return FluentIterable.of(new Iterable<>() {
            @Override
            public @NotNull Iterator<T> iterator() {
                return new Iterator<>() {
                    private final Deque<T> stack = new ArrayDeque<>();

                    {
                        // add first element during initialization
                        stack.push(root);
                    }

                    @Override
                    public boolean hasNext() {
                        return !stack.isEmpty();
                    }

                    @Override
                    public T next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException("No more nodes in the tree");
                        }

                        final T current = stack.pop();

                        // Push children in reverse order so they're popped in correct order
                        List<T> children = new ArrayList<>();
                        // NPE if the current is null
                        childExtractor.apply(current).forEach(children::add);

                        for (int i = children.size() - 1; i >= 0; i--) {
                            // NPE if the child is null
                            stack.push(children.get(i));
                        }
                        return current;
                    }
                };
            }
        });
    }

    /**
     * Returns an iterator that traverses a tree structure in breadth-first order.
     * Null nodes are strictly forbidden.
     * <p>
     * In breadth-first traversal, all nodes at a given level are visited before any nodes
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
    public static <T> FluentIterable<T> breadthFirstTraversal(final T root, final @NotNull Function<T, Iterable<T>> childExtractor) {
        Objects.requireNonNull(childExtractor, "Children extractor function must not be null");

        if (root == null) {
            return FluentIterable.empty();
        }

        return FluentIterable.of(new Iterable<>() {
            @Override
            public @NotNull Iterator<T> iterator() {
                return new Iterator<>() {
                    private final Deque<T> queue = new ArrayDeque<>();

                    {
                        // add first element during initialization
                        queue.addLast(root);
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
                };
            }
        });
    }
}
