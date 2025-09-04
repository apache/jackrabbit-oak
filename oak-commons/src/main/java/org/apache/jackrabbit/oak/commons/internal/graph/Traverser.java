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
package org.apache.jackrabbit.oak.commons.internal.graph;

import org.apache.commons.collections4.FluentIterable;
import org.apache.commons.collections4.iterators.UnmodifiableIterator;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
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

        Objects.requireNonNull(root, "root must not be null");
        Objects.requireNonNull(childExtractor, "Children extractor function must not be null");

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

        Objects.requireNonNull(root, "root must not be null");
        Objects.requireNonNull(childExtractor, "Children extractor function must not be null");

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

    /**
     * Returns an iterator that traverses a tree structure in post-order. Null nodes are strictly forbidden.
     * <p>
     * In post-order traversal, all children of a node are visited from left to right, followed by
     * the node itself. This creates a bottom-up traversal pattern where leaf nodes are visited first,
     * then their parents, and finally the root.
     *
     * @param <T> the type of value in the tree nodes
     * @param root the root node of the tree, may be null
     * @param childExtractor function to extract children from a node, must not be null
     * @return an iterator that traverses the tree in post-order
     * @throws NullPointerException if childExtractor or any child is null
     */
    public static <T> FluentIterable<T> postOrderTraversal(final T root, final Function<T, Iterable<? extends T>> childExtractor) {
        Objects.requireNonNull(root, "root must not be null");
        Objects.requireNonNull(childExtractor, "Children extractor function must not be null");

        return FluentIterable.of(new Iterable<>() {
            @Override
            public @NotNull Iterator<T> iterator() {
                return UnmodifiableIterator.unmodifiableIterator(new PostOrderIterator<>(root, childExtractor));
            }
        });
    }

    private static final class PostOrderIterator<T> implements Iterator<T> {

        private final Deque<PostOrderNode<T>> stack;
        private final Function<T, Iterable<? extends T>> childExtractor;

        PostOrderIterator(final T root, final Function<T, Iterable<? extends T>> childExtractor) {
            this.childExtractor = childExtractor;
            this.stack = new ArrayDeque<>();
            // Start by pushing the leftmost path to initialize
            pushChildren(root, childExtractor);
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

            while (!stack.isEmpty()) {
                final PostOrderNode<T> frame = stack.getLast();

                // If there are more children, process the next one first
                if (frame.childIterator.hasNext()) {
                    // throw NPE if any child is null
                    T nextChild = frame.childIterator.next();
                    Objects.requireNonNull(nextChild, "Child nodes must not be null");

                    pushChildren(nextChild, childExtractor);
                    continue; // Continue the loop to get the next leaf
                }

                // No more children, remove this node (post-order behavior)
                stack.removeLast();
                return frame.node;
            }

            throw new AssertionError("Should not reach here");
        }

        private void pushChildren(final T node, final Function<T, Iterable<? extends T>> childExtractor) {

            // Iteratively go down the leftmost path
            T current = node;
            Objects.requireNonNull(node, "node must not be null");
            while (current != null) {
                Iterator<? extends T> childItr = childExtractor.apply(current).iterator();
                stack.addLast(new PostOrderNode<>(current, childItr));

                // Move to the leftmost child if available
                // Check for null child IMMEDIATELY when retrieving it
                if (childItr.hasNext()) {
                    T child = childItr.next();
                    // This validation needs to happen here, before any other processing
                    current = Objects.requireNonNull(child, "Child nodes must not be null");
                } else {
                    current = null;
                }
            }
        }
    }

    private static class PostOrderNode<T> {
        final T node;
        final Iterator<? extends T> childIterator;

        PostOrderNode(final T node, final Iterator<? extends T> childItr) {
            this.node = node;
            this.childIterator = childItr;
        }
    }
}

