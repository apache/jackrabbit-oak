/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.model;

/**
 * Storage abstraction for content trees. At any given point in time
 * the stored content tree is rooted at a single immutable node state.
 * Changes in the tree are constructed by branching off a private copy
 * using the {@link #branch(NodeState)} method which can be modified
 * and merged back using the {@link #merge(NodeStateEditor, NodeState)}
 * method.
 * <p>
 * This is a low-level interface that doesn't cover functionality like
 * merging concurrent changes or rejecting new tree states based on some
 * higher-level consistency constraints.
 */
public interface NodeStore {

    /**
     * Returns the latest state of the content tree.
     *
     * @return root node state
     */
    NodeState getRoot();

    /**
     * Creates a private branch from a {@code base} node state
     * for editing. The branch can later be merged back into
     * the node store using the {@link #merge(NodeStateEditor, NodeState) merge}
     * method.
     *
     * @param base base node state
     * @return a private branch rooted at {@code base}
     */
    NodeStateEditor branch(NodeState base);

    /**
     * Atomically merges the changes from {@code branch} back
     * into the sub-tree rooted at {@code base}.
     *
     * @param branch branch to merge into {@code base}
     * @param base base of the sub-tree for merging
     * @return result of the merge operation: the new node state of the
     *         sub tree rooted at {@code base}.
     */
    NodeState merge(NodeStateEditor branch, NodeState base);

}
