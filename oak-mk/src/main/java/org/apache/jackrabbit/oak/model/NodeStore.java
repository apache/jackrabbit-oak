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
package org.apache.jackrabbit.oak.model;

/**
 * Storage abstraction for content trees. At any given point in time
 * the stored content tree is rooted at a single immutable node state.
 * Changes in the tree are constructed using {@link NodeBuilder} instances
 * based on the root and other node states in the tree. The state of the
 * entire tree can then be changed by setting the resulting modified root
 * node state as the new root of the tree.
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
     * Updates the state of the content tree.
     *
     * @param newRoot new root node state
     */
    void setRoot(NodeState newRoot);

    /**
     * Returns a builder for constructing a new or modified node state.
     * The builder is initialized with all the properties and child nodes
     * from the given base node state, or with no properties or child nodes
     * if no base node state is given.
     *
     * @param base base node state,
     *             or <code>null</code> to construct a new node state
     * @return builder instance
     */
    NodeBuilder getNodeBuilder(NodeState base);

}
