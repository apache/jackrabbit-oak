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
package org.apache.jackrabbit.oak.spi.state;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValueFactory;

/**
 * Storage abstraction for trees. At any given point in time the stored
 * tree is rooted at a single immutable node state.
 * <p>
 * This is a low-level interface that doesn't cover functionality like
 * merging concurrent changes or rejecting new tree states based on some
 * higher-level consistency constraints.
 */
public interface NodeStore {

    /**
     * Returns the latest state of the tree.
     *
     * @return root node state
     */
    NodeState getRoot();

    /**
     * Returns a builder for constructing a new or modified node state.
     * The builder is initialized with all the properties and child nodes
     * from the given base node state.
     *
     * @param base  base node state, or {@code null} for building new nodes
     * @return  builder instance
     */
    NodeStateBuilder getBuilder(NodeState base);

    /**
     * Returns the factory for creating values used for building node states.
     *
     * @return value factory
     */
    CoreValueFactory getValueFactory();

    /**
     * Updates the state of the content tree.
     *
     * @param newRoot new root node state
     */
    void setRoot(NodeState newRoot) throws CommitFailedException;

    /**
     * Compares the given two node states. Any found differences are
     * reported by calling the relevant added, changed or deleted methods
     * of the given handler.
     *
     * @param before node state before changes
     * @param after node state after changes
     * @param diff handler of node state differences
     */
    void compare(NodeState before, NodeState after, NodeStateDiff diff);

}
