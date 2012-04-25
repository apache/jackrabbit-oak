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
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.oak.api.CommitFailedException;

/**
 * TODO update javadoc
 * Storage abstraction for content trees. At any given point in time
 * the stored content tree is rooted at a single immutable node state.
 * <p>
 * This is a low-level interface that doesn't cover functionality like
 * merging concurrent changes or rejecting new tree states based on some
 * higher-level consistency constraints.
 *
 * TODO: check if can be replaced by mk.model.NodeStore
 */
public interface NodeStore {

    /**
     * Returns the latest state of the content tree.
     *
     * @return root node state
     */
    NodeState getRoot();

    /**
     * FIXME document
     * @param nodeState
     * @return
     */
    NodeStateBuilder getBuilder(NodeState nodeState);

    /**
     * FIXME document
     * @param builder
     * @return
     */
    void apply(NodeStateBuilder builder) throws CommitFailedException;

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
