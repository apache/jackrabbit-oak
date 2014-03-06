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
package org.apache.jackrabbit.oak.plugins.observation;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Handler of content change events. Used to decouple processing of changes
 * from the content diff logic that detects them.
 * <p>
 * As the content diff recurses down the content tree, it will call the
 * {@link #getChildHandler(String, NodeState, NodeState)} method to
 * specialize the handler instance for each node under which changes are
 * detected. The other handler methods always apply to the properties
 * and direct children of the node for which that handler instance is
 * specialized. The handler is expected to keep track of contextual
 * information like the path or identifier of the current node based on
 * the sequence of those specialization calls.
 * <p>
 * The events belonging to this instance <em>should</em> be delivered
 * before events to other instance deeper down the tree are delivered.
 * <p>
 * All names and paths passed to handler methods use unmapped Oak names.
 */
public interface EventHandler {

    /**
     * Called before the given before and after states are compared.
     * The implementation can use this method to initialize any internal
     * state needed for processing the results of the comparison.
     *
     * @param before before state, non-existent if this node was added
     * @param after after state, non-existent if this node was removed
     */
    void enter(NodeState before, NodeState after);

    /**
     * Called after the given before and after states are compared.
     * The implementation can use this method to post-process information
     * collected during the content diff.
     *
     * @param before before state, non-existent if this node was added
     * @param after after state, non-existent if this node was removed
     */
    void leave(NodeState before, NodeState after);

    /**
     * Returns a handler of events within the given child node, or
     * {@code null} if changes within that child are not to be processed.
     *
     * @param name  name of the child node
     * @param before before state of the child node, possibly non-existent
     * @param after  after state of the child node, possibly non-existent
     * @return handler of events within the child node, or {@code null}
     */
    @CheckForNull
    EventHandler getChildHandler(
            String name, NodeState before, NodeState after);

    /**
     * Notification for an added property
     * @param after  added property
     */
    void propertyAdded(PropertyState after);

    /**
     * Notification for a changed property
     * @param before  property before the change
     * @param after  property after the change
     */
    void propertyChanged(PropertyState before, PropertyState after);

    /**
     * Notification for a deleted property
     * @param before  deleted property
     */
    void propertyDeleted(PropertyState before);

    /**
     * Notification for an added node
     * @param name  name of the node
     * @param after  added node
     */
    void nodeAdded(String name, NodeState after);

    /**
     * Notification for a deleted node
     * @param name  name of the deleted node
     * @param before  deleted node
     */
    void nodeDeleted(String name, NodeState before);

    /**
     * Notification for a moved node
     * @param sourcePath  source of the moved node
     * @param name        name of the moved node
     * @param moved       moved node
     */
    void nodeMoved(String sourcePath, String name, NodeState moved);

    /**
     * Notification for a reordered node
     * @param destName    name of the {@code orderBefore()} destination node
     * @param name        name of the moved node
     * @param reordered       moved node
     */
    void nodeReordered(String destName, String name, NodeState reordered);

}
