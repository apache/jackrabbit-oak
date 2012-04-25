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

import org.apache.jackrabbit.oak.api.PropertyState;

/**
 * Builder interface for constructing new {@link NodeState node states}.
 */
public interface NodeStateBuilder {

    /**
     * Returns an immutable node state that matches the current state of
     * the builder.
     *
     * @return immutable node state
     */
    NodeState getNodeState();

    /**
     * Get a builder for a child node
     *
     * @param name  name of the child node
     * @return  builder for the {@code name}d child node
     */
    NodeStateBuilder getChildBuilder(String name);

    /**
     * Add a sub-tree
     *
     * @param name  name child node containing the sub-tree
     * @param nodeState  sub-tree
     * @return  builder for the added sub-tree
     */
    NodeStateBuilder addNode(String name, NodeState nodeState);

    /**
     * Set or removes the named child node.
     *
     * @param name  name of the child node
     */
    NodeStateBuilder addNode(String name);

    /**
     * Remove a child node
     * @param name  name of the child node
     * @return  {@code true} iff the child node existed
     */
    boolean removeNode(String name);

    /**
     * Set a property.
     *
     * @param property  property to set
     */
    void setProperty(PropertyState property);

    /**
     * Remove the named property
     * @param name  name of the property
     */
    void removeProperty(String name);

    /**
     * Move this node
     * @param destParent  builder for the parent node of the destination
     * @param destName  name of the moved node
     * @return  {@code true} iff the move succeeded
     */
    boolean moveTo(NodeStateBuilder destParent, String destName);

    /**
     * Copy this node
     * @param destParent  builder for the parent node of the destination
     * @param destName  name of the copied node
     * @return  {@code true} iff the copy succeeded
     */
    boolean copyTo(NodeStateBuilder destParent, String destName);
}
