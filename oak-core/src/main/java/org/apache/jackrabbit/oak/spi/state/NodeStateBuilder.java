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

import org.apache.jackrabbit.oak.api.CoreValue;

import java.util.List;

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
     * Add a sub-tree
     *
     * @param name  name child node containing the sub-tree
     * @param nodeState  sub-tree
     */
    void setNode(String name, NodeState nodeState);

    /**
     * Remove a child node
     * @param name  name of the child node
     */
    void removeNode(String name);

    /**
     * Set a property.
     *
     * @param name property name
     * @param value
     */
    void setProperty(String name, CoreValue value);

    /**
     * Set a property.
     *
     * @param name property name
     * @param values
     */
    void setProperty(String name, List<CoreValue> values);

    /**
     * Remove the named property
     * @param name  name of the property
     */
    void removeProperty(String name);

}
