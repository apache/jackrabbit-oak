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
 * Builder interface for constructing new {@link NodeState node states}.
 */
public interface NodeBuilder {

    /**
     * Sets or removes the named property.
     *
     * @param name property name
     * @param encodedValue encoded value of the property,
     *                     or <code>null</code> to remove the named property
     */
    void setProperty(String name, String encodedValue);

    /**
     * Sets or removes the named child node.
     *
     * @param name child node name
     * @param childNode new child node state,
     *                  or <code>null</code> to remove the named child node
     */
    void setChildNode(String name, NodeState childNode);

    /**
     * Returns an immutable node state that matches the current state of
     * the builder.
     *
     * @return immutable node state
     */
    NodeState getNodeState();

}
