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
package org.apache.jackrabbit.oak.api;

import java.util.List;

/**
 * A transient node state represents a mutable node.
 * <p>
 * A transient node state contains the current state of a node and is
 * in contrast to {@link org.apache.jackrabbit.mk.model.NodeState} instances
 * mutable and not thread safe.
 * <p>
 * The various accessors on this class mirror these of {@code NodeState}. However,
 * since instances of this class are mutable return values may change between
 * invocations.
 */
public interface TransientNodeState {
    /**
     * @return  the name of this transient node state
     */
    String getName();

    /**
     * @return  relative path of this transient node state
     */
    String getPath();

    /**
     * @return  the parent of this transient node state
     */
    TransientNodeState getParent();

    /**
     * Get a property state
     * @param name name of the property state
     * @return  the property state with the given {@code name} or {@code null}
     *          if no such property state exists.
     */
    PropertyState getProperty(String name);

    /**
     * Determine if a property state exists
     * @param name  name of the property state
     * @return  {@code true} if and only if a property with the given {@code name}
     *          exists.
     */
    boolean hasProperty(String name);

    /**
     * Determine the number of properties.
     * @return  number of properties
     */
    long getPropertyCount();

    /**
     * All property states. The returned {@code Iterable} has snapshot semantics. That
     * is, it reflect the state of this transient node state instance at the time of the
     * call. Later changes to this instance are no visible to iterators obtained from
     * the returned iterable.
     * @return  An {@code Iterable} for all property states
     */
    Iterable<PropertyState> getProperties();

    /**
     * Get a child node state
     * @param name  name of the child node state
     * @return  the child node state with the given {@code name} or {@code null}
     *          if no such child node state exists.
     */
    TransientNodeState getChildNode(String name);

    /**
     * Determine if a child node state exists
     * @param name  name of the child node state
     * @return  {@code true} if and only if a child node with the given {@code name}
     *          exists.
     */
    boolean hasNode(String name);

    /**
     * Determine the number of child nodes.
     * @return  number of child nodes.
     */
    long getChildNodeCount();

    /**
     * All child node states. The returned {@code Iterable} has snapshot semantics. That
     * is, it reflect the state of this transient node state instance at the time of the
     * call. Later changes to this instance are no visible to iterators obtained from
     * the returned iterable.
     * @return  An {@code Iterable} for all child node states
     */
    Iterable<TransientNodeState> getChildNodes();

    /**
     * Add the child node state with the given {@code name}. Does nothing
     * if such a child node already exists.
     *
     * @param name name of the new node state
     * @return the transient state of the child node with that name.
     */
    TransientNodeState addNode(String name);

    /**
     * Remove the child node state with the given {@code name}. Does nothing
     * if no such child node exists.
     * @param name  name of the node state to remove
     * @return  {@code false} iff no such child node exists.
     */
    boolean removeNode(String name);

    /**
     * Set a single valued property state on this node state.
     *
     * @param name The name of this property
     * @param value The value of this property
     */
    void setProperty(String name, Scalar value);

    /**
     * Set a multivalued valued property state on this node state.
     *
     * @param name The name of this property
     * @param values The value of this property
     */
    void setProperty(String name, List<Scalar> values);

    /**
     * Remove a property from this node state
     * @param name name of the property
     */
    void removeProperty(String name);

}
