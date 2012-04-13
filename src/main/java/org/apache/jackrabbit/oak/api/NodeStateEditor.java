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
 * An editor for modifying existing and creating new node states.
 */
public interface NodeStateEditor {

    /**
     * Add the child node state with the given {@code name}. Does nothing
     * if such a child node already exists.
     *
     * @param name name of the new node state
     * @return the transient state of the child node with that name or {@code null}
     * if no such node exists.
     */
    TransientNodeState addNode(String name);

    /**
     * Remove the child node state with the given {@code name}. Does nothing
     * if no such child node exists.
     * @param name  name of the node state to remove
     */
    void removeNode(String name);

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

    /**
     * Move the node state located at {@code sourcePath} to a node
     * state at {@code destPath}. Do noting if either the source
     * does not exist, the parent of the destination does not exist
     * or the destination exists already. Both paths must resolve
     * to node states located in the subtree below the transient
     * state this editor is acting upon.
     *
     * @param sourcePath source path relative to this node state
     * @param destPath destination path relative to this node state
     */
    void move(String sourcePath, String destPath);

    /**
     * Copy the node state located at {@code sourcePath} to a node
     * state at {@code destPath}. Do noting if either the source
     * does not exist, the parent of the destination does not exist
     * or the destination exists already. Both paths must resolve
     * to node states located in the subtree below the transient
     * state this editor is acting upon.
     *
     * @param sourcePath source path relative to this node state
     * @param destPath destination path relative to this node state
     */
    void copy(String sourcePath, String destPath);

    /**
     * Edit the child node state at the given {@code path}.
     * The path must resolve to a node state located in the subtree
     * below the transient state this editor is acting upon.
     *
     * @param path path of the child node state to edit.
     * @return editor for the child node state at the given path or
     *         {@code null} if no such node state exists.
     */
    NodeStateEditor edit(String path);

    /**
     * Return the transient state which this editor is acting upon
     * @return transient node state
     */
    TransientNodeState getTransientState();
}
