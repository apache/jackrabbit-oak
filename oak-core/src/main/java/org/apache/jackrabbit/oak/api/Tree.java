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
 * A content tree represents a snapshot of the content in a
 * {@code ContentRepository} at the time the instance was acquired.
 * {@code ContentTree} instances may become invalid over time due to
 * garbage collection of old content, at which point an outdated
 * snapshot will start throwing {@code IllegalStateException}s to
 * indicate that the snapshot is no longer available.
 * <p>
 * {@code ContentTree} instance belongs to the client and its state
 * is only modified in response to method calls made by the client.
 * The various accessors on this class mirror these of {@code NodeState}.
 * However, since instances of this class are mutable return values may
 * change between invocations.
 * <p>
 * {@code ContentTree} instances are not thread-safe for write access, so
 * writing clients need to ensure that they are not accessed concurrently
 * from multiple threads. {@code ContentTree} instances are however
 * thread-safe for read access, so implementations need to ensure that all
 * reading clients see a coherent state.
 *
 */
public interface Tree {

    /**
     * Status of an item in a {@code ContentTree}
     */
    enum Status {
        /**
         * Item is persisted
         */
        EXISTING,

        /**
         * Item is new
         */
        NEW,

        /**
         * Item is modified: has added or removed children or added, removed or modified
         * properties.
         */
        MODIFIED,

        /**
         * Item is removed
         */
        REMOVED
    }

    /**
     * @return  the name of this {@code ContentTree} instance.
     */
    String getName();

    /**
     * @return  path of this {@code ContentTree} instance.
     */
    String getPath();

    /**
     * @return  the parent of this {@code ContentTree} instance.
     */
    Tree getParent();

    /**
     * Get a property state
     * @param name name of the property state
     * @return  the property state with the given {@code name} or {@code null}
     *          if no such property state exists.
     */
    PropertyState getProperty(String name);

    /**
     * Get the {@code Status} of a property state
     * @param name  name of the property state
     * @return  the status of the property state with the given {@code name}
     *          or {@code null} in no such property state exists.
     */
    Status getPropertyStatus(String name);

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
     * is, it reflect the state of this {@code ContentTree} instance at the time of the
     * call. Later changes to this instance are no visible to iterators obtained from
     * the returned iterable.
     * @return  An {@code Iterable} for all property states
     */
    Iterable<PropertyState> getProperties();

    /**
     * Get a child of this {@code ContentTree} instance
     * @param name  name of the child
     * @return  the child with the given {@code name} or {@code null} if no such child
     * exists.
     */
    Tree getChild(String name);

    /**
     * Get the {@code Status} of a child tree
     * @param name  name of the child
     * @return  the status of the child with the given {@code name} or {@code null} in
     *          no such child exists.
     */
    Status getChildStatus(String name);

    /**
     * Determine if a child of this {@code ContentTree} instance exists.
     * @param name  name of the child
     * @return  {@code true} if and only if a child with the given {@code name}
     *          exists.
     */
    boolean hasChild(String name);

    /**
     * Determine the number of children of this {@code ContentTree} instance.
     * @return  number of children
     */
    long getChildrenCount();

    /**
     * All children of this {@code ContentTree} instance. The returned {@code Iterable}
     * has snapshot semantics. That is, it reflect the state of this {@code ContentTree}
     * instance. instance at the time of the call. Later changes to this instance are no
     * visible to iterators obtained from the returned iterable.
     * @return  An {@code Iterable} for all children
     */
    Iterable<Tree> getChildren();

    /**
     * Add a child with the given {@code name}. Does nothing if such a child
     * already exists.
     *
     * @param name name of the child
     * @return the {@code ContentTree} instance of the child with the given {@code name}.
     */
    Tree addChild(String name);

    /**
     * Remove a child with the given {@code name}. Does nothing if no such child exists.
     * @param name  name of the child to remove
     * @return  {@code false} iff no such child exists.
     */
    boolean removeChild(String name);

    /**
     * Set a single valued property state
     *
     * @param name The name of this property
     * @param value The value of this property
     */
    void setProperty(String name, CoreValue value);

    /**
     * Set a multivalued valued property state
     *
     * @param name The name of this property
     * @param values The value of this property
     */
    void setProperty(String name, List<CoreValue> values);

    /**
     * Remove a property
     * @param name name of the property
     */
    void removeProperty(String name);

}
