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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * A tree instance represents a snapshot of the {@code ContentRepository}
 * tree at the time the instance was acquired. Tree instances may
 * become invalid over time due to garbage collection of old content, at
 * which point an outdated snapshot will start throwing
 * {@code IllegalStateException}s to indicate that the snapshot is no
 * longer available.
 * <p>
 * A tree instance belongs to the client and its state is only modified
 * in response to method calls made by the client. The various accessors
 * on this interface mirror these of the underlying {@code NodeState}
 * interface. However, since instances of this class are mutable return
 * values may change between invocations.
 * <p>
 * Tree instances are not thread-safe for write access, so writing clients
 * need to ensure that they are not accessed concurrently from multiple
 * threads. Instances are however thread-safe for read access, so
 * implementations need to ensure that all reading clients see a
 * coherent state.
 */
public interface Tree {

    /**
     * Status of an item in a {@code Tree}
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
     * @return  the name of this {@code Tree} instance.
     */
    @Nonnull
    String getName();

    /**
     * @return {@code true} iff this is the root
     */
    boolean isRoot();

    /**
     * @return  path of this {@code Tree} instance relative to its {@link Root}.
     */
    @Nonnull
    String getPath();

    /**
     * Get the {@code Status} of this tree instance.
     *
     * @return The status of this tree instance.
     */
    @Nonnull
    Status getStatus();

    /**
     * @return the current location
     */
    @Nonnull
    TreeLocation getLocation();

    /**
     * @return the parent of this {@code Tree} instance. This method returns
     * {@code null} if the parent is not accessible or if no parent exists (root
     * node).
     */
    @CheckForNull
    Tree getParent();

    /**
     * Get a property state
     *
     * @param name The name of the property state.
     * @return the property state with the given {@code name} or {@code null}
     * if no such property state exists or the property is not accessible.
     */
    @CheckForNull
    PropertyState getProperty(String name);

    /**
     * Get the {@code Status} of a property state or {@code null}.
     *
     * @param name The name of the property state.
     * @return The status of the property state with the given {@code name}
     * or {@code null} in no such property state exists or if the name refers
     * to a property that is not accessible.
     */
    @CheckForNull
    Status getPropertyStatus(String name);

    /**
     * Determine if a property state exists and is accessible.
     *
     * @param name The name of the property state
     * @return {@code true} if and only if a property with the given {@code name}
     *          exists and is accessible.
     */
    boolean hasProperty(String name);

    /**
     * Determine the number of properties accessible to the current content session.
     *
     * @return The number of accessible properties.
     */
    long getPropertyCount();

    /**
     * All accessible property states. The returned {@code Iterable} has snapshot
     * semantics. That is, it reflect the state of this {@code Tree} instance at
     * the time of the call. Later changes to this instance are no visible to
     * iterators obtained from the returned iterable.
     *
     * @return An {@code Iterable} for all accessible property states.
     */
    @Nonnull
    Iterable<? extends PropertyState> getProperties();

    /**
     * Get a child of this {@code Tree} instance.
     *
     * @param name The name of the child to retrieve.
     * @return The child with the given {@code name} or {@code null} if no such
     * child exists or the child is not accessible.
     */
    @CheckForNull
    Tree getChild(String name);

    /**
     * Determine if a child of this {@code Tree} instance exists. If no child
     * exists or an existing child isn't accessible this method returns {@code false}.
     *
     * @param name The name of the child
     * @return {@code true} if and only if a child with the given {@code name}
     * exists and is accessible for the current content session.
     */
    boolean hasChild(String name);

    /**
     * Determine the number of children of this {@code Tree} instance taking
     * access restrictions into account.
     *
     * @return The number of accessible children.
     */
    long getChildrenCount();

    /**
     * All accessible children of this {@code Tree} instance. The returned
     * {@code Iterable} has snapshot semantics. That is, it reflect the state of
     * this {@code Tree} instance at the time of the call. Later changes to this
     * instance are not visible to iterators obtained from the returned iterable.
     *
     * @return An {@code Iterable} for all accessible children
     */
    @Nonnull
    Iterable<Tree> getChildren();

    /**
     * Remove this tree instance. This operation never succeeds for the root tree.
     *
     * @return {@code true} if the node was removed; {@code false} otherwise.
     */
    boolean remove();

    /**
     * Add a child with the given {@code name}. Does nothing if such a child
     * already exists.
     *
     * @param name name of the child
     * @return the {@code Tree} instance of the child with the given {@code name}.
     */
    @Nonnull
    Tree addChild(String name);

    /**
     * Set a single valued property state
     *
     * @param name The name of this property
     * @param value The value of this property
     * @return the affected property state
     */
    @Nonnull
    PropertyState setProperty(String name, @Nonnull CoreValue value);

    /**
     * Set a multivalued valued property state
     *
     * @param name The name of this property
     * @param values The value of this property
     * @return the affected property state
     */
    @Nonnull
    PropertyState setProperty(String name, @Nonnull List<CoreValue> values);

    /**
     * Remove the property with the given name. This method has no effect if a
     * property of the given {@code name} does not exist.
     *
     * @param name The name of the property
     */
    void removeProperty(String name);

}
