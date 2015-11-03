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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A tree instance represents a snapshot of the {@link ContentRepository}
 * tree at the time the instance was acquired from a {@link ContentSession}.
 * Tree instances may become invalid over time due to garbage collection of
 * old content, at which point an outdated snapshot will start throwing
 * {@code IllegalStateException}s to indicate that the snapshot is no
 * longer available.
 *
 * <h2>Order and orderability</h2>
 * The children of a {@code Tree} are generally unordered. That is, the
 * sequence of the children returned by {@link #getChildren()} may change over
 * time as this {@code Tree} is modified either directly or through some other
 * session. Calling {@link #orderBefore(String)} will persist the current order
 * and maintain the order as new children are added or removed. In this case a
 * new child will be inserted after the last child as seen by {@link #getChildren()}.
 *
 * <h2>State and state transitions</h2>
 * A tree instance belongs to the client and its state is only modified
 * in response to method calls made by the client. The various accessors
 * on this interface mirror these of the underlying {@code NodeState}
 * interface. However, since instances of this class are mutable return
 * values may change between invocations.
 * <p>
 * All tree instances created in the context of a content session become invalid
 * after the content session is closed. Any method called on an invalid tree instance
 * will throw an {@code InvalidStateException}.
 * <p>
 * {@link Tree} instances may become non existing after a call to
 * {@link Root#refresh()}, {@link Root#rebase()} or {@link Root#commit()}.
 * Any write access to non existing {@code Tree} instances will cause an
 * {@code InvalidStateException}.
 *
 * <h2>Thread safety</h2>
 * Tree instances are not thread-safe for write access, so writing clients
 * need to ensure that they are not accessed concurrently from multiple
 * threads. Instances are however thread-safe for read access, so
 * implementations need to ensure that all reading clients see a
 * coherent state.
 *
 * <h2>Visibility and access control</h2>
 * The data returned by this class and intermediary objects such as are access
 * controlled governed by the {@code ContentSession} instance from which
 * the containing {@code Root} was obtained.
 *
 * <h2>Existence and iterability of trees</h2>
 * <p>
 * The {@link #getChild(String)} method is special in that it <em>never</em>
 * returns a {@code null} value, even if the named tree does not exist.
 * Instead a client should use the {@link #exists()} method on the returned
 * tree to check whether that tree exists.
 * <p>
 * The <em>iterability</em> of a tree is a related to existence. A node
 * state is <em>iterable</em> if it is included in the return values of the
 * {@link #getChildrenCount(long)} and {@link #getChildren()} methods. An iterable
 * node is guaranteed to exist, though not all existing nodes are necessarily
 * iterable.
 * <p>
 * Furthermore, a non-existing node is guaranteed to contain no properties
 * or iterable child nodes. It can, however contain non-iterable children.
 * Such scenarios are typically the result of access control restrictions.
 */
public interface Tree {

    /**
     * Status of an item in a {@code Tree}
     */
    enum Status {
        /**
         * Item is unchanged
         */
        UNCHANGED,

        /**
         * Item is new
         */
        NEW,

        /**
         * Item is modified: has added or removed children or added, removed or modified
         * properties.
         */
        MODIFIED
    }

    /**
     * @return the name of this {@code Tree} instance.
     */
    @Nonnull
    String getName();

    /**
     * @return {@code true} iff this is the root
     */
    boolean isRoot();

    /**
     * @return the absolute path of this {@code Tree} instance from its {@link Root}.
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
     * Determine whether this tree has been removed or does not exist otherwise (e.g. caused
     * by a refresh, rebase or commit) or is not visible due to access control restriction
     * or does not exist at all.
     * @return {@code true} if this tree exists, {@code false} otherwise.
     */
    boolean exists();

    /**
     * @return the possibly non existent parent of this {@code Tree}.
     * @throws IllegalStateException if called on the root tree.
     */
    @Nonnull
    Tree getParent();

    /**
     * Get a property state
     *
     * @param name The name of the property state.
     * @return the property state with the given {@code name} or {@code null}
     *         if no such property state exists or the property is not accessible.
     */
    @CheckForNull
    PropertyState getProperty(@Nonnull String name);

    /**
     * Get the {@code Status} of a property state or {@code null}.
     *
     * @param name The name of the property state.
     * @return The status of the property state with the given {@code name}
     *         or {@code null} in no such property state exists or if the name refers
     *         to a property that is not accessible.
     */
    @CheckForNull
    Status getPropertyStatus(@Nonnull String name);

    /**
     * Determine if a property state exists and is accessible.
     *
     * @param name The name of the property state
     * @return {@code true} if and only if a property with the given {@code name}
     *         exists and is accessible.
     */
    boolean hasProperty(@Nonnull String name);

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
     * Get a possibly non existing child of this {@code Tree}.
     * @param name The name of the child to retrieve.
     * @return The child with the given {@code name}.
     * @throws IllegalArgumentException if the given name is invalid
     */
    @Nonnull
    Tree getChild(@Nonnull String name) throws IllegalArgumentException;

    /**
     * Determine if a child of this {@code Tree} instance exists. If no child
     * exists or an existing child isn't accessible this method returns {@code false}.
     *
     * @param name The name of the child
     * @return {@code true} if and only if a child with the given {@code name}
     *         exists and is accessible for the current content session.
     */
    boolean hasChild(@Nonnull String name);
    
    /**
     * Determine the number of children of this {@code Tree} instance taking
     * access restrictions into account.
     * <p>
     * If an implementation does know the exact value, it returns it (even if
     * the value is higher than max). If the implementation does not know the
     * exact value, and the child node count is higher than max, it may return
     * Long.MAX_VALUE. The cost of the operation is at most O(max).
     * 
     * @param max the maximum value
     * @return the number of accessible children.
     */    
    long getChildrenCount(long max);

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
     * @param name name of the child. A valid name does not start with a colon,
     *             is not empty and does not contain a forward slash.
     * @return the {@code Tree} instance of the child with the given {@code name}.
     * @throws IllegalArgumentException if {@code name} is not valid.
     */
    @Nonnull
    Tree addChild(@Nonnull String name) throws IllegalArgumentException;

    /**
     * Changes the nature of this tree such that the order of the children
     * is kept stable. The expected behavior is as follows:
     * <p>
     * <ol>
     * <li>Calling {@code setOrderableChildren(true)} on a tree
     * the first time will stabilize the order of existing children. Any
     * subsequent {@link #addChild(String)} call is guaranteed to insert
     * the new tree and the end of the child list.</li>
     * <li>Calling {@code setOrderableChildren(true)} on a tree
     * that already has its children ordered has no effect.</li>
     * <li>Calling {@code setOrderableChildren(false)} on a tree that
     * doesn't have ordered children has not effect</li>
     * <li>Calling {@code setOrderableChildren(false)} on a tree
     * with ordered children will remove the necessity to keep the child
     * list stable. The order of children upon {@link #getChildren()} is
     * subsequently undefined.</li>
     * </ol>
     * <p>
     * Calling {@link #orderBefore(String)} on a tree, implicitly enables
     * orderable children on the parent tree.
     *
     * @param enable Enable (or disable) orderable children for this tree.
     */
    void setOrderableChildren(boolean enable);

    /**
     * Orders this {@code Tree} before the sibling tree with the given
     * {@code name}. Calling this method for the first time on this
     * {@code Tree} or any of its siblings will persist the current order
     * of siblings and maintain it from this point on.
     *
     * @param name the name of the sibling node where this tree is ordered
     *             before. This tree will become the last sibling if
     *             {@code name} is {@code null}.
     * @return {@code false} if there is no sibling with the given
     *         {@code name} or no reordering was performed;
     *         {@code true} otherwise.
     * @throws IllegalArgumentException if the given name is invalid
     */
    boolean orderBefore(@Nullable String name);

    /**
     * Set a property state
     *
     * @param property The property state to set
     * @throws IllegalArgumentException if {@code property} has a non valid name. A valid name
     *         does not start with a colon, is not empty and does not contain a forward slash.
     */
    void setProperty(@Nonnull PropertyState property);

    /**
     * Set a property state
     *
     * @param name  The name of this property. A valid name does not start with a colon,
     *              is not empty and does not contain a forward slash.
     * @param value The value of this property
     * @param <T>   The type of this property. Must be one of {@code String, Blob, byte[], Long, Integer, Double, Boolean, BigDecimal}
     * @throws IllegalArgumentException if {@code T} is not one of the above types or
     *         if {@code name} is not valid.
     */
    <T> void setProperty(@Nonnull String name, @Nonnull T value)
            throws IllegalArgumentException;

    /**
     * Set a property state
     *
     * @param name  The name of this property. A valid name does not start with a colon,
     *              is not empty and does not contain a forward slash.
     * @param value The value of this property
     * @param type  The type of this property.
     * @param <T>   The type of this property.
     * @throws IllegalArgumentException if {@code name} is not valid.
     */
    <T> void setProperty(@Nonnull String name, @Nonnull T value, @Nonnull Type<T> type)
            throws IllegalArgumentException;

    /**
     * Remove the property with the given name. This method has no effect if a
     * property of the given {@code name} does not exist.
     *
     * @param name The name of the property
     */
    void removeProperty(@Nonnull String name);

    /**
     * Empty array of trees.
     */
    Tree[] EMPTY_ARRAY = new Tree[0];

}
