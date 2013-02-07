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

import org.apache.jackrabbit.oak.api.Tree.Status;

/**
 * A {@code TreeLocation} denotes a location inside a tree.
 * It can either refer to a inner node (that is a {@link org.apache.jackrabbit.oak.api.Tree}),
 * to a leaf (that is a {@link org.apache.jackrabbit.oak.api.PropertyState}) or to an
 * invalid location which refers to neither of the former.
 * {@code TreeLocation} instances provide methods for navigating trees. {@code TreeLocation}
 * instances are immutable and navigating a tree always results in new {@code TreeLocation}
 * instances. Navigation never fails. Errors are deferred until the underlying item itself is
 * accessed. That is, if a {@code TreeLocation} points to an item which does not exist or
 * is unavailable otherwise (i.e. due to access control restrictions) accessing the tree
 * will return {@code null} at this point.
 */
public interface TreeLocation {

    /**
     * Navigate to the parent or an invalid location for the root of the hierarchy.
     * @return  a {@code TreeLocation} for the parent of this location.
     */
    @Nonnull
    TreeLocation getParent();

    /**
     * Navigate to a child through a relative path. A relative path consists of a
     * possibly empty lists of names separated by forward slashes.
     * @param relPath  relative path to the child
     * @return  a {@code TreeLocation} for a child with the given {@code name}.
     */
    @Nonnull
    TreeLocation getChild(String relPath);

    /**
     * Determine whether the underlying {@link org.apache.jackrabbit.oak.api.Tree} or
     * {@link org.apache.jackrabbit.oak.api.PropertyState} for this {@code TreeLocation}
     * is available.
     * @return  {@code true} if either {@link #getTree()} or {@link #getParent()} is non {@code null}.
     * {@code false} otherwise.
     */
    boolean exists();

    /**
     * Get the underlying {@link org.apache.jackrabbit.oak.api.Tree} for this {@code TreeLocation}.
     * @return  underlying {@code Tree} instance or {@code null} if not available.
     */
    @CheckForNull
    Tree getTree();

    /**
     * Get the underlying {@link org.apache.jackrabbit.oak.api.PropertyState} for this {@code TreeLocation}.
     * @return  underlying {@code PropertyState} instance or {@code null} if not available.
     */
    @CheckForNull
    PropertyState getProperty();

    /**
     * Get the {@link org.apache.jackrabbit.oak.api.Tree.Status} of the underlying item for this
     * {@code TreeLocation}.
     * @return  underlying status or {@code null} if not available.
     */
    @CheckForNull
    Status getStatus();

    /**
     * The path of this location
     * @return  path
     */
    @Nonnull
    String getPath();

    /**
     * Remove the underlying item.
     *
     * @return {@code true} if the item was removed, {@code false} otherwise.
     */
    boolean remove();

    /**
     * Set the underlying property of this {@code TreeLocation}. If the underlying item is
     * not a property, this method return {@code false}.
     * @param property The property to set
     * @return {@code true} if the property state was set, {@code false} otherwise.
     */
    boolean set(PropertyState property);

}
