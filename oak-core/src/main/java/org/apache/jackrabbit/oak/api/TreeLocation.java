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
 * It can either refer to a inner node (that is a {@link org.apache.jackrabbit.oak.api.Tree})
 * or to a leaf (that is a {@link org.apache.jackrabbit.oak.api.PropertyState}).
 * {@code TreeLocation} instances provide methods for navigating trees. {@code TreeLocation}
 * instances are immutable and navigating a tree always results in new {@code TreeLocation}
 * instances. Navigation never fails. Errors are deferred until the underlying item itself is
 * accessed. That is, if a {@code TreeLocation} points to an item which does not exist or
 * is unavailable otherwise (i.e. due to access control restrictions) accessing the tree
 * will return {@code null} at this point.
 */
public interface TreeLocation {

    /**
     * Navigate to the parent
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
     * {@link org.apache.jackrabbit.oak.api.Tree.Status} of the underlying item or {@code null} if no
     * such item exists.
     * @return
     */
    @CheckForNull
    Status getStatus();

    /**
     * The path of the underlying item or {@code null} if no such item exists.
     * @return  path
     */
    @CheckForNull
    String getPath();

}
