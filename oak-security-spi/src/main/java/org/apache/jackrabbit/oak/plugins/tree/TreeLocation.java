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
package org.apache.jackrabbit.oak.plugins.tree;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAbsolute;

/**
 * A {@code TreeLocation} denotes a location inside a tree.
 * <p>
 * It can either refer to a inner node (that is a {@link Tree}), to a leaf (that is a
 * {@link PropertyState}) or to an invalid location which refers to neither of the former.
 * {@code TreeLocation} instances provide methods for navigating trees such that navigation
 * always results in new {@code TreeLocation} instances. Navigation never fails. Errors are
 * deferred until the underlying item itself is accessed. That is, if a {@code TreeLocation}
 * points to an item which does not exist or is unavailable otherwise (i.e. due to access
 * control restrictions) accessing the tree will return {@code null} at this point.
 */
public abstract class TreeLocation {

    /**
     * Create a new {@code TreeLocation} instance for a {@code tree}
     */
    public static TreeLocation create(@Nonnull Tree tree) {
        return new NodeLocation(tree);
    }

    /**
     * Create a new {@code TreeLocation} instance for the item
     * at the given {@code path} in {@code root}.
     */
    public static TreeLocation create(Root root, String path) {
        checkArgument(isAbsolute(path));
        TreeLocation location = create(root.getTree(PathUtils.ROOT_PATH));
        for (String name : elements(path)) {
            location = location.getChild(name);
        }
        return location;
    }

    /**
     * Equivalent to {@code create(root, "/")}
     */
    public static TreeLocation create(Root root) {
        return create(root, PathUtils.ROOT_PATH);
    }

    /**
     * Navigate to the parent or an invalid location for the root of the hierarchy.
     * @return a {@code TreeLocation} for the parent of this location.
     */
    @Nonnull
    public abstract TreeLocation getParent();

    /**
     * Determine whether the underlying {@link org.apache.jackrabbit.oak.api.Tree} or
     * {@link org.apache.jackrabbit.oak.api.PropertyState} for this {@code TreeLocation}
     * is available.
     * @return  {@code true} if the underlying item is available and has not been disconnected.
     * @see org.apache.jackrabbit.oak.api.Tree#exists()
     */
    public abstract boolean exists();

    /**
     * The name of this location
     * @return name
     */
    @Nonnull
    public abstract String getName();

    /**
     * The path of this location
     * @return  path
     */
    @Nonnull
    public abstract String getPath();

    /**
     * Remove the underlying item.
     *
     * @return {@code true} if the item was removed, {@code false} otherwise.
     */
    public abstract boolean remove();

    /**
     * Navigate to a child of the given {@code name}.
     * @param name  name of the child
     * @return  this default implementation return a non existing location
     */
    @Nonnull
    public TreeLocation getChild(String name) {
        return new NullLocation(this, name);
    }

    /**
     * Get the underlying {@link org.apache.jackrabbit.oak.api.Tree} for this {@code TreeLocation}.
     * @return  this default implementation return {@code null}.
     */
    @CheckForNull
    public Tree getTree() {
        return null;
    }

    /**
     * Get the underlying {@link org.apache.jackrabbit.oak.api.PropertyState} for this {@code TreeLocation}.
     * @return  this default implementation return {@code null}.
     */
    @CheckForNull
    public PropertyState getProperty() {
        return null;
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("path", getPath()).toString();
    }

    /**
     * This {@code TreeLocation} refers to child tree in a
     * {@code Tree}.
     */
    private static class NodeLocation extends TreeLocation {
        private final Tree tree;

        public NodeLocation(Tree tree) {
            this.tree = tree;
        }

        @Nonnull
        @Override
        public TreeLocation getParent() {
            return tree.isRoot()
                ? NullLocation.NULL
                : new NodeLocation(tree.getParent());
        }

        @Nonnull
        @Override
        public TreeLocation getChild(String name) {
            if (tree.hasProperty(name)) {
                return new PropertyLocation(tree, name);
            } else {
                return new NodeLocation(tree.getChild(name));
            }
        }

        @Override
        public boolean exists() {
            return tree.exists();
        }

        @Nonnull
        @Override
        public String getName() {
            return tree.getName();
        }

        @Override
        public Tree getTree() {
            return exists() ? tree : null;
        }

        @Nonnull
        @Override
        public String getPath() {
            return tree.getPath();
        }

        @Override
        public boolean remove() {
            return exists() && tree.remove();
        }
    }

    /**
     * This {@code TreeLocation} refers to property in a
     * {@code Tree}.
     */
    private static class PropertyLocation extends TreeLocation {
        private final Tree parent;
        private final String name;

        public PropertyLocation(Tree parent, String name) {
            this.parent = parent;
            this.name = name;
        }

        @Nonnull
        @Override
        public TreeLocation getParent() {
            return new NodeLocation(parent);
        }

        @Override
        public boolean exists() {
            return parent.hasProperty(name);
        }

        @Nonnull
        @Override
        public String getName() {
            return name;
        }

        @Override
        public PropertyState getProperty() {
            return parent.getProperty(name);
        }

        @Nonnull
        @Override
        public String getPath() {
            return PathUtils.concat(parent.getPath(), name);
        }

        @Override
        public boolean remove() {
            parent.removeProperty(name);
            return true;
        }
    }

    /**
     * This {@code TreeLocation} refers to an invalid location in a tree. That is
     * to a location where no item resides.
     */
    private static final class NullLocation extends TreeLocation {
        public static final NullLocation NULL = new NullLocation();

        private final TreeLocation parent;
        private final String name;

        public NullLocation(TreeLocation parent, String name) {
            this.parent = parent;
            this.name = name;
        }

        private NullLocation() {
            this.parent = this;
            this.name = "";
        }

        @Nonnull
        @Override
        public TreeLocation getParent() {
            return parent;
        }

        /**
         * @return {@code false}
         */
        @Override
        public boolean exists() {
            return false;
        }

        @Nonnull
        @Override
        public String getName() {
            return name;
        }

        @Nonnull
        @Override
        public String getPath() {
            return parent == this ? "" : PathUtils.concat(parent.getPath(), name);
        }

        /**
         * @return Always {@code false}.
         */
        @Override
        public boolean remove() {
            return false;
        }

    }
}
