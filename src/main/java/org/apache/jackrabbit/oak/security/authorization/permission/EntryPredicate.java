/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.security.authorization.permission;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * Predicate used to evaluation if a given {@code PermissionEntry} matches
 * the specified tree, property or path.
 */
final class EntryPredicate implements Predicate<PermissionEntry> {

    private final Tree tree;
    private final PropertyState property;
    private final String path;

    private final String parentPath;
    private final Tree parent;

    public EntryPredicate(@Nonnull Tree tree, @Nullable PropertyState property,
                          boolean respectParent) {
        this(tree, property, tree.getPath(), respectParent);
    }

    public EntryPredicate(@Nonnull String path, boolean respectParent) {
        this(null, null, path, respectParent);
    }

    public EntryPredicate() {
        this(null, null, null, false);
    }

    private EntryPredicate(@Nullable Tree tree, @Nullable PropertyState property,
                           @Nullable String path, boolean respectParent) {
        this.tree = tree;
        this.property = property;
        this.path = path;

        if (respectParent) {
            parentPath = (path == null || "/".equals(path)) ? null : PathUtils.getParentPath(path);
            parent = (tree == null || tree.isRoot()) ? null : tree.getParent();
        } else {
            parentPath = null;
            parent = null;
        }
    }

    @CheckForNull
    public String getPath() {
        return path;
    }

    @Override
    public boolean apply(@Nullable PermissionEntry entry) {
        if (entry == null) {
            return false;
        }
        if (tree != null) {
            return entry.matches(tree, property) || applyToParent(entry);
        } else if (path != null) {
            return entry.matches(path) || applyToParent(entry);
        } else {
            return entry.matches();
        }
    }

    private boolean applyToParent(@Nonnull PermissionEntry entry) {
        if (parent != null) {
            return entry.matches(parent, null);
        } else if (parentPath != null) {
            return entry.matches(parentPath);
        } else {
            return false;
        }
    }
}