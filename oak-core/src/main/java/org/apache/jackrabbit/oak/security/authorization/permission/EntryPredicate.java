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

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Predicate used to evaluation if a given {@code PermissionEntry} matches
 * the specified tree, property or path.
 */
interface EntryPredicate extends Predicate<PermissionEntry> {

    @Nullable
    String getPath();

    default boolean apply(@Nullable PermissionEntry entry) {
        return entry != null && apply(entry, true);
    }

    boolean apply(@NotNull PermissionEntry entry, boolean respectParent);

    static EntryPredicate create() {
        return new EntryPredicate() {
            @Nullable
            @Override
            public String getPath() {
                return null;
            }

            @Override
            public boolean apply(@NotNull PermissionEntry entry, boolean respectParent) {
                return entry.matches();
            }
        };
    }

    static EntryPredicate create(@NotNull Tree tree, @Nullable PropertyState property, boolean respectParent) {
        Tree parent = (!respectParent || tree.isRoot()) ? null : tree.getParent();
        boolean rp = respectParent && parent != null;
        return new EntryPredicate() {
            @NotNull
            @Override
            public String getPath() {
                return tree.getPath();
            }

            @Override
            public boolean apply(@NotNull PermissionEntry entry, boolean respectParent) {
                respectParent &= rp;
                return entry.matches(tree, property) || (respectParent && entry.matches(parent, null));
            }
        };
    }

    static EntryPredicate create(@NotNull String path, boolean respectParent) {
        String parentPath = (!respectParent || PathUtils.ROOT_PATH.equals(path)) ? null : PathUtils.getParentPath(path);
        boolean rp = respectParent && parentPath != null;
        return new EntryPredicate() {
            @NotNull
            @Override
            public String getPath() {
                return path;
            }

            @Override
            public boolean apply(@NotNull PermissionEntry entry, boolean respectParent) {
                respectParent &= rp;
                return entry.matches(path) || (respectParent && entry.matches(parentPath));
            }
        };
    }
}
