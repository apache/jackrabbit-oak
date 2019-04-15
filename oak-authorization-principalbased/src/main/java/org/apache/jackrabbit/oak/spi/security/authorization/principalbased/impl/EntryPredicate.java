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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

final class EntryPredicate {

    private EntryPredicate() {}

    @NotNull
    static Predicate<PermissionEntry> create(@Nullable String oakPath) {
        if (oakPath == null) {
            return permissionEntry -> permissionEntry.matches();
        } else {
            return permissionEntry -> permissionEntry.matches(oakPath);
        }
    }

    @NotNull
    static Predicate<PermissionEntry> create(@NotNull Tree tree, @Nullable PropertyState property) {
        if (!tree.exists()) {
            // target node does not exist (anymore) in this workspace
            // use best effort calculation based on the item path.
            String predicatePath = (property == null) ? tree.getPath() : PathUtils.concat(tree.getPath(), property.getName());
            return create(predicatePath);
        } else {
            return permissionEntry -> permissionEntry.matches(tree, property);
        }
    }

    @NotNull
    static Predicate<PermissionEntry> createParent(@NotNull String treePath, @Nullable Tree parentTree, long permissions) {
        if (!Permissions.respectParentPermissions(permissions)) {
            return Predicates.alwaysFalse();
        }
        if (treePath.isEmpty() || PathUtils.denotesRoot(treePath)) {
            return Predicates.alwaysFalse();
        } else if (parentTree != null && parentTree.exists()) {
            return permissionEntry -> permissionEntry.appliesTo(parentTree.getPath()) && permissionEntry.matches(parentTree, null);
        } else {
            String parentPath = PathUtils.getParentPath(treePath);
            return permissionEntry -> permissionEntry.appliesTo(parentPath) && permissionEntry.matches(parentPath);
        }
    }

    @NotNull
    static Predicate<PermissionEntry> createParent(@NotNull Tree tree, long permissions) {
        if (!Permissions.respectParentPermissions(permissions)) {
            return Predicates.alwaysFalse();
        }
        if (!tree.exists()) {
            return createParent(tree.getPath(), tree.getParent(), permissions);
        } else {
            if (!tree.isRoot()) {
                Tree parentTree = tree.getParent();
                return permissionEntry -> permissionEntry.appliesTo(parentTree.getPath()) && permissionEntry.matches(parentTree, null);
            } else {
                return Predicates.alwaysFalse();
            }
        }
    }
}