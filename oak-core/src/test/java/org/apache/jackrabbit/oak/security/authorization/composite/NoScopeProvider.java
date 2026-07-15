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
package org.apache.jackrabbit.oak.security.authorization.composite;

import java.util.Set;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@code AggregatedPermissionProvider} that doesn't supported anything anywhere
 * and which consequently must be completely ignored from the permission evaluation.
 */
final class NoScopeProvider extends AbstractAggrProvider {

    NoScopeProvider(Root root) {
        super(root);
    }

    @NotNull
    @Override
    public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
        return PrivilegeBits.EMPTY;
    }

    @Override
    public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
        return Permissions.NO_PERMISSION;
    }

    @Override
    public long supportedPermissions(@NotNull TreeLocation location, long permissions) {
        return Permissions.NO_PERMISSION;
    }

    @Override
    public long supportedPermissions(@NotNull TreePermission treePermission, @Nullable PropertyState property, long permissions) {
        return Permissions.NO_PERMISSION;
    }

    @Override
    public boolean isGranted(@NotNull TreeLocation location, long permissions) {
        throw new UnsupportedOperationException("should never get here");
    }

    @NotNull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        throw new UnsupportedOperationException("should never get here");
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @NotNull String... privilegeNames) {
        throw new UnsupportedOperationException("should never get here");
    }

    @NotNull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        throw new UnsupportedOperationException("should never get here");
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreePermission parentPermission) {
        if (tree.isRoot()) {
            return TreePermission.NO_RECOURSE;
        } else {
            throw new UnsupportedOperationException("should never get here");
        }
    }

    @Override
    public boolean isGranted(@NotNull Tree tree, @Nullable PropertyState property, long permissions) {
        throw new UnsupportedOperationException("should never get here");
    }

    @Override
    public boolean isGranted(@NotNull String oakPath, @NotNull String jcrActions) {
        throw new UnsupportedOperationException("should never get here");
    }
}
