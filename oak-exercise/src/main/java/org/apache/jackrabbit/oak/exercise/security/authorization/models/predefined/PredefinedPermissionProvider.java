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
package org.apache.jackrabbit.oak.exercise.security.authorization.models.predefined;

import java.security.Principal;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * EXERCISE: complete PermissionProvider implementation
 */
class PredefinedPermissionProvider implements PermissionProvider {

    private final Set<Principal> principals;

    PredefinedPermissionProvider(@NotNull Set<Principal> principals) {
        this.principals = principals;
    }

    @Override
    public void refresh() {
        // EXERCISE: complete PermissionProvider implementation

    }

    @NotNull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        // EXERCISE: complete PermissionProvider implementation
        return Set.of();
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @NotNull String... privilegeNames) {
        // EXERCISE: complete PermissionProvider implementation
        return false;
    }

    @NotNull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        // EXERCISE: complete PermissionProvider implementation
        return RepositoryPermission.EMPTY;
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreePermission parentPermission) {
        // EXERCISE: complete PermissionProvider implementation
        return TreePermission.EMPTY;
    }

    @Override
    public boolean isGranted(@NotNull Tree tree, @Nullable PropertyState property, long permissions) {
        // EXERCISE: complete PermissionProvider implementation
        return false;
    }

    @Override
    public boolean isGranted(@NotNull String oakPath, @NotNull String jcrActions) {
        // EXERCISE: complete PermissionProvider implementation
        return false;
    }
}
