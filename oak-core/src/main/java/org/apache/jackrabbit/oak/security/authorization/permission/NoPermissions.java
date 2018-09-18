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

import java.util.Collections;
import java.util.Set;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of the {@code CompiledPermission} interface that denies all permissions.
 */
final class NoPermissions implements CompiledPermissions {

    private static final CompiledPermissions INSTANCE = new NoPermissions();

    private NoPermissions() {
    }

    static CompiledPermissions getInstance() {
        return INSTANCE;
    }

    @Override
    public void refresh(@NotNull Root root, @NotNull String workspaceName) {
        // nop
    }

    @NotNull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return RepositoryPermission.EMPTY;
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreePermission parentPermission) {
        return TreePermission.EMPTY;
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreeType type, @NotNull TreePermission parentPermission) {
        return TreePermission.EMPTY;
    }

    @Override
    public boolean isGranted(@NotNull Tree tree, @Nullable PropertyState property, long permissions) {
        return false;
    }

    @Override
    public boolean isGranted(@NotNull String path, long permissions) {
        return false;
    }

    @NotNull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        return Collections.emptySet();
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @NotNull String... privilegeNames) {
        return false;
    }
}
