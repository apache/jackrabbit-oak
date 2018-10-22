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
package org.apache.jackrabbit.oak.core;

import java.util.Set;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Dummy permission provider implementation that grants read access to all trees
 * that have a name that isn't equal to {@link #NAME_NON_ACCESSIBLE}.
 */
final class TestPermissionProvider implements PermissionProvider {

    static final String NAME_ACCESSIBLE = "accessible";
    static final String NAME_NON_ACCESSIBLE = "notAccessible";
    static final String NAME_NON_EXISTING = "nonExisting";

    boolean canReadAll = false;
    boolean canReadProperties = false;

    boolean denyAll;

    private TreePermission getTreePermission(@NotNull String name) {
        if (denyAll) {
            return TreePermission.EMPTY;
        } else {
            return new TreePermission() {
                @NotNull
                @Override
                public TreePermission getChildPermission(@NotNull String childName, @NotNull NodeState childState) {
                    return getTreePermission(childName);
                }

                @Override
                public boolean canRead() {
                    return canReadAll || !name.contains(NAME_NON_ACCESSIBLE);
                }

                @Override
                public boolean canRead(@NotNull PropertyState property) {
                    return canReadProperties || !property.getName().contains(NAME_NON_ACCESSIBLE);
                }

                @Override
                public boolean canReadAll() {
                    return canReadAll;
                }

                @Override
                public boolean canReadProperties() {
                    return canReadProperties;
                }

                @Override
                public boolean isGranted(long permissions) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean isGranted(long permissions, @NotNull PropertyState property) {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    @Override
    public void refresh() {
        denyAll = !denyAll;
    }

    @NotNull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @NotNull String... privilegeNames) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreePermission parentPermission) {
        return getTreePermission(tree.getName());
    }

    @Override
    public boolean isGranted(@NotNull Tree tree, @Nullable PropertyState property, long permissions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isGranted(@NotNull String oakPath, @NotNull String jcrActions) {
        throw new UnsupportedOperationException();
    }
}
