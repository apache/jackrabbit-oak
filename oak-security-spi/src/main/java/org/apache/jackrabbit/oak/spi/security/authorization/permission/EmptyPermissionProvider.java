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
package org.apache.jackrabbit.oak.spi.security.authorization.permission;

import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

/**
 * Permission provider implementation that does not grant any permissions.
 */
public final class EmptyPermissionProvider implements PermissionProvider {

    private static final PermissionProvider INSTANCE = new EmptyPermissionProvider();

    private EmptyPermissionProvider() {
    }

    public static PermissionProvider getInstance() {
        return INSTANCE;
    }

    @Override
    public void refresh() {
        // nothing to do
    }

    @Nonnull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        return Collections.emptySet();
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
        return false;
    }

    @Nonnull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return RepositoryPermission.EMPTY;
    }

    @Nonnull
    @Override
    public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
        return TreePermission.EMPTY;
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
        return false;
    }

    @Override
    public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
        return false;
    }
}
