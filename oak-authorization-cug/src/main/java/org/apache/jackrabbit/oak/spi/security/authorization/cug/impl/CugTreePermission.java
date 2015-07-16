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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code TreePermission} implementation for all items located with a CUG.
 */
final class CugTreePermission implements TreePermission {

    private final Tree tree;
    private final boolean allow;
    private final PermissionProvider permissionProvider;

    CugTreePermission(@Nonnull Tree tree, boolean allow, @Nonnull PermissionProvider permissionProvider) {
        this.tree = tree;
        this.allow = allow;
        this.permissionProvider = permissionProvider;
    }

    CugTreePermission(@Nonnull Tree tree, @Nonnull CugTreePermission parent) {
        this.tree = tree;
        this.allow = parent.allow;
        this.permissionProvider = parent.permissionProvider;
    }

    //-----------------------------------------------------< TreePermission >---
    @Nonnull
    @Override
    public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
        return permissionProvider.getTreePermission(tree.getChild(childName), this);
    }

    @Override
    public boolean canRead() {
        return allow;
    }

    @Override
    public boolean canRead(@Nonnull PropertyState property) {
        return allow;
    }

    @Override
    public boolean canReadAll() {
        return false;
    }

    @Override
    public boolean canReadProperties() {
        return false;
    }

    @Override
    public boolean isGranted(long permissions) {
        return allow && permissions == Permissions.READ_NODE;
    }

    @Override
    public boolean isGranted(long permissions, @Nonnull PropertyState property) {
        return allow && permissions == Permissions.READ_PROPERTY;
    }
}