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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

abstract class AbstractTreePermission implements TreePermission  {

    private final Tree tree;
    private final TreeType type;

    AbstractTreePermission(@NotNull Tree tree, @NotNull TreeType type) {
        this.tree = tree;
        this.type = type;
    }

    abstract PrincipalBasedPermissionProvider getPermissionProvider();

    @NotNull
    Tree getTree() {
        return tree;
    }

    @NotNull
    TreeType getType() {
        return type;
    }

    @Override
    public @NotNull TreePermission getChildPermission(@NotNull String childName, @NotNull NodeState childState) {
        return getPermissionProvider().getTreePermission(childName, childState, this);
    }

    @Override
    public boolean canRead() {
        long permission = (type == TreeType.ACCESS_CONTROL) ? Permissions.READ_ACCESS_CONTROL : Permissions.READ_NODE;
        return getPermissionProvider().isGranted(tree, null, permission);
    }

    @Override
    public boolean canRead(@NotNull PropertyState property) {
        long permission = (type == TreeType.ACCESS_CONTROL) ? Permissions.READ_ACCESS_CONTROL : Permissions.READ_PROPERTY;
        return getPermissionProvider().isGranted(tree, property, permission);
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
        return getPermissionProvider().isGranted(tree, null, permissions);
    }

    @Override
    public boolean isGranted(long permissions, @NotNull PropertyState property) {
        return getPermissionProvider().isGranted(tree, property, permissions);
    }
}