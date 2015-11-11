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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;

abstract class AbstractTreePermission implements TreePermission {

    final Tree tree;
    final TreeType type;
    final CugPermissionProvider permissionProvider;

    AbstractTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull CugPermissionProvider permissionProvider) {
        this.tree = tree;
        this.type = type;
        this.permissionProvider = permissionProvider;
    }

    @Nonnull
    @Override
    public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
        return permissionProvider.getTreePermission(tree, type, childName, childState, this);
    }
}