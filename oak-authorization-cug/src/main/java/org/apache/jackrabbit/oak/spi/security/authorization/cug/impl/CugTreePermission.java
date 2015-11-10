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
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;

/**
 * {@code TreePermission} implementation for all items located with a CUG.
 */
final class CugTreePermission extends AbstractTreePermission {

    private final boolean allow;

    CugTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, boolean allow, @Nonnull CugPermissionProvider permissionProvider) {
        super(tree, type, permissionProvider);
        this.allow = allow;
    }

    CugTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull CugTreePermission parent) {
        super(tree, type, parent);
        this.allow = parent.allow;
    }

    //-----------------------------------------------------< TreePermission >---

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
        return allow;
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