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
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;

/**
 * {@code TreePermission} implementation for all tree located within one of the
 * supported paths which may or may not contain a CUG.
 */
final class CugTreePermission extends AbstractTreePermission implements CugConstants {

    private final TreePermission parent;
    private Boolean inCug;
    private Boolean allow;

    CugTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull TreePermission parent,
                      @Nonnull CugPermissionProvider permissionProvider) {
        super(tree, type, permissionProvider);
        this.parent = parent;
    }

    CugTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull TreePermission parent,
                      @Nonnull CugPermissionProvider permissionProvider, boolean inCug, boolean canRead) {
        super(tree, type, permissionProvider);
        this.parent = parent;
        this.inCug = inCug;
        this.allow = canRead;
    }

    boolean isInCug() {
        if (inCug == null) {
            loadCug();
        }
        return inCug;
    }

    boolean isAllow() {
        if (allow == null) {
            loadCug();
        }
        return allow;
    }

    private void loadCug() {
        Tree cugTree = CugUtil.getCug(tree);
        if (cugTree != null) {
            inCug = true;
            allow = permissionProvider.isAllow(cugTree);
        } else if (parent instanceof CugTreePermission) {
            inCug = ((CugTreePermission) parent).isInCug();
            allow = ((CugTreePermission) parent).isAllow();
        } else {
            inCug = false;
            allow = false;
        }
    }

    //-----------------------------------------------------< TreePermission >---

    @Override
    public boolean canRead() {
        return isAllow();
    }

    @Override
    public boolean canRead(@Nonnull PropertyState property) {
        return isAllow();
    }

    @Override
    public boolean canReadAll() {
        return false;
    }

    @Override
    public boolean canReadProperties() {
        return isAllow();
    }

    @Override
    public boolean isGranted(long permissions) {
        return permissions == Permissions.READ_NODE && isAllow();
    }

    @Override
    public boolean isGranted(long permissions, @Nonnull PropertyState property) {
        return permissions == Permissions.READ_PROPERTY && isAllow();
    }
}