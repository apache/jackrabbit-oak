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

import javax.annotation.CheckForNull;
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
    private Status status;

    CugTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull TreePermission parent,
                      @Nonnull CugPermissionProvider permissionProvider) {
        super(tree, type, permissionProvider);
        this.parent = parent;
    }

    CugTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull TreePermission parent,
                      @Nonnull CugPermissionProvider permissionProvider, boolean inCug, boolean canRead, boolean hasNestedCug) {
        super(tree, type, permissionProvider);
        this.parent = parent;
        status = new Status(inCug, canRead, hasNestedCug);
    }

    boolean isInCug() {
        if (status == null) {
            loadStatus();
        }
        return status.inCug;
    }

    boolean isAllow() {
        if (status == null) {
            loadStatus();
        }
        return status.allow;
    }
    
    boolean hasNestedCug() {
        if (status == null) {
            loadStatus();
        }
        return status.hasNested;
    }
    
    private Status getStatus() {
        if (status == null) {
            loadStatus();
        }
        return status;
    }
    
    private void loadStatus() {
        CugTreePermission parentCugPerm = (parent instanceof CugTreePermission) ? (CugTreePermission) parent : null;
        if (neverNested(parentCugPerm)) {
            status = parentCugPerm.getStatus();
        } else {
            // need to load information
            Tree cugTree = CugUtil.getCug(tree);
            if (cugTree != null) {
                status = new Status(true, permissionProvider.isAllow(cugTree), CugUtil.hasNestedCug(cugTree));
            } else if (parentCugPerm != null) {
                status = parentCugPerm.getStatus();
            } else {
                status = Status.FALSE;
            }
        }
    }

    private static boolean neverNested(@CheckForNull CugTreePermission parentCugPerm) {
        if (parentCugPerm != null) {
            Status st = parentCugPerm.status;
            return st != null && st.inCug && !st.hasNested;
        }
        return false;
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

    //--------------------------------------------------------------------------
    private static final class Status {

        private static final Status FALSE = new Status(false, false, false);

        private final boolean inCug;
        private final boolean allow;
        private final boolean hasNested;

        private Status(boolean inCug, boolean allow, boolean hasNested) {
            this.inCug = inCug;
            this.allow = allow;
            this.hasNested = hasNested;
        }
    }
}