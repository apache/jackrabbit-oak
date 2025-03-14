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
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

abstract class AbstractTreePermission implements TreePermission  {

    private final Tree tree;
    private final TreeType type;
    private PermissionStatus permissionStatus;

    AbstractTreePermission(@NotNull Tree tree, @NotNull TreeType type) {
        this.tree = tree;
        this.type = type;
    }
    
    AbstractTreePermission(@NotNull Tree tree, @NotNull TreeType type, @NotNull TreePermission parentPermission) {
        this.tree = tree;
        this.type = type;
        if (parentPermission instanceof AbstractTreePermission) {
            AbstractTreePermission absParentPermission = (AbstractTreePermission) parentPermission;
            ensurePermissionStatusFromParent(absParentPermission.getPermissionStatus());
        }
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
    
    private PermissionStatus getPermissionStatus() {
        return permissionStatus;
    }

    @Override
    public @NotNull TreePermission getChildPermission(@NotNull String childName, @NotNull NodeState childState) {
        return getPermissionProvider().getTreePermission(childName, childState, this);
    }

    @Override
    public boolean canRead() {
        ensurePermissionStatus();
        if (permissionStatus.hasRestrictions()) {
            long permission = (type == TreeType.ACCESS_CONTROL) ? Permissions.READ_ACCESS_CONTROL : Permissions.READ_NODE;
            return getPermissionProvider().isGranted(tree, null, permission);
        } else {
            return (type == TreeType.ACCESS_CONTROL) ? permissionStatus.canReadAccessControl() : permissionStatus.canReadContent();
        }
    }

    @Override
    public boolean canRead(@NotNull PropertyState property) {
        ensurePermissionStatus();
        if (permissionStatus.hasRestrictions()) {
            long permission = (type == TreeType.ACCESS_CONTROL) ? Permissions.READ_ACCESS_CONTROL : Permissions.READ_PROPERTY;
            return getPermissionProvider().isGranted(tree, property, permission);
        } else {
            return (type == TreeType.ACCESS_CONTROL) ? permissionStatus.canReadAccessControl() : permissionStatus.canReadProperties();
        }
    }

    @Override
    public boolean canReadAll() {
        ensurePermissionStatus();
        // As principal based permissions does not allow deny entries then if the read permission is given in this node
        // it is also granted for all child nodes and properties however this couldn't be true depending on the restrictions
        if (!permissionStatus.hasRestrictions()) {
            return permissionStatus.canReadAll();
        }
        return false;
    }

    @Override
    public boolean canReadProperties() {
        return canReadAll();
    }

    @Override
    public boolean isGranted(long permissions) {
        return getPermissionProvider().isGranted(tree, null, permissions);
    }

    @Override
    public boolean isGranted(long permissions, @NotNull PropertyState property) {
        return getPermissionProvider().isGranted(tree, property, permissions);
    }

    private void ensurePermissionStatus() {
        if (getPermissionProvider().hasRestrictions()) {
            //Create a read status that indicates that permissions has to be calculated for this tree
            permissionStatus = new PermissionStatus();
        }
        if (permissionStatus == null) {
            PrivilegeBits bits = getPermissionProvider().getGrantedPrivilegeBits(tree);
            long permissions = PrivilegeBits.calculatePermissions(bits, PrivilegeBits.EMPTY, true);
            permissionStatus = new PermissionStatus(Permissions.includes(permissions, Permissions.READ_NODE), Permissions.includes(permissions, Permissions.READ_PROPERTY), Permissions.includes(permissions, Permissions.READ_ACCESS_CONTROL));
        }
    }

    private void ensurePermissionStatusFromParent(PermissionStatus parentStatus) {
        if (parentStatus == null) { //In case parent permission has not been calculated
            return;
        }
        
        if (parentStatus.hasRestrictions()) {
            this.permissionStatus = new PermissionStatus();
        } else {
            // Parent permission is only used in case of granting permissions, if it's not granted then the permission has to be calculated
            PrivilegeBits bits = getPermissionProvider().getGrantedPrivilegeBits(tree);
            long permissions = PrivilegeBits.calculatePermissions(bits, PrivilegeBits.EMPTY, true);
            
            boolean readContent = parentStatus.canReadContent() || Permissions.includes(permissions, Permissions.READ_NODE);
            boolean readProperties = parentStatus.canReadProperties() || Permissions.includes(permissions, Permissions.READ_PROPERTY);
            boolean readAccessControl = parentStatus.canReadAccessControl() || Permissions.includes(permissions, Permissions.READ_ACCESS_CONTROL);
            
            this.permissionStatus = new PermissionStatus(readContent, readProperties, readAccessControl);
        }
    }

    private static class PermissionStatus {
        
        private final boolean permissionWithRestrictions;
        private final boolean readContent;
        private final boolean readProperties;
        private final boolean readAccessControl;

        public PermissionStatus(boolean readContent, boolean readProperty, boolean readAC) {
            this.permissionWithRestrictions = false;
            this.readContent = readContent;
            this.readProperties = readProperty;
            this.readAccessControl = readAC;
        }

        public PermissionStatus() {
            this.permissionWithRestrictions = true;
            this.readContent = false;
            this.readProperties = false;
            this.readAccessControl = false;
        }
        
        public boolean hasRestrictions() {
            return permissionWithRestrictions;
        }

        public boolean canReadAll() {
            return readContent && readProperties && readAccessControl;
        }

        public boolean canReadContent() {
            return readContent;
        }

        public boolean canReadProperties() {
            return readProperties;
        }

        public boolean canReadAccessControl() {
            return readAccessControl;
        }
    }
}