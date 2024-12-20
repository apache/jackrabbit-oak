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
package org.apache.jackrabbit.oak.exercise.security.authorization.models.simplifiedroles;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.tree.ReadOnly;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.security.Principal;
import java.util.Set;

class ThreeRolesPermissionProvider implements AggregatedPermissionProvider, ThreeRolesConstants {

    private static final PrivilegeBits SUPPORTED_PRIVBITS = PrivilegeBits.getInstance(
            PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ),
            PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_WRITE),
            PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_VERSION_MANAGEMENT),
            PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ_ACCESS_CONTROL),
            PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL));

    private final Root root;
    private final Set<String> principalNames;
    private final String supportedPath;
    private final Context ctx;
    private final RootProvider rootProvider;

    private Root readOnlyRoot;

    ThreeRolesPermissionProvider(@NotNull Root root, Set<Principal> principals,
                                 @NotNull String supportedPath, @NotNull Context ctx,
                                 @NotNull RootProvider rootProvider) {
        this.root = root;
        this.principalNames = CollectionUtils.toSet(Iterables.transform(principals, Principal::getName));
        this.supportedPath = supportedPath;
        this.ctx = ctx;
        this.rootProvider = rootProvider;

        this.readOnlyRoot = rootProvider.createReadOnlyRoot(root);
    }

    @NotNull
    @Override
    public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
        if (tree == null) {
            return PrivilegeBits.EMPTY;
        }

        PrivilegeBits pb;
        if (privilegeBits == null) {
            pb = SUPPORTED_PRIVBITS;
        } else {
            pb = PrivilegeBits.getInstance(privilegeBits);
            pb.retain(SUPPORTED_PRIVBITS);
        }

        if (pb.isEmpty() || !Utils.isSupportedPath(supportedPath, tree.getPath())) {
            return PrivilegeBits.EMPTY;
        } else {
            return pb;
        }
    }

    @Override
    public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
        if (tree == null) {
            // repository level permissions are not supported
            return Permissions.NO_PERMISSION;
        }

        long supported = permissions & SUPPORTED_PERMISSIONS;
        if (supported != Permissions.NO_PERMISSION && Utils.isSupportedPath(supportedPath, tree.getPath())) {
            return supported;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public long supportedPermissions(@NotNull TreeLocation location, long permissions) {
        long supported = permissions & SUPPORTED_PERMISSIONS;
        if (supported != Permissions.NO_PERMISSION && Utils.isSupportedPath(supportedPath, location.getPath())) {
            return supported;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public long supportedPermissions(@NotNull TreePermission treePermission, @Nullable PropertyState property, long permissions) {
        long supported = permissions & SUPPORTED_PERMISSIONS;
        if (supported != Permissions.NO_PERMISSION && (treePermission instanceof ThreeRolesTreePermission)) {
            return supported;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public boolean isGranted(@NotNull TreeLocation location, long permissions) {
        if (Utils.isSupportedPath(supportedPath, location.getPath())) {
            TreePermission tp = getTreePermission(location);

            PropertyState property = location.getProperty();
            return (property == null) ? tp.isGranted(permissions) : tp.isGranted(permissions, property);
        } else {
            return false;
        }
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreeType type, @NotNull TreePermission parentPermission) {
        // EXERCISE : currently this implementation ignores TreeType -> complete implementation
        return getTreePermission(tree, parentPermission);
    }

    @Override
    public void refresh() {
        this.readOnlyRoot = rootProvider.createReadOnlyRoot(root);

    }

    @NotNull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        if (tree != null) {
            TreePermission tp = getTreePermission(getReadOnlyTree(tree));
            if (tp instanceof ThreeRolesTreePermission) {
                return ((ThreeRolesTreePermission) tp).getRole().getPrivilegeNames();
            }
        }

        return Set.of();
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @NotNull String... privilegeNames) {
        return getPrivileges(tree).containsAll(CollectionUtils.toSet(privilegeNames));
    }

    @NotNull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return RepositoryPermission.EMPTY;
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreePermission parentPermission) {
        if (parentPermission instanceof ThreeRolesTreePermission) {
            // nested policies are not supported => within a given tree defined
            // by a ThreeRolePolicy all items share the same permission setup.
            return parentPermission;
        }

        String path = tree.getPath();
        if (Utils.isSupportedPath(supportedPath, path)) {
            Tree t = getReadOnlyTree(tree);
            if (t.hasChild(REP_3_ROLES_POLICY)) {
                return new ThreeRolesTreePermission(getRole(t), ctx.definesContextRoot(t));
            } else {
                return TreePermission.EMPTY;
            }
        } else if (isAncestor(path)) {
            return TreePermission.EMPTY;
        } else {
            return TreePermission.NO_RECOURSE;
        }
    }

    @Override
    public boolean isGranted(@NotNull Tree tree, @Nullable PropertyState property, long permissions) {
        if (Utils.isSupportedPath(supportedPath, tree.getPath())) {
            TreePermission tp = getTreePermission(tree);
            return (property == null) ? tp.isGranted(permissions) : tp.isGranted(permissions, property);
        } else {
            return false;
        }
    }

    @Override
    public boolean isGranted(@NotNull String oakPath, @NotNull String jcrActions) {
        TreeLocation tl = TreeLocation.create(readOnlyRoot, oakPath);
        long permissions = Permissions.getPermissions(jcrActions, tl, ctx.definesLocation(tl));

        return isGranted(tl, permissions);
    }

    //--------------------------------------------------------------------------

    private boolean isAncestor(@NotNull String path) {
        return Text.isDescendant(path, supportedPath);
    }

    private Role getRole(@NotNull Tree tree) {
        Tree policy = tree.getChild(REP_3_ROLES_POLICY);
        if (policy.exists()) {
            if (containsAny(policy, REP_OWNERS)) {
                return Role.OWNER;
            } else if (containsAny(policy, REP_EDITORS)) {
                return Role.EDITOR;
            } else if (containsAny(policy, REP_READERS)) {
                return Role.READER;
            }
        }
        return Role.NONE;
    }

    private boolean containsAny(@NotNull Tree policyTree, @NotNull String propName) {
        Iterable<String> names = TreeUtil.getStrings(policyTree, propName);
        if (names != null) {
            for (String principalName : names) {
                if (principalNames.contains(principalName)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Tree getReadOnlyTree(@NotNull Tree tree) {
        if (tree instanceof ReadOnly) {
            return tree;
        } else {
            return readOnlyRoot.getTree(tree.getPath());
        }
    }

    private TreePermission getTreePermission(@NotNull Tree readOnlyTree) {
        Tree t = readOnlyTree;
        while (Utils.isSupportedPath(supportedPath, t.getPath())) {
            if (t.hasChild(REP_3_ROLES_POLICY)) {
                return new ThreeRolesTreePermission(getRole(t), ctx.definesContextRoot(t));
            }
            t = t.getParent();
        }
        return TreePermission.EMPTY;
    }

    private TreePermission getTreePermission(@NotNull TreeLocation location) {
        TreeLocation l = location;
        while (Utils.isSupportedPath(supportedPath, l.getPath())) {
            Tree tree = location.getTree();
            if (tree != null) {
                return getTreePermission(getReadOnlyTree(tree));
            }
            l = l.getParent();
        }
        return TreePermission.EMPTY;
    }
}
