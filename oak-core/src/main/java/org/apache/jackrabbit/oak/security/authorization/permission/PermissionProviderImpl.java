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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.security.Principal;
import java.util.Set;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.security.authorization.ProviderCtx;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PermissionProviderImpl implements PermissionProvider, AccessControlConstants, PermissionConstants, AggregatedPermissionProvider {

    private final Root root;

    private final String workspaceName;

    private final Set<Principal> principals;

    private final RestrictionProvider restrictionProvider;

    private final ConfigurationParameters options;

    private final Context ctx;

    private final ProviderCtx providerCtx;

    private CompiledPermissions compiledPermissions;

    private Root immutableRoot;

    public PermissionProviderImpl(@NotNull Root root,
                                  @NotNull String workspaceName,
                                  @NotNull Set<Principal> principals,
                                  @NotNull RestrictionProvider restrictionProvider,
                                  @NotNull ConfigurationParameters options,
                                  @NotNull Context ctx,
                                  @NotNull ProviderCtx providerCtx) {
        this.root = root;
        this.workspaceName = workspaceName;
        this.principals = principals;
        this.restrictionProvider = restrictionProvider;
        this.options = options;
        this.ctx = ctx;
        this.providerCtx = providerCtx;

        immutableRoot = providerCtx.getRootProvider().createReadOnlyRoot(root);
    }

    @Override
    public void refresh() {
        immutableRoot = providerCtx.getRootProvider().createReadOnlyRoot(root);
        getCompiledPermissions().refresh(immutableRoot, workspaceName);
    }

    @NotNull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        return getCompiledPermissions().getPrivileges(PermissionUtil.getReadOnlyTreeOrNull(tree, immutableRoot));
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @NotNull String... privilegeNames) {
        return getCompiledPermissions().hasPrivileges(PermissionUtil.getReadOnlyTreeOrNull(tree, immutableRoot), privilegeNames);
    }

    @NotNull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return getCompiledPermissions().getRepositoryPermission();
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreePermission parentPermission) {
        return getCompiledPermissions().getTreePermission(PermissionUtil.getReadOnlyTree(tree, immutableRoot), parentPermission);
    }

    @Override
    public boolean isGranted(@NotNull Tree tree, @Nullable PropertyState property, long permissions) {
        return getCompiledPermissions().isGranted(PermissionUtil.getReadOnlyTree(tree, immutableRoot), property, permissions);
    }

    @Override
    public boolean isGranted(@NotNull String oakPath, @NotNull String jcrActions) {
        TreeLocation location = TreeLocation.create(immutableRoot, oakPath);
        boolean isAcContent = ctx.definesLocation(location);
        long permissions = Permissions.getPermissions(jcrActions, location, isAcContent);

        return isGranted(location, oakPath, permissions);
    }

    //---------------------------------------< AggregatedPermissionProvider >---
    @NotNull
    @Override
    public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
        return (privilegeBits != null) ? privilegeBits : new PrivilegeBitsProvider(immutableRoot).getBits(PrivilegeConstants.JCR_ALL);
    }

    @Override
    public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
        return permissions;
    }

    @Override
    public long supportedPermissions(@NotNull TreeLocation location, long permissions) {
        return permissions;
    }

    @Override
    public long supportedPermissions(@NotNull TreePermission treePermission, @Nullable PropertyState property, long permissions) {
        return permissions;
    }

    @Override
    public boolean isGranted(@NotNull TreeLocation location, long permissions) {
        return isGranted(location, location.getPath(), permissions);
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreeType type, @NotNull TreePermission parentPermission) {
        return getCompiledPermissions().getTreePermission(PermissionUtil.getReadOnlyTree(tree, immutableRoot), type, parentPermission);
    }

    //--------------------------------------------------------------------------

    private CompiledPermissions getCompiledPermissions() {
        CompiledPermissions cp = compiledPermissions;
        if (cp == null) {
            if (PermissionUtil.isAdminOrSystem(principals, options)) {
                cp = AllPermissions.getInstance();
            } else {
                cp = CompiledPermissionImpl.create(immutableRoot, workspaceName,
                        getPermissionStore(immutableRoot, workspaceName, restrictionProvider), principals,
                        options, ctx, providerCtx);
            }
            compiledPermissions = cp;
        }
        return cp;
    }

    @NotNull
    protected PermissionStore getPermissionStore(@NotNull Root root, @NotNull String workspaceName, @NotNull RestrictionProvider restrictionProvider) {
        return new PermissionStoreImpl(root, workspaceName, restrictionProvider);
    }

    private static boolean isVersionStorePath(@NotNull String oakPath) {
        return oakPath.startsWith(VersionConstants.VERSION_STORE_PATH);
    }

    private boolean isGranted(@NotNull TreeLocation location, @NotNull String oakPath, long permissions) {
        boolean isGranted = false;
        PropertyState property = location.getProperty();
        Tree tree = (property == null) ? location.getTree() : location.getParent().getTree();
        if (tree != null) {
            isGranted = isGranted(tree, property, permissions);
        } else if (!isVersionStorePath(location.getPath())) {
            isGranted = getCompiledPermissions().isGranted(oakPath, permissions);
        }
        return isGranted;
    }
}
