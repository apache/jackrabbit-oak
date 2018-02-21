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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
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

public class PermissionProviderImpl implements PermissionProvider, AccessControlConstants, PermissionConstants, AggregatedPermissionProvider {

    private final Root root;

    private final String workspaceName;

    private final Set<Principal> principals;

    private final RestrictionProvider restrictionProvider;

    private final ConfigurationParameters options;

    private final Context ctx;

    private final RootProvider rootProvider;

    private CompiledPermissions compiledPermissions;

    private Root immutableRoot;

    public PermissionProviderImpl(@Nonnull Root root,
                                  @Nonnull String workspaceName,
                                  @Nonnull Set<Principal> principals,
                                  @Nonnull RestrictionProvider restrictionProvider,
                                  @Nonnull ConfigurationParameters options,
                                  @Nonnull Context ctx,
                                  @Nonnull RootProvider rootProvider) {
        this.root = root;
        this.workspaceName = workspaceName;
        this.principals = principals;
        this.restrictionProvider = restrictionProvider;
        this.options = options;
        this.ctx = ctx;
        this.rootProvider = rootProvider;

        immutableRoot = rootProvider.createReadOnlyRoot(root);
    }

    @Override
    public void refresh() {
        immutableRoot = rootProvider.createReadOnlyRoot(root);
        getCompiledPermissions().refresh(immutableRoot, workspaceName);
    }

    @Nonnull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        return getCompiledPermissions().getPrivileges(PermissionUtil.getImmutableTree(tree, immutableRoot));
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
        return getCompiledPermissions().hasPrivileges(PermissionUtil.getImmutableTree(tree, immutableRoot), privilegeNames);
    }

    @Nonnull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return getCompiledPermissions().getRepositoryPermission();
    }

    @Nonnull
    @Override
    public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
        return getCompiledPermissions().getTreePermission(PermissionUtil.getImmutableTree(tree, immutableRoot), parentPermission);
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
        return getCompiledPermissions().isGranted(PermissionUtil.getImmutableTree(tree, immutableRoot), property, permissions);
    }

    @Override
    public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
        TreeLocation location = TreeLocation.create(immutableRoot, oakPath);
        boolean isAcContent = ctx.definesLocation(location);
        long permissions = Permissions.getPermissions(jcrActions, location, isAcContent);

        return isGranted(location, oakPath, permissions);
    }

    //---------------------------------------< AggregatedPermissionProvider >---
    @Nonnull
    @Override
    public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
        return (privilegeBits != null) ? privilegeBits : new PrivilegeBitsProvider(immutableRoot).getBits(PrivilegeConstants.JCR_ALL);
    }

    @Override
    public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
        return permissions;
    }

    @Override
    public long supportedPermissions(@Nonnull TreeLocation location, long permissions) {
        return permissions;
    }

    @Override
    public long supportedPermissions(@Nonnull TreePermission treePermission, @Nullable PropertyState property, long permissions) {
        return permissions;
    }

    @Override
    public boolean isGranted(@Nonnull TreeLocation location, long permissions) {
        return isGranted(location, location.getPath(), permissions);
    }

    @Nonnull
    @Override
    public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull TreePermission parentPermission) {
        return getCompiledPermissions().getTreePermission(PermissionUtil.getImmutableTree(tree, immutableRoot), type, parentPermission);
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
                        restrictionProvider, options, ctx);
            }
            compiledPermissions = cp;
        }
        return cp;
    }

    protected PermissionStore getPermissionStore(Root root, String workspaceName,
            RestrictionProvider restrictionProvider) {
        return new PermissionStoreImpl(root, workspaceName, restrictionProvider);
    }

    private static boolean isVersionStorePath(@Nonnull String oakPath) {
        return oakPath.startsWith(VersionConstants.VERSION_STORE_PATH);
    }

    private boolean isGranted(@Nonnull TreeLocation location, @Nonnull String oakPath, long permissions) {
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
