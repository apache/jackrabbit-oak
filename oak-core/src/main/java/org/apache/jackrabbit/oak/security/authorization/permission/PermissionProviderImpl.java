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
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;

public class PermissionProviderImpl implements PermissionProvider, AccessControlConstants, PermissionConstants, AggregatedPermissionProvider {

    private final Root root;

    private final String workspaceName;

    private final AuthorizationConfiguration acConfig;

    private final CompiledPermissions compiledPermissions;

    private Root immutableRoot;

    public PermissionProviderImpl(@Nonnull Root root, @Nonnull String workspaceName, @Nonnull Set<Principal> principals,
                                  @Nonnull AuthorizationConfiguration acConfig) {
        this.root = root;
        this.workspaceName = workspaceName;
        this.acConfig = acConfig;

        immutableRoot = RootFactory.createReadOnlyRoot(root);

        if (principals.contains(SystemPrincipal.INSTANCE) || isAdmin(principals)) {
            compiledPermissions = AllPermissions.getInstance();
        } else {
            compiledPermissions = CompiledPermissionImpl.create(immutableRoot, workspaceName, principals, acConfig);
        }
    }

    @Override
    public void refresh() {
        immutableRoot = RootFactory.createReadOnlyRoot(root);
        compiledPermissions.refresh(immutableRoot, workspaceName);
    }

    @Nonnull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        return compiledPermissions.getPrivileges(getImmutableTree(tree));
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
        return compiledPermissions.hasPrivileges(getImmutableTree(tree), privilegeNames);
    }

    @Nonnull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return compiledPermissions.getRepositoryPermission();
    }

    @Nonnull
    @Override
    public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
        return compiledPermissions.getTreePermission(getImmutableTree(tree), parentPermission);
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
        return compiledPermissions.isGranted(getImmutableTree(tree), property, permissions);
    }

    @Override
    public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
        TreeLocation location = TreeLocation.create(immutableRoot, oakPath);
        boolean isAcContent = acConfig.getContext().definesLocation(location);
        long permissions = Permissions.getPermissions(jcrActions, location, isAcContent);

        boolean isGranted = false;
        PropertyState property = location.getProperty();
        Tree tree = (property == null) ? location.getTree() : location.getParent().getTree();
        if (tree != null) {
            isGranted = isGranted(tree, property, permissions);
        } else if (!isVersionStorePath(oakPath)) {
            isGranted = compiledPermissions.isGranted(oakPath, permissions);
        }
        return isGranted;
    }

    //---------------------------------------< AggregatedPermissionProvider >---
    @Override
    public boolean handles(@Nonnull String path, @Nonnull String jcrAction) {
        return true;
    }

    @Override
    public boolean handles(@Nonnull Tree tree, @Nonnull PrivilegeBits privilegeBits) {
        return true;
    }

    @Override
    public boolean handles(@Nonnull Tree tree, long permission) {
        return true;
    }

    @Override
    public boolean handles(@Nonnull TreePermission treePermission, long permission) {
        return true;
    }

    @Override
    public boolean handlesRepositoryPermissions() {
        return true;
    }

    //--------------------------------------------------------------------------

    private boolean isAdmin(Set<Principal> principals) {
        Set<String> adminNames = acConfig.getParameters().getConfigValue(PARAM_ADMINISTRATIVE_PRINCIPALS, Collections.EMPTY_SET);
        for (Principal principal : principals) {
            if (principal instanceof AdminPrincipal || adminNames.contains(principal.getName())) {
                return true;
            }
        }
        return false;
    }

    private ImmutableTree getImmutableTree(@Nullable Tree tree) {
        if (tree instanceof ImmutableTree) {
            return (ImmutableTree) tree;
        } else {
            return (tree == null) ? null : (ImmutableTree) immutableRoot.getTree(tree.getPath());
        }
    }

    private static boolean isVersionStorePath(@Nonnull String oakPath) {
        if (oakPath.indexOf(JcrConstants.JCR_SYSTEM) == 1) {
            for (String p : VersionConstants.SYSTEM_PATHS) {
                if (oakPath.startsWith(p)) {
                    return true;
                }
            }
        }
        return false;
    }
}
