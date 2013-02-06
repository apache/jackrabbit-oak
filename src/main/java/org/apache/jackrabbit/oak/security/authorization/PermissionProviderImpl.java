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
package org.apache.jackrabbit.oak.security.authorization;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ReadOnlyRoot;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.security.authorization.permission.AllPermissions;
import org.apache.jackrabbit.oak.security.authorization.permission.CompiledPermissionImpl;
import org.apache.jackrabbit.oak.security.authorization.permission.CompiledPermissions;
import org.apache.jackrabbit.oak.security.authorization.permission.NoPermissions;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.Permissions;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PermissionProviderImpl... TODO
 * <p/>
 * FIXME: permissions need to be refreshed if something changes in the permission tree
 * FIXME: define read/write access patterns on version-store content
 * FIXME: proper access permissions on activity-store and configuration-store
 */
public class PermissionProviderImpl implements PermissionProvider, AccessControlConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionProviderImpl.class);

    private final Root root;
    private final Context acContext;

    private final String workspaceName = "default"; // FIXME: use proper workspace as associated with the root

    private final CompiledPermissions compiledPermissions;

    public PermissionProviderImpl(@Nonnull Root root, @Nonnull Set<Principal> principals,
                                  @Nonnull SecurityProvider securityProvider) {
        this.root = new ReadOnlyRoot(root);
        this.acContext = securityProvider.getAccessControlConfiguration().getContext();
        if (principals.contains(SystemPrincipal.INSTANCE) || isAdmin(principals)) {
            compiledPermissions = AllPermissions.getInstance();
        } else {
            String relativePath = PERMISSIONS_STORE_PATH + '/' + workspaceName;
            ReadOnlyTree rootTree = ReadOnlyTree.createFromRoot(root);
            ReadOnlyTree permissionsTree = getPermissionsRoot(rootTree, relativePath);
            if (permissionsTree == null) {
                compiledPermissions = NoPermissions.getInstance();
            } else {
                compiledPermissions = new CompiledPermissionImpl(permissionsTree, principals);
            }
        }
    }

    @Nonnull
    @Override
    public Set<String> getPrivilegeNames(@Nullable Tree tree) {
        // TODO
        return Collections.emptySet();
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, String... privilegeNames) {
        // TODO
        return false;
    }

    @Override
    public boolean canRead(@Nonnull Tree tree) {
        if (acContext.definesTree(tree)) {
            return compiledPermissions.isGranted(tree, Permissions.READ_ACCESS_CONTROL);
        } else if (isVersionContent(tree)) {
            return canReadVersionContent(tree, null);
        } else {
            return compiledPermissions.canRead(tree);
        }
    }

    @Override
    public boolean canRead(@Nonnull Tree tree, @Nonnull PropertyState property) {
        if (acContext.definesTree(tree)) {
            return compiledPermissions.isGranted(tree, property, Permissions.READ_ACCESS_CONTROL);
        } else if (isVersionContent(tree)) {
            return canReadVersionContent(tree, property);
        } else {
            return compiledPermissions.canRead(tree, property);
        }
    }

    @Override
    public boolean isGranted(long permissions) {
        return compiledPermissions.isGranted(permissions);
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, long permissions) {
        if (isVersionContent(tree)) {
            return compiledPermissions.isGranted(getVersionablePath(tree, null), permissions);
        } else {
            return compiledPermissions.isGranted(tree, permissions);
        }
    }

    @Override
    public boolean isGranted(@Nonnull Tree parent, @Nonnull PropertyState property, long permissions) {
        if (isVersionContent(parent)) {
            return compiledPermissions.isGranted(getVersionablePath(parent, property), permissions);
        } else {
            return compiledPermissions.isGranted(parent, property, permissions);
        }
    }

    @Override
    public boolean hasPermission(@Nonnull String oakPath, String jcrActions) {
        TreeLocation location = root.getLocation(oakPath);
        long permissions = Permissions.getPermissions(jcrActions, location);
        if (!location.exists()) {
            // TODO: deal with version content
            return compiledPermissions.isGranted(oakPath, permissions);
        } else if (location.getProperty() != null) {
            return isGranted(location.getTree(), location.getProperty(), permissions);
        } else {
            return isGranted(location.getTree(), permissions);
        }
    }

    //--------------------------------------------------------------------------

    private static boolean isAdmin(Set<Principal> principals) {
        for (Principal principal : principals) {
            if (principal instanceof AdminPrincipal) {
                return true;
            }
        }
        return false;
    }

    @CheckForNull
    private static ReadOnlyTree getPermissionsRoot(ReadOnlyTree rootTree, String relativePath) {
        Tree tree = rootTree.getLocation().getChild(relativePath).getTree();
        return (tree == null) ? null : (ReadOnlyTree) tree;
    }

    private boolean canReadVersionContent(@Nonnull Tree versionStoreTree, @Nullable PropertyState property) {
        String versionablePath = getVersionablePath(versionStoreTree, property);
        if (versionablePath != null) {
            long permission = (property == null) ? Permissions.READ_NODE : Permissions.READ_PROPERTY;
            return compiledPermissions.isGranted(versionablePath, permission);
        } else {
            return false;
        }
    }

    @CheckForNull
    private String getVersionablePath(@Nonnull Tree versionStoreTree, @Nullable PropertyState property) {
        String versionablePath = null;
        Tree t = versionStoreTree;
        while (!JcrConstants.JCR_SYSTEM.equals(t.getName())) {
            if (JcrConstants.NT_VERSIONHISTORY.equals(TreeUtil.getPrimaryTypeName(t))) {
                PropertyState prop = t.getProperty(workspaceName);
                if (prop != null) {
                    versionablePath = prop.getValue(Type.PATH);
                    if (t != versionStoreTree) {
                        String rel = PathUtils.relativize(t.getPath(), versionStoreTree.getPath());
                        String propName = (property == null) ? "" : property.getName();
                        versionablePath = PathUtils.concat(versionablePath, rel, propName);
                    }
                }
                break;
            }// FIXME: handle activities and configurations
            t = t.getParent();
        }

        if (versionablePath == null || versionablePath.length() == 0) {
            log.warn("Unable to determine path of the versionable node.");
        }
        return Strings.emptyToNull(versionablePath);
    }

    private static boolean isVersionContent(@Nonnull Tree tree) {
        if (tree.isRoot()) {
            return false;
        }
        if (VersionConstants.VERSION_NODE_NAMES.contains(tree.getName())) {
            return true;
        } else if (VersionConstants.VERSION_NODE_TYPE_NAMES.contains(TreeUtil.getPrimaryTypeName(tree))) {
            return true;
        } else {
            return isVersionContent(tree.getPath());
        }
    }

    private static boolean isVersionContent(@Nonnull String path) {
        return VersionConstants.SYSTEM_PATHS.contains(Text.getAbsoluteParent(path, 1));
    }
}
