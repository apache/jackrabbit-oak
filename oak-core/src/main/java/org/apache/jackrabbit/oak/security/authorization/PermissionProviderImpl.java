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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.plugins.name.NamespaceConstants;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
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
import org.apache.jackrabbit.oak.version.VersionConstants;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PermissionProviderImpl... TODO
 */
public class PermissionProviderImpl implements PermissionProvider, AccessControlConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionProviderImpl.class);

    private final ReadOnlyTree rootTree;
    private final SecurityProvider securityProvider;

    private final String workspaceName = "default"; // FIXME: use proper workspace as associated with the root

    private final CompiledPermissions compiledPermissions;

    public PermissionProviderImpl(@Nonnull Root root, @Nonnull Set<Principal> principals,
                                  @Nonnull SecurityProvider securityProvider) {
        this(ReadOnlyTree.createFromRoot(root), principals, securityProvider);
    }

    public PermissionProviderImpl(@Nonnull Tree rootTree, @Nonnull Set<Principal> principals,
                                  @Nonnull SecurityProvider securityProvider) {
        this.rootTree = ReadOnlyTree.createFromRootTree(rootTree);
        this.securityProvider = securityProvider;
        if (principals.contains(SystemPrincipal.INSTANCE) || isAdmin(principals)) {
            compiledPermissions = AllPermissions.getInstance();
        } else {
            String relativePath = PERMISSIONS_STORE_PATH + '/' + workspaceName;
            ReadOnlyTree permissionsTree = getPermissionsRoot(this.rootTree, relativePath);
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
        if (getAccessControlContext().definesTree(tree)) {
            return compiledPermissions.isGranted(Permissions.READ_ACCESS_CONTROL, tree);
        } else if (isVersionContent(tree)) {
            // TODO: add proper implementation
            Tree versionableTree = getVersionableTree(tree);
            return versionableTree != null && compiledPermissions.canRead(versionableTree);
        } else {
            return compiledPermissions.canRead(tree);
        }
    }

    @Override
    public boolean canRead(@Nonnull Tree tree, @Nonnull PropertyState property) {
        if (getAccessControlContext().definesTree(tree)) {
            return compiledPermissions.isGranted(Permissions.READ_ACCESS_CONTROL, tree, property);
        } else if (isVersionContent(tree)) {
            // TODO: add proper implementation
            Tree versionableTree = getVersionableTree(tree);
            return versionableTree != null && compiledPermissions.canRead(versionableTree, property);
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
        if (Permissions.includes(permissions, Permissions.VERSION_MANAGEMENT)) {
            // FIXME: path to check for permission must be adjusted to be
            // FIXME: the one of the versionable node instead of the target parent in case of version-store is affected.
        } else if (Permissions.includes(permissions, Permissions.READ_NODE)) {
            // TODO
        }

        return compiledPermissions.isGranted(permissions, tree);
    }

    @Override
    public boolean isGranted(@Nonnull Tree parent, @Nonnull PropertyState property, long permissions) {
        if (Permissions.includes(permissions, Permissions.VERSION_MANAGEMENT)) {
            // FIXME: path to check for permission must be adjusted to be
            // FIXME: the one of the versionable node instead of the target parent in case of version-store is affected.
        } else if (Permissions.includes(permissions, Permissions.READ_PROPERTY)) {
            // TODO
        }

        return compiledPermissions.isGranted(permissions, parent, property);
    }

    @Override
    public boolean hasPermission(@Nonnull String oakPath, String jcrActions) {
        TreeLocation location = rootTree.getLocation().getChild(oakPath.substring(1));
        long permissions = Permissions.getPermissions(jcrActions, location);

        // TODO
        return false;
    }

    @Override
    public long getPermission(@Nonnull Tree tree, long defaultPermission) {
        String path = tree.getPath();
        long permission;
        if (isNamespaceDefinition(path)) {
            permission = Permissions.NAMESPACE_MANAGEMENT;
        } else if (isNodeTypeDefinition(path)) {
            permission = Permissions.NODE_TYPE_DEFINITION_MANAGEMENT;
        } else if (isVersionContent(tree)) {
            permission = Permissions.VERSION_MANAGEMENT;
        } else if (getPrivilegeContext().definesTree(tree)) {
            permission = Permissions.PRIVILEGE_MANAGEMENT;
        } else if (getAccessControlContext().definesTree(tree)) {
            permission = Permissions.MODIFY_ACCESS_CONTROL;
        } else if (getUserContext().definesTree(tree)) {
            permission = Permissions.USER_MANAGEMENT;
        } else {
            // TODO  - workspace management
            // TODO: identify renaming/move of nodes that only required MODIFY_CHILD_NODE_COLLECTION permission
            permission = defaultPermission;
        }
        return permission;
    }

    @Override
    public long getPermission(@Nonnull Tree parent, @Nonnull PropertyState propertyState, long defaultPermission) {
        String parentPath = parent.getPath();
        String name = propertyState.getName();

        long permission;
        if (JcrConstants.JCR_PRIMARYTYPE.equals(name) || JcrConstants.JCR_MIXINTYPES.equals(name)) {
            // FIXME: distinguish between autocreated and user-supplied modification (?)
            permission = Permissions.NODE_TYPE_MANAGEMENT;
        } else if (isLockProperty(name)) {
            permission = Permissions.LOCK_MANAGEMENT;
        } else if (isNamespaceDefinition(parentPath)) {
            permission = Permissions.NAMESPACE_MANAGEMENT;
        } else if (isNodeTypeDefinition(parentPath)) {
            permission = Permissions.NODE_TYPE_DEFINITION_MANAGEMENT;
        } else if (isVersionProperty(parent, propertyState)) {
            permission = Permissions.VERSION_MANAGEMENT;
        } else if (getPrivilegeContext().definesProperty(parent, propertyState)) {
            permission = Permissions.PRIVILEGE_MANAGEMENT;
        } else if (getAccessControlContext().definesProperty(parent, propertyState)) {
            permission = Permissions.MODIFY_ACCESS_CONTROL;
        } else if (getUserContext().definesProperty(parent, propertyState)) {
            permission = Permissions.USER_MANAGEMENT;
        } else {
            permission = defaultPermission;
        }
        return permission;
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

    @CheckForNull
    private Tree getVersionableTree(Tree versionStoreTree) {
        String locationPath = workspaceName + '/' + REP_VERSIONABLE_PATH;
        String versionablePath = null;
        Tree t = versionStoreTree;
        while (versionablePath == null && !JcrConstants.JCR_VERSIONSTORAGE.equals(t.getName())) {
            if (t.hasChild(REP_VERSIONABLE_INFO)) {
                PropertyState prop = t.getLocation().getChild(locationPath).getProperty();
                if (prop != null) {
                    versionablePath = prop.getValue(Type.PATH);
                }
            }
            t = t.getParent();
        }
        if (versionablePath == null || versionablePath.isEmpty()) {
            log.warn("Unable to determine path of the versionable node.");
            return null;
        } else {
            String relPath = versionablePath.substring(1);
            return rootTree.getLocation().getChild(relPath).getTree();
        }
    }

    // FIXME: versionable-info not detected
    private static boolean isVersionContent(Tree tree) {
        if (tree.isRoot()) {
            return false;
        }
        if (VersionConstants.VERSION_NODE_NAMES.contains(tree.getName())) {
            return true;
        } else if (VersionConstants.VERSION_NODE_TYPE_NAMES.contains(getPrimaryTypeName(tree))) {
            return true;
        } else {
            String path = tree.getPath();
            return VersionConstants.SYSTEM_PATHS.contains(Text.getAbsoluteParent(path, 1));
        }
    }

    // FIXME: versionable-info not detected
    private static boolean isVersionProperty(Tree parent, PropertyState property) {
        if (VersionConstants.VERSION_PROPERTY_NAMES.contains(property.getName())) {
            return true;
        } else {
            return isVersionContent(parent);
        }
    }

    private static boolean isLockProperty(String name) {
        return JcrConstants.JCR_LOCKISDEEP.equals(name) || JcrConstants.JCR_LOCKOWNER.equals(name);
    }

    private static boolean isNamespaceDefinition(String path) {
        return Text.isDescendant(NamespaceConstants.NAMESPACES_PATH, path);
    }

    private static boolean isNodeTypeDefinition(String path) {
        return Text.isDescendant(NodeTypeConstants.NODE_TYPES_PATH, path);
    }

    private Context getUserContext() {
        return securityProvider.getUserConfiguration().getContext();
    }

    private Context getPrivilegeContext() {
        return securityProvider.getPrivilegeConfiguration().getContext();
    }

    private Context getAccessControlContext() {
        return securityProvider.getAccessControlConfiguration().getContext();
    }

    private static String getPrimaryTypeName(Tree tree) {
        PropertyState property = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        if (property != null && !property.isArray()) {
            return property.getValue(Type.STRING);
        } else {
            return null;
        }
    }
}
