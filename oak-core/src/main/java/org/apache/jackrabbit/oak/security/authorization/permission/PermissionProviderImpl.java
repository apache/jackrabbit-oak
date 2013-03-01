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
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.Permissions;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * PermissionProviderImpl... TODO
 * <p/>
 * FIXME: define read/write access patterns on version-store content
 * FIXME: proper access permissions on activity-store and configuration-store
 */
public class PermissionProviderImpl implements PermissionProvider, AccessControlConstants, PermissionConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionProviderImpl.class);

    private final Root root;

    private final Context acContext;

    private final String workspaceName = "default"; // FIXME: use proper workspace as associated with the root

    private final CompiledPermissions compiledPermissions;

    public PermissionProviderImpl(@Nonnull Root root, @Nonnull Set<Principal> principals,
                                  @Nonnull SecurityProvider securityProvider) {
        this.root = root;
        AccessControlConfiguration acConfig = securityProvider.getAccessControlConfiguration();
        this.acContext = acConfig.getContext();
        if (principals.contains(SystemPrincipal.INSTANCE) || isAdmin(principals)) {
            compiledPermissions = AllPermissions.getInstance();
        } else {
            ReadOnlyTree permissionsTree = getPermissionsRoot();
            if (permissionsTree == null || principals.isEmpty()) {
                compiledPermissions = NoPermissions.getInstance();
            } else {
                compiledPermissions = new CompiledPermissionImpl(principals, permissionsTree, getBitsProvider(), acConfig.getRestrictionProvider(NamePathMapper.DEFAULT));
            }
        }
    }

    @Override
    public void refresh() {
        if (compiledPermissions instanceof CompiledPermissionImpl) {
            ((CompiledPermissionImpl) compiledPermissions).refresh(getPermissionsRoot(), getBitsProvider());
        }
    }

    @Nonnull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        return compiledPermissions.getPrivileges(tree);
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, String... privilegeNames) {
        return compiledPermissions.hasPrivileges(tree, privilegeNames);
    }

    @Override
    public boolean canRead(@Nonnull Tree tree) {
        if (isAccessControlContent(tree)) {
            return canReadAccessControlContent(tree, null);
        } else if (isVersionContent(tree)) {
            return canReadVersionContent(tree, null);
        } else {
            return compiledPermissions.canRead(tree);
        }
    }

    @Override
    public boolean canRead(@Nonnull Tree tree, @Nonnull PropertyState property) {
        if (isAccessControlContent(tree)) {
            return canReadAccessControlContent(tree, property);
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
            String path = getVersionablePath(tree, null);
            return path != null && compiledPermissions.isGranted(path, permissions);
        } else {
            return compiledPermissions.isGranted(tree, permissions);
        }
    }

    @Override
    public boolean isGranted(@Nonnull Tree parent, @Nonnull PropertyState property, long permissions) {
        if (isVersionContent(parent)) {
            String path = getVersionablePath(parent, property);
            return path != null && compiledPermissions.isGranted(path, permissions);
        } else {
            return compiledPermissions.isGranted(parent, property, permissions);
        }
    }

    @Override
    public boolean hasPermission(@Nonnull String oakPath, @Nonnull String jcrActions) {
        TreeLocation location = getReadOnlyRoot().getLocation(oakPath);
        long permissions = Permissions.getPermissions(jcrActions, location);
        if (!location.exists()) {
            // TODO: deal with version content
            return compiledPermissions.isGranted(oakPath, permissions);
        }

        PropertyState property = location.getProperty();
        if (property != null) {
            Tree parent = location.getParent().getTree();
            return parent != null && isGranted(parent, property, permissions);
        } else {
            Tree tree = location.getTree();
            return tree != null && isGranted(tree, permissions);
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

    private ReadOnlyRoot getReadOnlyRoot() {
        if (root instanceof ReadOnlyRoot) {
            return (ReadOnlyRoot) root;
        } else {
            return new ReadOnlyRoot(root);
        }
    }

    @CheckForNull
    private ReadOnlyTree getPermissionsRoot() {
        String relativePath = PERMISSIONS_STORE_PATH + '/' + workspaceName;
        ReadOnlyTree rootTree = checkNotNull(getReadOnlyRoot().getTree("/"));
        Tree tree = rootTree.getLocation().getChild(relativePath).getTree();
        return (tree == null) ? null : (ReadOnlyTree) tree;
    }

    @Nonnull
    private PrivilegeBitsProvider getBitsProvider() {
        return new PrivilegeBitsProvider(getReadOnlyRoot());
    }

    private boolean isAccessControlContent(@Nonnull Tree tree) {
        return acContext.definesTree(tree);
    }

    private boolean canReadAccessControlContent(@Nonnull Tree acTree, @Nullable PropertyState acProperty) {
        if (acProperty != null) {
            return compiledPermissions.isGranted(acTree, acProperty, Permissions.READ_ACCESS_CONTROL);
        } else {
            return compiledPermissions.isGranted(acTree, Permissions.READ_ACCESS_CONTROL);
        }
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
        String relPath = "";
        String propName = (property == null) ? "" : property.getName();
        String versionablePath = null;
        Tree t = versionStoreTree;
        while (t != null && !JcrConstants.JCR_VERSIONSTORAGE.equals(t.getName())) {
            String name = t.getName();
            String ntName = checkNotNull(TreeUtil.getPrimaryTypeName(t));
            if (VersionConstants.JCR_FROZENNODE.equals(name) && t != versionStoreTree) {
                relPath = PathUtils.relativize(t.getPath(), versionStoreTree.getPath());
            } else if (JcrConstants.NT_VERSIONHISTORY.equals(ntName)) {
                PropertyState prop = t.getProperty(workspaceName);
                if (prop != null) {
                    versionablePath = PathUtils.concat(prop.getValue(Type.PATH), relPath, propName);
                }
                break;
            }
            t = t.getParent();
        }

        if (versionablePath == null || versionablePath.length() == 0) {
            log.warn("Unable to determine path of the version controlled node.");
        }
        return Strings.emptyToNull(versionablePath);
    }
}
