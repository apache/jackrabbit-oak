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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.TreeLocation;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.core.TreeTypeProvider;
import org.apache.jackrabbit.oak.core.TreeTypeProviderImpl;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * PermissionProviderImpl... TODO
 * <p/>
 * FIXME: define read/write access patterns on version-store content
 * FIXME: proper access permissions on activity-store and configuration-store
 * FIXME: decide on where to filter out hidden items (OAK-753)
 */
public class PermissionProviderImpl implements PermissionProvider, AccessControlConstants, PermissionConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionProviderImpl.class);

    private final Root root;

    private final String workspaceName;

    private final AccessControlConfiguration acConfig;

    private final CompiledPermissions compiledPermissions;

    public PermissionProviderImpl(@Nonnull Root root, @Nonnull Set<Principal> principals,
                                  @Nonnull SecurityProvider securityProvider) {
        this.root = root;
        this.workspaceName = root.getContentSession().getWorkspaceName();
        acConfig = securityProvider.getAccessControlConfiguration();
        if (principals.contains(SystemPrincipal.INSTANCE) || isAdmin(principals)) {
            compiledPermissions = AllPermissions.getInstance();
        } else {
            ImmutableTree permissionsTree = getPermissionsRoot();
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
    public ReadStatus getReadStatus(@Nonnull Tree tree, @Nullable PropertyState property) {
        int type = getType(tree, property);
        switch (type) {
            case TreeTypeProvider.TYPE_HIDDEN:
                // TODO: OAK-753 decide on where to filter out hidden items.
                return ReadStatus.DENY_ALL;
            case TreeTypeProvider.TYPE_AC:
                // TODO: review if read-ac permission is never fine-granular
                return canReadAccessControlContent(tree, null) ? ReadStatus.ALLOW_ALL : ReadStatus.DENY_ALL;
            case TreeTypeProvider.TYPE_VERSION:
                return getVersionContentReadStatus(tree, property);
            default:
                return compiledPermissions.getReadStatus(tree, property);
        }
    }

    @Override
    public boolean isGranted(long repositoryPermissions) {
        return compiledPermissions.isGranted(repositoryPermissions);
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
        int type = getType(tree, property);
        switch (type) {
            case TreeTypeProvider.TYPE_HIDDEN:
                // TODO: OAK-753 decide on where to filter out hidden items.
                return false;
            case TreeTypeProvider.TYPE_VERSION:
                TreeLocation location = getVersionableLocation(tree, property);
                if (location == null) {
                    // TODO: review permission evaluation on hierarchy nodes within the different version stores.
                    return compiledPermissions.isGranted(tree, property, permissions);
                }
                Tree versionableTree = (property == null) ? location.getTree() : location.getParent().getTree();
                if (versionableTree != null) {
                    return compiledPermissions.isGranted(versionableTree, property, permissions);
                } else {
                    return compiledPermissions.isGranted(location.getPath(), permissions);
                }
            default:
                return compiledPermissions.isGranted(tree, property, permissions);
        }
    }

    @Override
    public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
        TreeLocation location = getImmutableRoot().getLocation(oakPath);
        boolean isAcContent = acConfig.getContext().definesLocation(location);
        long permissions = Permissions.getPermissions(jcrActions, location, isAcContent);

        boolean isGranted = false;
        if (!location.exists()) {
            // TODO: deal with version content
            isGranted = compiledPermissions.isGranted(oakPath, permissions);
        } else {
            PropertyState property = location.getProperty();
            Tree tree = (property == null) ? location.getTree() : location.getParent().getTree();
            if (tree != null) {
                isGranted = isGranted(tree, property, permissions);
            }
        }
        return isGranted;
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

    private ImmutableRoot getImmutableRoot() {
        if (root instanceof ImmutableRoot) {
            return (ImmutableRoot) root;
        } else {
            return new ImmutableRoot(root, new TreeTypeProviderImpl(acConfig.getContext()));
        }
    }

    @CheckForNull
    private ImmutableTree getPermissionsRoot() {
        String path = PERMISSIONS_STORE_PATH + '/' + workspaceName;
        Tree tree = getImmutableRoot().getLocation(path).getTree();
        return (tree == null) ? null : (ImmutableTree) tree;
    }

    @Nonnull
    private PrivilegeBitsProvider getBitsProvider() {
        return new PrivilegeBitsProvider(getImmutableRoot());
    }

    private static int getType(@Nonnull Tree tree, @Nullable PropertyState property) {
        // TODO: OAK-753 decide on where to filter out hidden items.
        // TODO: deal with hidden properties
        return ImmutableTree.getType(tree);
    }

    private boolean canReadAccessControlContent(@Nonnull Tree acTree, @Nullable PropertyState acProperty) {
        return compiledPermissions.isGranted(acTree, acProperty, Permissions.READ_ACCESS_CONTROL);
    }

    private ReadStatus getVersionContentReadStatus(@Nonnull Tree versionStoreTree, @Nullable PropertyState property) {
        TreeLocation location = getVersionableLocation(versionStoreTree, property);
        ReadStatus status;
        if (location != null) {
            Tree tree = (property == null) ? location.getTree() : location.getParent().getTree();
            if (tree == null) {
                long permission = (property == null) ? Permissions.READ_NODE : Permissions.READ_PROPERTY;
                if (compiledPermissions.isGranted(location.getPath(), permission)) {
                    status = ReadStatus.ALLOW_THIS;
                } else {
                    status = ReadStatus.DENY_THIS;
                }
            } else {
                status = compiledPermissions.getReadStatus(tree, property);
            }
        } else {
            // TODO: review access on hierarchy nodes within the different version stores.
            status = compiledPermissions.getReadStatus(versionStoreTree, property);
        }
        return status;
    }

    @CheckForNull
    private TreeLocation getVersionableLocation(@Nonnull Tree versionStoreTree, @Nullable PropertyState property) {
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
            log.debug("Unable to determine versionable path of the version store node.");
            return null;
        } else {
            return getImmutableRoot().getLocation(versionablePath);
        }
    }
}
