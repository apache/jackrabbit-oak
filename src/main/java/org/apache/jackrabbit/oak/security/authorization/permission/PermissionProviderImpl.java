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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.IdentifierManager;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.core.TreeTypeProvider;
import org.apache.jackrabbit.oak.core.TreeTypeProviderImpl;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.util.TreeLocation;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * PermissionProviderImpl... TODO
 * <p/>
 * FIXME: decide on where to filter out hidden items (OAK-753)
 */
public class PermissionProviderImpl implements PermissionProvider, AccessControlConstants, PermissionConstants {

    private static final Logger log = LoggerFactory.getLogger(PermissionProviderImpl.class);

    private static final Set<String> V_ROOT_NAMES = ImmutableSet.of(
            JcrConstants.JCR_VERSIONSTORAGE,
            VersionConstants.JCR_CONFIGURATIONS,
            VersionConstants.JCR_ACTIVITIES);

    private final Root root;

    private final String workspaceName;

    private final AuthorizationConfiguration acConfig;

    private final CompiledPermissions compiledPermissions;

    private ImmutableRoot immutableRoot;

    public PermissionProviderImpl(@Nonnull Root root, @Nonnull Set<Principal> principals,
                                  @Nonnull SecurityProvider securityProvider) {
        this.root = root;
        this.workspaceName = root.getContentSession().getWorkspaceName();

        acConfig = securityProvider.getConfiguration(AuthorizationConfiguration.class);
        immutableRoot = getImmutableRoot(root, acConfig);

        if (principals.contains(SystemPrincipal.INSTANCE) || isAdmin(principals)) {
            compiledPermissions = AllPermissions.getInstance();
        } else {
            ImmutableTree permissionsTree = getPermissionsRoot();
            if (!permissionsTree.exists() || principals.isEmpty()) {
                compiledPermissions = NoPermissions.getInstance();
            } else {
                String[] readPaths = acConfig.getParameters().getConfigValue(PARAM_READ_PATHS, DEFAULT_READ_PATHS);
                compiledPermissions = new CompiledPermissionImpl(principals,
                        permissionsTree, getBitsProvider(),
                        acConfig.getRestrictionProvider(),
                        ImmutableSet.copyOf(readPaths));
            }
        }
    }

    @Override
    public void refresh() {
        immutableRoot = getImmutableRoot(root, acConfig);
        compiledPermissions.refresh(getPermissionsRoot(), getBitsProvider());
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
                return ReadStatus.ALLOW_ALL;
            case TreeTypeProvider.TYPE_AC:
                // TODO: review if read-ac permission is never fine-granular
                // TODO: replace by calling #getReadStatus
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
                return true;
            case TreeTypeProvider.TYPE_VERSION:
                TreeLocation location = getLocation(tree, property);
                if (location == null) {
                    // unable to determine the location of the versionable item -> deny access.
                    return false;
                }
                Tree versionableTree = (property == null) ? location.getTree() : location.getParent().getTree();
                if (versionableTree != null) {
                    return compiledPermissions.isGranted(versionableTree, property, permissions);
                } else {
                    // versionable node does not exist (anymore) in this workspace;
                    // use best effort calculation based on the item path.
                    return compiledPermissions.isGranted(location.getPath(), permissions);
                }
            default:
                return compiledPermissions.isGranted(tree, property, permissions);
        }
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

    //--------------------------------------------------------------------------

    private boolean isAdmin(Set<Principal> principals) {
        Set<String> adminNames = ImmutableSet.copyOf(acConfig.getParameters().getConfigValue(PARAM_ADMINISTRATIVE_PRINCIPALS, new String[0]));
        for (Principal principal : principals) {
            if (principal instanceof AdminPrincipal || adminNames.contains(principal.getName())) {
                return true;
            }
        }
        return false;
    }

    private static ImmutableRoot getImmutableRoot(Root base, SecurityConfiguration acConfig) {
        if (base instanceof ImmutableRoot) {
            return (ImmutableRoot) base;
        } else {
            return new ImmutableRoot(base, new TreeTypeProviderImpl(acConfig.getContext()));
        }
    }

    @Nonnull
    private ImmutableTree getPermissionsRoot() {
        return immutableRoot.getTree(PERMISSIONS_STORE_PATH + '/' + workspaceName);
    }

    @Nonnull
    private PrivilegeBitsProvider getBitsProvider() {
        return new PrivilegeBitsProvider(immutableRoot);
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
        TreeLocation location = getLocation(versionStoreTree, property);
        ReadStatus status = ReadStatus.DENY_THIS;
        if (location != null) {
            Tree tree = (property == null) ? location.getTree() : location.getParent().getTree();
            if (tree != null) {
                status = compiledPermissions.getReadStatus(tree, property);
            } else {
                // versionable node does not exist (anymore) in this workspace;
                // use best effort calculation based on the item path.
                long permission = (property == null) ? Permissions.READ_NODE : Permissions.READ_PROPERTY;
                if (compiledPermissions.isGranted(location.getPath(), permission)) {
                    status = ReadStatus.ALLOW_THIS;
                }
            }
        }
        return status;
    }

    @CheckForNull
    private TreeLocation getLocation(@Nonnull Tree versionStoreTree, @Nullable PropertyState property) {
        String relPath = "";
        String propName = (property == null) ? "" : property.getName();
        String versionablePath = null;
        Tree t = versionStoreTree;
        while (t.exists() && !t.isRoot() && !V_ROOT_NAMES.contains(t.getName())) {
            String ntName = checkNotNull(TreeUtil.getPrimaryTypeName(t));
            if (VersionConstants.JCR_FROZENNODE.equals(t.getName()) && t != versionStoreTree) {
                relPath = PathUtils.relativize(t.getPath(), versionStoreTree.getPath());
            } else if (JcrConstants.NT_VERSIONHISTORY.equals(ntName)) {
                PropertyState prop = t.getProperty(workspaceName);
                if (prop != null) {
                    versionablePath = PathUtils.concat(prop.getValue(Type.PATH), relPath, propName);
                }
                return createLocation(versionablePath);
            } else if (VersionConstants.NT_CONFIGURATION.equals(ntName)) {
                String rootId = TreeUtil.getString(t, VersionConstants.JCR_ROOT);
                versionablePath = new IdentifierManager(root).getPath(rootId);
                return createLocation(versionablePath);
            } else if (VersionConstants.NT_ACTIVITY.equals(ntName)) {
                return createLocation(versionStoreTree, property);
            }
            t = t.getParent();
        }

        // intermediate node in the version, configuration or activity store that
        // matches none of the special conditions checked above -> regular permission eval.
        return createLocation(versionStoreTree, property);
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

    @CheckForNull
    private TreeLocation createLocation(@Nullable String oakPath) {
        if (oakPath != null && PathUtils.isAbsolute(oakPath)) {
            return TreeLocation.create(immutableRoot, oakPath);
        } else {
            log.debug("Unable to create location for path " + oakPath);
            return null;
        }
    }

    @Nonnull
    private static TreeLocation createLocation(@Nonnull Tree tree, @Nullable PropertyState property) {
        if (property == null) {
            return TreeLocation.create(tree);
        } else {
            return TreeLocation.create(tree, property);
        }
    }
}
