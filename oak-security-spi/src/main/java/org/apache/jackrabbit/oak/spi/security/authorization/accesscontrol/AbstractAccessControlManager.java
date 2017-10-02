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
package org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.AccessDeniedException;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@code JackrabbitAccessControlManager} interface.
 * This implementation covers both editing access control content by path and
 * by {@code Principal} resulting both in the same content structure.
 */
public abstract class AbstractAccessControlManager implements JackrabbitAccessControlManager, AccessControlConstants {

    private static final Logger log = LoggerFactory.getLogger(AbstractAccessControlManager.class);

    private final Root root;
    private final String workspaceName;
    private final NamePathMapper namePathMapper;
    private final AuthorizationConfiguration config;
    private final PrivilegeManager privilegeManager;

    private PermissionProvider permissionProvider;

    protected AbstractAccessControlManager(@Nonnull Root root,
                                           @Nonnull NamePathMapper namePathMapper,
                                           @Nonnull SecurityProvider securityProvider) {
        this.root = root;
        this.workspaceName = root.getContentSession().getWorkspaceName();
        this.namePathMapper = namePathMapper;

        privilegeManager = securityProvider.getConfiguration(PrivilegeConfiguration.class).getPrivilegeManager(root, namePathMapper);
        config = securityProvider.getConfiguration(AuthorizationConfiguration.class);
    }

    //-----------------------------------------------< AccessControlManager >---
    @Nonnull
    @Override
    public Privilege[] getSupportedPrivileges(@Nullable String absPath) throws RepositoryException {
        getTree(getOakPath(absPath), Permissions.NO_PERMISSION, false);
        return privilegeManager.getRegisteredPrivileges();
    }

    @Nonnull
    @Override
    public Privilege privilegeFromName(@Nonnull String privilegeName) throws RepositoryException {
        return privilegeManager.getPrivilege(privilegeName);
    }

    @Override
    public boolean hasPrivileges(@Nullable String absPath, @Nullable Privilege[] privileges) throws RepositoryException {
        return hasPrivileges(absPath, privileges, getPermissionProvider(), Permissions.NO_PERMISSION, false);
    }

    @Nonnull
    @Override
    public Privilege[] getPrivileges(@Nullable String absPath) throws RepositoryException {
        return getPrivileges(absPath, getPermissionProvider(), Permissions.NO_PERMISSION);
    }

    //-------------------------------------< JackrabbitAccessControlManager >---
    @Override
    public boolean hasPrivileges(@Nullable String absPath, @Nonnull Set<Principal> principals, @Nullable Privilege[] privileges) throws RepositoryException {
        if (getPrincipals().equals(principals)) {
            return hasPrivileges(absPath, privileges);
        } else {
            PermissionProvider provider = config.getPermissionProvider(root, workspaceName, principals);
            return hasPrivileges(absPath, privileges, provider, Permissions.READ_ACCESS_CONTROL, false);
        }
    }

    @Override
    public Privilege[] getPrivileges(@Nullable String absPath, @Nonnull Set<Principal> principals) throws RepositoryException {
        if (getPrincipals().equals(principals)) {
            return getPrivileges(absPath);
        } else {
            PermissionProvider provider = config.getPermissionProvider(root, workspaceName, principals);
            return getPrivileges(absPath, provider, Permissions.READ_ACCESS_CONTROL);
        }
    }

    //----------------------------------------------------------< protected >---
    @Nonnull
    protected AuthorizationConfiguration getConfig() {
        return config;
    }

    @Nonnull
    protected Root getRoot() {
        return root;
    }

    @Nonnull
    protected Root getLatestRoot() {
        return root.getContentSession().getLatestRoot();
    }

    @Nonnull
    protected NamePathMapper getNamePathMapper() {
        return namePathMapper;
    }

    @Nonnull
    protected PrivilegeManager getPrivilegeManager() {
        return privilegeManager;
    }

    @CheckForNull
    protected String getOakPath(@Nullable String jcrPath) throws RepositoryException {
        if (jcrPath == null) {
            return null;
        } else {
            String oakPath = namePathMapper.getOakPath(jcrPath);
            if (oakPath == null || !PathUtils.isAbsolute(oakPath)) {
                throw new RepositoryException("Failed to resolve JCR path " + jcrPath);
            }
            return oakPath;
        }
    }

    @Nonnull
    protected Tree getTree(@Nullable String oakPath, long permissions, boolean checkAcContent) throws RepositoryException {
        Tree tree = (oakPath == null) ? root.getTree("/") : root.getTree(oakPath);
        if (!tree.exists()) {
            throw new PathNotFoundException("No tree at " + oakPath);
        }
        if (permissions != Permissions.NO_PERMISSION) {
            // check permissions
            checkPermissions((oakPath == null) ? null : tree, permissions);
        }
        // check if the tree defines access controlled content
        if (checkAcContent && config.getContext().definesTree(tree)) {
            throw new AccessControlException("Tree " + tree.getPath() + " defines access control content.");
        }
        return tree;
    }

    @Nonnull
    protected PermissionProvider getPermissionProvider() {
        if (permissionProvider == null) {
            permissionProvider = config.getPermissionProvider(root, workspaceName, getPrincipals());
        } else {
            permissionProvider.refresh();
        }
        return permissionProvider;
    }

    //------------------------------------------------------------< private >---
    @Nonnull
    private Set<Principal> getPrincipals() {
        return root.getContentSession().getAuthInfo().getPrincipals();
    }

    private void checkPermissions(@Nullable Tree tree, long permissions) throws AccessDeniedException {
        boolean isGranted;
        if (tree == null) {
            isGranted = getPermissionProvider().getRepositoryPermission().isGranted(permissions);
        } else {
            isGranted = getPermissionProvider().isGranted(tree, null, permissions);
        }
        if (!isGranted) {
            throw new AccessDeniedException("Access denied.");
        }
    }

    @Nonnull
    private Privilege[] getPrivileges(@Nullable String absPath,
                                      @Nonnull PermissionProvider provider,
                                      long permissions) throws RepositoryException {
        Tree tree;
        if (absPath == null) {
            tree = null;
            if (permissions != Permissions.NO_PERMISSION) {
                checkPermissions(null, permissions);
            }
        } else {
            tree = getTree(getOakPath(absPath), permissions, false);
        }
        Set<String> pNames = provider.getPrivileges(tree);
        if (pNames.isEmpty()) {
            return new Privilege[0];
        } else {
            Set<Privilege> privileges = new HashSet<Privilege>(pNames.size());
            for (String name : pNames) {
                privileges.add(privilegeManager.getPrivilege(namePathMapper.getJcrName(name)));
            }
            return privileges.toArray(new Privilege[privileges.size()]);
        }
    }

    private boolean hasPrivileges(@Nullable String absPath, @Nullable Privilege[] privileges,
                                  @Nonnull PermissionProvider provider, long permissions,
                                  boolean checkAcContent) throws RepositoryException {
        Tree tree;
        if (absPath == null) {
            tree = null;
            if (permissions != Permissions.NO_PERMISSION) {
                checkPermissions(null, permissions);
            }
        } else {
            tree = getTree(getOakPath(absPath), permissions, checkAcContent);
        }
        if (privileges == null || privileges.length == 0) {
            // null or empty privilege array -> return true
            log.debug("No privileges passed -> allowed.");
            return true;
        } else {
            Set<String> privilegeNames = new HashSet<String>(privileges.length);
            for (Privilege privilege : privileges) {
                privilegeNames.add(namePathMapper.getOakName(privilege.getName()));
            }
            return provider.hasPrivileges(tree, privilegeNames.toArray(new String[privilegeNames.size()]));
        }
    }
}
