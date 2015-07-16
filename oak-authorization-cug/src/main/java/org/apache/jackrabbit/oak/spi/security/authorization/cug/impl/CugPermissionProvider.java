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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CugPermissionProvider implements AggregatedPermissionProvider, CugConstants {

    private static final Logger log = LoggerFactory.getLogger(CugPermissionProvider.class);

    private static final Set<String> READ_PRIVILEGE_NAMES = ImmutableSet.of(
            PrivilegeConstants.JCR_READ,
            PrivilegeConstants.REP_READ_NODES,
            PrivilegeConstants.REP_READ_PROPERTIES
    );

    private final Root root;
    private final Set<String> principalNames;

    private final Context ctx;

    private final SupportedPaths supportedPaths;

    private Root immutableRoot;

    CugPermissionProvider(@Nonnull Root root,
                          @Nonnull Set<Principal> principals,
                          @Nonnull Set<String> supportedPaths,
                          @Nonnull Context ctx) {
        this.root = root;

        immutableRoot = RootFactory.createReadOnlyRoot(root);
        principalNames = new HashSet<String>(principals.size());
        for (Principal p : Iterables.filter(principals, Predicates.notNull())) {
            principalNames.add(p.getName());
        }

        this.supportedPaths = new SupportedPaths(supportedPaths);
        this.ctx = ctx;
    }

    //-------------------------------------------------< PermissionProvider >---
    @Override
    public void refresh() {
        immutableRoot = RootFactory.createReadOnlyRoot(root);
    }

    @Nonnull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        if (tree != null && canRead(tree)) {
            return READ_PRIVILEGE_NAMES;
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
        if (tree == null) {
            return false;
        }
        for (String privilegeName : privilegeNames) {
            if (!READ_PRIVILEGE_NAMES.contains(privilegeName)) {
                return false;
            }
        }
        return canRead(tree);
    }

    @Nonnull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return RepositoryPermission.EMPTY;
    }

    @Nonnull
    @Override
    public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
        Tree immutableTree = getImmutableTree(tree);
        if (parentPermission == TreePermission.EMPTY && !immutableTree.isRoot() || isAcContent(immutableTree, true)) {
            return TreePermission.EMPTY;
        }

        TreePermission tp;
        if (parentPermission instanceof CugTreePermission) {
            tp =  new CugTreePermission(immutableTree, ((CugTreePermission) parentPermission));
            if (hasCug(immutableTree)) {
                // a new (nested) cug starts off here
                tp = createCugPermission(immutableTree, tp);
            }
        } else {
            String path = immutableTree.getPath();
            if (supportedPaths.includes(path)) {
                tp =  createCugPermission(immutableTree, null);
            } else if (supportedPaths.mayContainCug(path)) {
                tp =  new EmptyCugTreePermission(immutableTree, this);
            } else {
                tp = TreePermission.EMPTY;
            }
        }
        return tp;
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, PropertyState property, long permissions) {
        if (isRead(permissions)) {
            return canRead(tree);
        } else {
            return false;
        }
    }

    @Override
    public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
        TreeLocation location = TreeLocation.create(immutableRoot, oakPath);
        boolean isAcContent = isAcContent(location);
        long permissions = Permissions.getPermissions(jcrActions, location, isAcContent);

        return isGranted(location, permissions);
    }

    //---------------------------------------< AggregatedPermissionProvider >---
    @Override
    public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
        if (tree == null) {
            return PrivilegeBits.EMPTY;
        }

        PrivilegeBits pb;
        if (privilegeBits == null) {
            pb = PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ);
        } else {
            pb = PrivilegeBits.getInstance(privilegeBits);
            pb.retain(PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ));
        }

        if (pb.isEmpty() || !includesCug(tree, tree.getPath())) {
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

        long supported = permissions & Permissions.READ;
        if (supported != Permissions.NO_PERMISSION && includesCug(tree, tree.getPath())) {
            return supported;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public long supportedPermissions(@Nonnull TreeLocation location, long permissions) {
        long supported = permissions & Permissions.READ;
        if (supported != Permissions.NO_PERMISSION && includesCug(getTreeFromLocation(location, location.getProperty()), location.getPath())) {
            return supported;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public long supportedPermissions(@Nonnull TreePermission treePermission, long permissions) {
        long supported = permissions & Permissions.READ;
        if (supported != Permissions.NO_PERMISSION && (treePermission instanceof CugTreePermission)) {
            return supported;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public boolean isGranted(@Nonnull TreeLocation location, long permissions) {
        if (isRead(permissions)) {
            PropertyState property = location.getProperty();
            Tree tree = getTreeFromLocation(location, property);
            if (tree != null) {
                return isGranted(tree, property, permissions);
            }
        }
        return false;
    }

    //--------------------------------------------------------------------------
    private static boolean isRead(long permission) {
        return permission == Permissions.READ_NODE || permission == Permissions.READ_PROPERTY || permission == Permissions.READ;
    }

    private static boolean hasCug(@Nonnull Tree tree) {
        return tree.exists() && tree.hasChild(REP_CUG_POLICY);
    }

    private boolean isAcContent(@Nonnull Tree tree, boolean testForCtxRoot) {
        return (testForCtxRoot) ? ctx.definesContextRoot(tree) : ctx.definesTree(tree);
    }

    private boolean isAcContent(@Nonnull TreeLocation location) {
        return ctx.definesLocation(location);
    }

    private boolean includesCug(@CheckForNull Tree tree, @Nonnull String path) {
        return tree != null && getCugRoot(tree, path) != null;
    }

    /**
     * Returns the {@code tree} that holds a CUG policy in the ancestry of the
     * given {@code tree} with the specified {@code path} or {@code null} if no
     * such tree exists and thus no CUG is effective at the specified path.
     *
     * @param tree The target tree.
     * @param path The path of the given target tree.
     * @return the {@code tree} holding the CUG policy that effects the specified
     * path or {@code null} if no such policy exists.
     */
    @CheckForNull
    private Tree getCugRoot(@Nonnull Tree tree, @Nonnull String path) {
        if (!supportedPaths.includes(path)) {
            return null;
        }
        Tree immutableTree = getImmutableTree(tree);
        if (hasCug(immutableTree)) {
            return immutableTree;
        }
        String parentPath;
        while (!immutableTree.isRoot()) {
            parentPath = PathUtils.getParentPath(path);
            if (!supportedPaths.includes(parentPath)) {
                break;
            }
            immutableTree = immutableTree.getParent();
            if (hasCug(immutableTree)) {
                return immutableTree;
            }
        }
        return null;
    }

    private boolean canRead(@Nonnull Tree tree) {
        Tree immutableTree = getImmutableTree(tree);
        if (isAcContent(immutableTree, false)) {
            // cug defining access control content is not accessible
            return false;
        }
        Tree cugRoot = getCugRoot(immutableTree, immutableTree.getPath());
        return cugRoot != null && createCugPermission(cugRoot, null).canRead();
    }

    @Nonnull
    private Tree getImmutableTree(@Nonnull Tree tree) {
        return TreeUtil.isReadOnlyTree(tree) ? tree : immutableRoot.getTree(tree.getPath());
    }

    @CheckForNull
    private static Tree getTreeFromLocation(@Nonnull TreeLocation location, @CheckForNull PropertyState property) {
        Tree tree = (property == null) ? location.getTree() : location.getParent().getTree();
        while (tree == null && !PathUtils.denotesRoot(location.getPath())) {
            location = location.getParent();
            tree = location.getTree();
        }
        return tree;
    }

    @Nonnull
    private TreePermission createCugPermission(@Nonnull Tree tree, @Nullable TreePermission fallback) {
        Tree cugTree = tree.getChild(REP_CUG_POLICY);
        if (CugUtil.definesCug(cugTree)) {
            PropertyState princNamesState = cugTree.getProperty(REP_PRINCIPAL_NAMES);
            if (princNamesState != null) {
                boolean allow = false;
                for (String pName : princNamesState.getValue(Type.STRINGS)) {
                    if (principalNames.contains(pName)) {
                        allow = true;
                        break;
                    }
                }
                return new CugTreePermission(tree, allow, this);
            } else {
                log.warn("Tree at {0} doesn't represent a valid CUG.", cugTree.getPath());
            }
        }
        return (fallback == null) ? new EmptyCugTreePermission(tree, this) : fallback;
    }
}