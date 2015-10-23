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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;

class CugPermissionProvider implements AggregatedPermissionProvider, CugConstants {

    private static final Set<String> READ_PRIVILEGE_NAMES = ImmutableSet.of(
            PrivilegeConstants.JCR_READ,
            PrivilegeConstants.REP_READ_NODES,
            PrivilegeConstants.REP_READ_PROPERTIES
    );

    private final Root root;
    private final String workspaceName;
    private final Set<String> principalNames;

    private final TreeTypeProvider typeProvider;
    private final Context ctx;

    private final SupportedPaths supportedPaths;

    private Root immutableRoot;
    private ReadOnlyVersionManager versionManager;

    CugPermissionProvider(@Nonnull Root root,
                          @Nonnull String workspaceName,
                          @Nonnull Set<Principal> principals,
                          @Nonnull Set<String> supportedPaths,
                          @Nonnull Context ctx) {
        this.root = root;
        this.workspaceName = workspaceName;

        immutableRoot = RootFactory.createReadOnlyRoot(root);
        principalNames = new HashSet<String>(principals.size());
        for (Principal p : Iterables.filter(principals, Predicates.notNull())) {
            principalNames.add(p.getName());
        }

        this.supportedPaths = new SupportedPaths(supportedPaths);
        this.typeProvider = new TreeTypeProvider(ctx);
        this.ctx = ctx;
    }

    //-------------------------------------------------< PermissionProvider >---
    @Override
    public void refresh() {
        immutableRoot = RootFactory.createReadOnlyRoot(root);
        versionManager = null;
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
        if (TreePermission.NO_RECOURSE == parentPermission) {
            throw new IllegalStateException("Attempt to create tree permission for unsupported path.");
        }
        Tree immutableTree = getImmutableTree(tree);
        TreeType type = typeProvider.getType(immutableTree);
        if (!isSupportedType(type)) {
            return TreePermission.NO_RECOURSE;
        }

        TreePermission tp;
        boolean parentIsCugPermission = (parentPermission instanceof CugTreePermission);
        if (TreeType.VERSION == type) {
            if (ReadOnlyVersionManager.isVersionStoreTree(immutableTree)) {
                tp = (parentIsCugPermission) ?
                        createCugPermission(immutableTree, (CugTreePermission) parentPermission) :
                        new EmptyCugTreePermission(immutableTree, this);
            } else {
                // TODO
                Tree versionableTree = getVersionManager().getVersionable(immutableTree, workspaceName);
                if (versionableTree == null) {
                    tp = TreePermission.NO_RECOURSE;
                } else if (!parentIsCugPermission &&
                        !supportedPaths.includes(versionableTree.getPath()) &&
                        !supportedPaths.mayContainCug(versionableTree.getPath())){
                    tp = TreePermission.NO_RECOURSE;
                } else {
                    Tree cugRoot = getCugRoot(versionableTree, typeProvider.getType(versionableTree));
                    if (cugRoot == null) {
                        // there might be a cug in the live correspondent of any of the frozen subtrees
                        tp = new EmptyCugTreePermission(immutableTree, this);
                    } else {
                        boolean canRead = createCugPermission(cugRoot, null).canRead();
                        tp = new CugTreePermission(immutableTree, canRead, this);
                    }
                }
            }
        } else {
            if (parentIsCugPermission) {
                tp = createCugPermission(immutableTree, (CugTreePermission) parentPermission);
            } else {
                String path = immutableTree.getPath();
                if (supportedPaths.includes(path)) {
                    tp =  createCugPermission(immutableTree, null);
                } else if (supportedPaths.mayContainCug(path) || isJcrSystemPath(path)) {
                    tp =  new EmptyCugTreePermission(immutableTree, this);
                } else {
                    tp = TreePermission.NO_RECOURSE;
                }
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
        if (ctx.definesLocation(location) || NodeStateUtils.isHidden(PathUtils.getName(oakPath))) {
            return false;
        }

        long permissions = Permissions.getPermissions(jcrActions, location, false);
        return isGranted(location, permissions);
    }

    //---------------------------------------< AggregatedPermissionProvider >---
    @Nonnull
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

        if (pb.isEmpty() || !includesCug(tree)) {
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
        if (supported != Permissions.NO_PERMISSION && includesCug(tree)) {
            return supported;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public long supportedPermissions(@Nonnull TreeLocation location, long permissions) {
        long supported = permissions & Permissions.READ;
        if (supported != Permissions.NO_PERMISSION && includesCug(getTreeFromLocation(location))) {
            return supported;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public long supportedPermissions(@Nonnull TreePermission treePermission, @Nullable PropertyState propertyState, long permissions) {
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
            Tree tree = getTreeFromLocation(location);
            if (tree != null) {
                return isGranted(tree, location.getProperty(), permissions);
            }
        }
        return false;
    }

    //--------------------------------------------------------------------------
    private static boolean isJcrSystemPath(@Nonnull String path) {
        return JcrConstants.JCR_SYSTEM.equals(Text.getName(path));
    }

    private static boolean isRead(long permission) {
        return permission == Permissions.READ_NODE || permission == Permissions.READ_PROPERTY || permission == Permissions.READ;
    }

    private static boolean hasCug(@Nonnull Tree tree) {
        return tree.exists() && tree.hasChild(REP_CUG_POLICY);
    }

    private static boolean isSupportedType(@Nonnull TreeType type) {
        return type == TreeType.DEFAULT || type == TreeType.VERSION;
    }

    private boolean includesCug(@CheckForNull Tree tree) {
        if (tree != null) {
            Tree immutableTree = getImmutableTree(tree);
            TreeType type = typeProvider.getType(immutableTree);
            if (isSupportedType(type)) {
                return getCugRoot(immutableTree, type) != null;
            }
        }
        return false;
    }

    /**
     * Returns the {@code tree} that holds a CUG policy in the ancestry of the
     * given {@code tree} with the specified {@code path} or {@code null} if no
     * such tree exists and thus no CUG is effective at the specified path.
     *
     * @param immutableTree The target tree.
     * @param type the type of this tree.
     * @return the {@code tree} holding the CUG policy that effects the specified
     * path or {@code null} if no such policy exists.
     */
    @CheckForNull
    private Tree getCugRoot(@Nonnull Tree immutableTree, @Nonnull TreeType type) {
        Tree tree = immutableTree;
        String p = immutableTree.getPath();
        if (TreeType.VERSION == type && !ReadOnlyVersionManager.isVersionStoreTree(tree)) {
            tree = getVersionManager().getVersionable(immutableTree, workspaceName);
            if (tree == null) {
                return null;
            }
            p = tree.getPath();
        }
        if (!supportedPaths.includes(p)) {
            return null;
        }
        if (hasCug(tree)) {
            return tree;
        }
        String parentPath;
        while (!tree.isRoot()) {
            parentPath = PathUtils.getParentPath(p);
            if (!supportedPaths.includes(parentPath)) {
                break;
            }
            tree = tree.getParent();
            if (hasCug(tree)) {
                return tree;
            }
        }
        return null;
    }

    private boolean canRead(@Nonnull Tree tree) {
        Tree immutableTree = getImmutableTree(tree);
        TreeType type = typeProvider.getType(immutableTree);
        if (!isSupportedType(type)) {
            return false;
        }
        Tree cugRoot = getCugRoot(immutableTree, type);
        return cugRoot != null && createCugPermission(cugRoot, null).canRead();
    }

    @Nonnull
    private Tree getImmutableTree(@Nonnull Tree tree) {
        return TreeUtil.isReadOnlyTree(tree) ? tree : immutableRoot.getTree(tree.getPath());
    }

    @CheckForNull
    private static Tree getTreeFromLocation(@Nonnull TreeLocation location) {
        Tree tree = (location.getProperty() == null) ? location.getTree() : location.getParent().getTree();
        while (tree == null && !PathUtils.denotesRoot(location.getPath())) {
            location = location.getParent();
            tree = location.getTree();
        }
        return tree;
    }

    @Nonnull
    private TreePermission createCugPermission(@Nonnull Tree tree, @Nullable CugTreePermission parent) {
        TreePermission tp;

        Tree cugTree = tree.getChild(REP_CUG_POLICY);
        if (CugUtil.definesCug(cugTree)) {
            // a new (possibly nested) cug starts off here
            PropertyState princNamesState = cugTree.getProperty(REP_PRINCIPAL_NAMES);
            boolean allow = princNamesState != null && Iterables.any(princNamesState.getValue(Type.STRINGS), new Predicate<String>() {
                @Override
                public boolean apply(@Nullable String principalName) {
                    return (principalName != null) && principalNames.contains(principalName);
                }
            });
            tp = new CugTreePermission(tree, allow, this);
        } else if (parent != null) {
            // still within the parents CUG
            tp = new CugTreePermission(tree, parent);
        } else {
            tp = new EmptyCugTreePermission(tree, this);
        }
        return tp;
    }

    @Nonnull
    private ReadOnlyVersionManager getVersionManager() {
        if (versionManager == null) {
            versionManager = ReadOnlyVersionManager.getInstance(immutableRoot, NamePathMapper.DEFAULT);
        }
        return versionManager;
    }
}