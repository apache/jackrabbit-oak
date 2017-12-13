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
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

class CugPermissionProvider implements AggregatedPermissionProvider, CugConstants {

    private static final Set<String> READ_PRIVILEGE_NAMES = ImmutableSet.of(
            PrivilegeConstants.JCR_READ,
            PrivilegeConstants.REP_READ_NODES,
            PrivilegeConstants.REP_READ_PROPERTIES
    );

    private final Root root;
    private final String workspaceName;
    private final String[] principalNames;

    private final TreeTypeProvider typeProvider;
    private final Context ctx;

    private final SupportedPaths supportedPaths;

    private Root immutableRoot;
    private ReadOnlyVersionManager versionManager;
    private TopLevelPaths topPaths;

    private final RootProvider rootProvider;
    private final TreeProvider treeProvider;

    CugPermissionProvider(@Nonnull Root root,
                          @Nonnull String workspaceName,
                          @Nonnull Set<Principal> principals,
                          @Nonnull Set<String> supportedPaths,
                          @Nonnull Context ctx,
                          @Nonnull RootProvider rootProvider,
                          @Nonnull TreeProvider treeProvider) {
        this.root = root;
        this.rootProvider = rootProvider;
        this.treeProvider = treeProvider;
        this.workspaceName = workspaceName;

        immutableRoot = rootProvider.createReadOnlyRoot(root);
        principalNames = new String[principals.size()];
        int i = 0;
        for (Principal p : principals) {
            principalNames[i++] = p.getName();
        }

        this.supportedPaths = new SupportedPaths(supportedPaths);
        this.typeProvider = new TreeTypeProvider(ctx);
        this.ctx = ctx;

        topPaths = new TopLevelPaths(immutableRoot);
    }

    @Nonnull
    TreePermission getTreePermission(@Nonnull Tree parent, @Nonnull TreeType parentType, @Nonnull String childName, @Nonnull NodeState childState, @Nonnull AbstractTreePermission parentPermission) {
        Tree t = treeProvider.createReadOnlyTree(parent, childName, childState);
        TreeType type = typeProvider.getType(t, parentType);
        return getTreePermission(t, type, parentPermission);
    }

    boolean isAllow(@Nonnull Tree cugTree) {
        PropertyState princNamesState = cugTree.getProperty(REP_PRINCIPAL_NAMES);
        if (princNamesState != null) {
            for (String pName : princNamesState.getValue(Type.STRINGS)) {
                for (String pN : principalNames) {
                    if (pName.equals(pN)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    //-------------------------------------------------< PermissionProvider >---
    @Override
    public void refresh() {
        immutableRoot = rootProvider.createReadOnlyRoot(root);
        versionManager = null;
        topPaths = new TopLevelPaths(immutableRoot);
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
            throw new IllegalStateException("Attempt to create tree permission for path '"+ tree.getPath() +"', which is either not supported or doesn't contain any CUGs.");
        }
        Tree immutableTree = getImmutableTree(tree);
        TreeType type = typeProvider.getType(immutableTree);
        return getTreePermission(immutableTree, type, parentPermission);
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
        if (ctx.definesLocation(location) || NodeStateUtils.isHiddenPath(oakPath)) {
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
    public long supportedPermissions(@Nonnull TreePermission treePermission, @Nullable PropertyState property, long permissions) {
        long supported = permissions & Permissions.READ;
        if (supported != Permissions.NO_PERMISSION && (treePermission instanceof CugTreePermission) && ((CugTreePermission) treePermission).isInCug()) {
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

    @Nonnull
    public TreePermission getTreePermission(@Nonnull Tree immutableTree, @Nonnull TreeType type, @Nonnull TreePermission parentPermission) {
        if (!isSupportedType(type) || !topPaths.hasAny()) {
            return TreePermission.NO_RECOURSE;
        }

        TreePermission tp;
        boolean parentIsCugPermission = (parentPermission instanceof CugTreePermission);
        if (TreeType.VERSION == type) {
            tp = createVersionPermission(immutableTree, type, parentPermission, parentIsCugPermission);
        } else {
            if (parentIsCugPermission) {
                tp = new CugTreePermission(immutableTree, type, parentPermission, this);
            } else {
                String path = immutableTree.getPath();
                if (includes(path)) {
                    if (topPaths.contains(path)) {
                        tp = new CugTreePermission(immutableTree, type, parentPermission, this);
                    } else {
                        tp = TreePermission.NO_RECOURSE;
                    }
                } else if (mayContain(path) || isJcrSystemPath(immutableTree)) {
                    tp =  new EmptyCugTreePermission(immutableTree, type, this);
                } else {
                    tp = TreePermission.NO_RECOURSE;
                }
            }
        }
        return tp;
    }

    //--------------------------------------------------------------------------

    private static boolean isJcrSystemPath(@Nonnull Tree tree) {
        return JcrConstants.JCR_SYSTEM.equals(tree.getName());
    }

    private static boolean isRead(long permission) {
        return permission == Permissions.READ_NODE || permission == Permissions.READ_PROPERTY || permission == Permissions.READ;
    }

    private static boolean isSupportedType(@Nonnull TreeType type) {
        return type == TreeType.DEFAULT || type == TreeType.VERSION;
    }

    private boolean includesCug(@CheckForNull Tree tree) {
        if (tree != null) {
            Tree immutableTree = getImmutableTree(tree);
            TreeType type = typeProvider.getType(immutableTree);
            if (isSupportedType(type) && topPaths.hasAny()) {
                return getCugRoot(immutableTree, type) != null;
            }
        }
        return false;
    }

    private boolean includes(@Nonnull String path) {
        return supportedPaths.includes(path);
    }

    private boolean mayContain(@Nonnull String path) {
        return supportedPaths.mayContainCug(path) && topPaths.contains(path);
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
        if (!includes(p)) {
            return null;
        }
        if (CugUtil.hasCug(tree)) {
            return tree;
        }
        String parentPath;
        while (!tree.isRoot()) {
            parentPath = PathUtils.getParentPath(p);
            if (!includes(parentPath)) {
                break;
            }
            tree = tree.getParent();
            if (CugUtil.hasCug(tree)) {
                return tree;
            }
        }
        return null;
    }

    private boolean canRead(@Nonnull Tree tree) {
        Tree immutableTree = getImmutableTree(tree);
        TreeType type = typeProvider.getType(immutableTree);
        if (!isSupportedType(type) || !topPaths.hasAny()) {
            return false;
        }
        Tree cugRoot = getCugRoot(immutableTree, type);
        if (cugRoot != null) {
            Tree cugTree = CugUtil.getCug(cugRoot);
            if (cugTree != null) {
                return isAllow(cugTree);
            }
        }
        return false;
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
    private TreePermission createVersionPermission(@Nonnull Tree tree, @Nonnull TreeType type, @Nonnull TreePermission parent, boolean parentIsCugPermission) {
        if (ReadOnlyVersionManager.isVersionStoreTree(tree)) {
            if (parentIsCugPermission) {
                return new CugTreePermission(tree, type, parent, this);
            } else {
                return new EmptyCugTreePermission(tree, type, this);
            }
        } else {
            Tree versionableTree = getVersionManager().getVersionable(tree, workspaceName);
            if (versionableTree == null) {
                return TreePermission.NO_RECOURSE;
            }
            TreeType versionableType = typeProvider.getType(versionableTree);
            if (!isSupportedType(versionableType)) {
                return TreePermission.NO_RECOURSE;
            }

            String path = versionableTree.getPath();
            boolean isSupportedPath = false;

            // test if the versionable node holds a cug
            Tree cug = null;
            if (parentIsCugPermission) {
                cug = CugUtil.getCug(versionableTree);
            } else if (includes(path)) {
                isSupportedPath = true;
                // the versionable tree might be included in a cug defined by
                // a parent node -> need to search for inherited cugs as well.
                Tree cugRoot = getCugRoot(versionableTree, versionableType);
                if (cugRoot != null) {
                    cug = CugUtil.getCug(cugRoot);
                }
            }

            TreePermission tp;
            if (cug != null) {
                // backing versionable tree holds a cug
                tp = new CugTreePermission(tree, type, parent, this, true, isAllow(cug), CugUtil.hasNestedCug(cug));
            } else if (parentIsCugPermission) {
                CugTreePermission ctp = (CugTreePermission) parent;
                tp = new CugTreePermission(tree, type, parent, this, ctp.isInCug(), ctp.isAllow(), ctp.hasNestedCug());
            } else if (isSupportedPath) {
                tp = new CugTreePermission(tree, type, parent, this, false, false, false);
            } else  if (mayContain(path)) {
                tp = new EmptyCugTreePermission(tree, type, this);
            } else {
                tp = TreePermission.NO_RECOURSE;
            }
            return tp;
        }
    }

    @Nonnull
    private ReadOnlyVersionManager getVersionManager() {
        if (versionManager == null) {
            versionManager = ReadOnlyVersionManager.getInstance(immutableRoot, NamePathMapper.DEFAULT);
        }
        return versionManager;
    }
}