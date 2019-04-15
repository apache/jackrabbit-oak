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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.ReadOnly;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission.ALL;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission.EMPTY;

class PrincipalBasedPermissionProvider implements AggregatedPermissionProvider, Constants {

    private static final Logger log = LoggerFactory.getLogger(PrincipalBasedPermissionProvider.class);

    private final Root root;
    private final String workspaceName;
    private final Iterable principalPaths;
    private final MgrProvider mgrProvider;
    private final TreeTypeProvider typeProvider;

    private final PrivilegeBits modAcBits;

    private Root immutableRoot;
    private RepositoryPermissionImpl repositoryPermission;
    private EntryCache entryCache;

    PrincipalBasedPermissionProvider(@NotNull Root root,
                                     @NotNull String workspaceName,
                                     @NotNull Iterable<String> principalPaths,
                                     @NotNull PrincipalBasedAuthorizationConfiguration authorizationConfiguration) {
        this.root = root;
        this.workspaceName = workspaceName;
        this.principalPaths = principalPaths;

        immutableRoot = authorizationConfiguration.getRootProvider().createReadOnlyRoot(root);
        mgrProvider = new MgrProviderImpl(authorizationConfiguration, immutableRoot, NamePathMapper.DEFAULT);
        typeProvider = new TreeTypeProvider(mgrProvider.getContext());
        modAcBits = mgrProvider.getPrivilegeBitsProvider().getBits(PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL);

        entryCache = new EntryCache(immutableRoot, principalPaths, mgrProvider.getRestrictionProvider());
    }

    //-------------------------------------------------< PermissionProvider >---

    @Override
    public void refresh() {
        immutableRoot = mgrProvider.getRootProvider().createReadOnlyRoot(root);
        mgrProvider.reset(immutableRoot, NamePathMapper.DEFAULT);

        entryCache = new EntryCache(immutableRoot, principalPaths, mgrProvider.getRestrictionProvider());
        if (repositoryPermission != null) {
            repositoryPermission.refresh();
        }
    }

    @NotNull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        return mgrProvider.getPrivilegeBitsProvider().getPrivilegeNames(getGrantedPrivilegeBits(tree));
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @NotNull String... privilegeNames) {
        return getGrantedPrivilegeBits(tree).includes(mgrProvider.getPrivilegeBitsProvider().getBits(privilegeNames));
    }

    @Override
    public @NotNull RepositoryPermission getRepositoryPermission() {
        if (repositoryPermission == null) {
            repositoryPermission = new RepositoryPermissionImpl();
        }
        return repositoryPermission;
    }

    @Override
    public @NotNull TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreePermission parentPermission) {
        Tree readOnly = getReadOnlyTree(tree);
        TreeType parentType;
        if (parentPermission instanceof AbstractTreePermission) {
            parentType = ((AbstractTreePermission) parentPermission).getType();
        } else {
            parentType = (tree.isRoot()) ? TreeType.DEFAULT : typeProvider.getType(tree.getParent());
        }
        return getTreePermission(readOnly, typeProvider.getType(readOnly, parentType), parentPermission);
    }

    @Override
    public boolean isGranted(@NotNull Tree tree, @Nullable PropertyState property, long permissions) {
        Tree readOnly = getReadOnlyTree(tree);
        TreeType type = typeProvider.getType(readOnly);
        switch (type) {
            case HIDDEN:
                return true;
            case INTERNAL:
                return false;
            case VERSION:
                if (!isVersionStoreTree(readOnly)) {
                    Tree versionableTree = getVersionableTree(readOnly);
                    if (versionableTree == null) {
                        // unable to determine the location of the versionable item -> deny access.
                        return false;
                    }
                    // reset the tree that is target for permission evaluation (see below)
                    readOnly = versionableTree;
                } // else: versionstore-tree is covered below
                break;
            case ACCESS_CONTROL:
                if (!isGrantedOnEffective(readOnly, permissions)) {
                    return false;
                }
                break;
            default:
                // covered below
                break;
        }

        return isGranted(readOnly.getPath(), EntryPredicate.create(readOnly, property), EntryPredicate.createParent(readOnly, permissions), permissions);
    }

    @Override
    public boolean isGranted(@NotNull String oakPath, @NotNull String jcrActions) {
        TreeLocation location = TreeLocation.create(immutableRoot, oakPath);

        boolean isAcContent = mgrProvider.getContext().definesLocation(location);
        long permissions = Permissions.getPermissions(jcrActions, location, isAcContent);

        return isGranted(location, permissions);
    }

    //---------------------------------------< AggregatedPermissionProvider >---

    @Override
    public @NotNull PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
        return (privilegeBits != null) ? privilegeBits : new PrivilegeBitsProvider(immutableRoot).getBits(PrivilegeConstants.JCR_ALL);

    }

    @Override
    public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
        return permissions;
    }

    @Override
    public long supportedPermissions(@NotNull TreeLocation location, long permissions) {
        return permissions;
    }

    @Override
    public long supportedPermissions(@NotNull TreePermission treePermission, @Nullable PropertyState property, long permissions) {
        return permissions;
    }

    @Override
    public boolean isGranted(@NotNull TreeLocation location, long permissions) {
        boolean isGranted = false;
        PropertyState property = location.getProperty();
        TreeLocation tl = (property == null) ? location : location.getParent();
        Tree tree = tl.getTree();
        String oakPath = location.getPath();
        if (tree != null) {
            isGranted = isGranted(tree, property, permissions);
        } else if (!oakPath.startsWith(VersionConstants.VERSION_STORE_PATH)) {
            Predicate<PermissionEntry> parentPredicate = EntryPredicate.createParent(tl.getPath(), tl.getParent().getTree(), permissions);
            isGranted = isGranted(oakPath, EntryPredicate.create(oakPath), parentPredicate, permissions);
        } else {
            log.debug("Cannot determine permissions for non-existing location {} below the version storage", location);
        }
        return isGranted;
    }

    @Override
    public @NotNull TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreeType type, @NotNull TreePermission parentPermission) {
        Tree readOnly = getReadOnlyTree(tree);
        if (readOnly.isRoot()) {
            return new RegularTreePermission(readOnly, TreeType.DEFAULT);
        }
        switch (type) {
            case HIDDEN:
                return ALL;
            case INTERNAL:
                return EMPTY;
            case VERSION:
                if (!isVersionStoreTree(readOnly)) {
                    if (parentPermission instanceof VersionTreePermission) {
                        return parentPermission.getChildPermission(readOnly.getName(), mgrProvider.getTreeProvider().asNodeState(readOnly));
                    } else {
                        Tree versionableTree = getVersionableTree(readOnly);
                        if (versionableTree == null) {
                            log.warn("Cannot retrieve versionable node for {}", readOnly.getPath());
                            return EMPTY;
                        } else {
                            return new VersionTreePermission(readOnly, versionableTree);
                        }
                    }
                } else {
                    // versionstorage -> regular tree permissions
                    return new RegularTreePermission(readOnly, type);
                }
            case ACCESS_CONTROL:
            default:
                return new RegularTreePermission(readOnly, type);
        }
    }

    //--------------------------------------------------------------------------

    private Iterator<PermissionEntry> getEntryIterator(@NotNull String path, @NotNull Predicate<PermissionEntry> predicate) {
        return new EntryIterator(path, predicate, entryCache);
    }

    private boolean isGranted(@NotNull String path, @NotNull Predicate<PermissionEntry> predicate,
                              @NotNull Predicate<PermissionEntry> parentPredicate, long permissions) {
        long allows = Permissions.NO_PERMISSION;
        PrivilegeBits bits = PrivilegeBits.getInstance();
        PrivilegeBits parentBits = PrivilegeBits.getInstance();

        Iterator<PermissionEntry> it = getEntryIterator(path, Predicates.alwaysTrue());
        while (it.hasNext()) {
            PermissionEntry entry = it.next();
            PrivilegeBits entryBits = entry.getPrivilegeBits();
            if (parentPredicate.apply(entry)) {
                parentBits.add(entryBits);
            }
            if (predicate.apply(entry)) {
                bits.add(entryBits);
            }
            allows |= PrivilegeBits.calculatePermissions(bits, parentBits, true);
            if ((allows | ~permissions) == -1) {
                return true;
            }
        }
        return false;
    }

    private boolean isGrantedOnEffective(@NotNull Tree tree, long permission) {
        long toTest = permission & Permissions.MODIFY_ACCESS_CONTROL;
        if (Permissions.NO_PERMISSION == toTest) {
            return Boolean.TRUE;
        }

        String effectivePath = getEffectivePath(tree);
        if (effectivePath == null) {
            return Boolean.TRUE;
        } else if (REPOSITORY_PERMISSION_PATH.equals(effectivePath)) {
            return getRepositoryPermission().isGranted(toTest);
        } else {
            Tree effectiveTree = immutableRoot.getTree(effectivePath);
            return isGranted(effectiveTree, null, toTest);
        }
    }

    @NotNull
    private PrivilegeBits getGrantedPrivilegeBits(@Nullable Tree tree) {
        Tree readOnly = (tree == null) ? null : getReadOnlyTree(tree);
        PrivilegeBits subtract = PrivilegeBits.EMPTY;
        if (readOnly != null) {
            TreeType type = typeProvider.getType(readOnly);
            switch (type) {
                case HIDDEN:
                case INTERNAL:
                    return PrivilegeBits.EMPTY;
                case VERSION:
                    if (!isVersionStoreTree(readOnly)) {
                        Tree versionableTree = getVersionableTree(readOnly);
                        if (versionableTree == null) {
                            // unable to determine the location of the versionable item -> deny access.
                            return PrivilegeBits.EMPTY;
                        } else {
                            readOnly = versionableTree;
                        }
                    }
                    break;
                case ACCESS_CONTROL:
                    subtract = getBitsToSubtract(readOnly);
                    break;
                default: // covered below
                    break;

            }
        }

        String oakPath;
        Predicate<PermissionEntry> predicate;
        if (readOnly == null) {
            oakPath = REPOSITORY_PERMISSION_PATH;
            predicate = EntryPredicate.create(null);
        } else {
            oakPath = readOnly.getPath();
            predicate = EntryPredicate.create(readOnly, null);
        }
        PrivilegeBits granted = getGrantedPrivilegeBits(oakPath, predicate);
        return subtract.isEmpty() ? granted : granted.diff(subtract);
    }

    /**
     * In case the tree of type access-control represents a principal-based entry or a restriction defined below,
     * modify-access-control is only granted if it is also granted on the effective target path.
     * Calculate if those permissions are granted and if not subtract them later from the final result.
     *
     * @param tree A read-only tree of type ACCESS_CONTROL.
     * @return PrivilegeBits to be subtracted from the final result or {@link PrivilegeBits#EMPTY}, if the given tree
     * does not represent a principal-based entry or both read and modify access control is granted and nothing needs to
     * be subtracted.
     */
    @NotNull
    PrivilegeBits getBitsToSubtract(@NotNull Tree tree) {
        String effectivePath = getEffectivePath(tree);
        if (effectivePath == null) {
            return PrivilegeBits.EMPTY;
        } else {
            return modAcBits.modifiable().diff(getGrantedPrivilegeBits(effectivePath, EntryPredicate.create(effectivePath)));
        }
    }

    @NotNull
    private PrivilegeBits getGrantedPrivilegeBits(@NotNull String oakPath, @NotNull Predicate<PermissionEntry> predicate) {
        PrivilegeBits pb = PrivilegeBits.getInstance();
        Iterator<PermissionEntry> entries = getEntryIterator(oakPath, predicate);
        while (entries.hasNext()) {
            pb.add(entries.next().getPrivilegeBits());
        }
        return pb;
    }

    @NotNull
    private Tree getReadOnlyTree(@NotNull Tree tree) {
        if (tree instanceof ReadOnly) {
            return tree;
        } else {
            return immutableRoot.getTree(tree.getPath());
        }
    }

    @Nullable
    private String getEffectivePath(@NotNull Tree tree) {
        Tree principalEntry = null;
        if (Utils.isPrincipalEntry(tree)) {
            principalEntry = tree;
        } else if (Utils.isPrincipalEntry(tree.getParent())) {
            principalEntry = tree.getParent();
        }
        return (principalEntry == null) ? null :  principalEntry.getProperty(REP_EFFECTIVE_PATH).getValue(Type.STRING);
    }

    @Nullable
    private Tree getVersionableTree(@NotNull Tree versionTree) {
        return ReadOnlyVersionManager.getInstance(immutableRoot, NamePathMapper.DEFAULT).getVersionable(versionTree, workspaceName);
    }

    private boolean isVersionStoreTree(@NotNull Tree tree) {
        return ReadOnlyVersionManager.isVersionStoreTree(tree);
    }

    @NotNull
    TreePermission getTreePermission(@NotNull String name, @NotNull NodeState nodeState, @NotNull AbstractTreePermission parentTreePermission) {
        Tree readOnly = mgrProvider.getTreeProvider().createReadOnlyTree(parentTreePermission.getTree(), name, nodeState);
        return getTreePermission(readOnly, typeProvider.getType(readOnly, parentTreePermission.getType()), parentTreePermission);
    }

    //--------------------------------------------------------------------------
    private final class RepositoryPermissionImpl implements RepositoryPermission {

        private long grantedPermissions = -1;

        @Override
        public boolean isGranted(long repositoryPermissions) {
            return Permissions.includes(getGranted(), repositoryPermissions);
        }

        private long getGranted() {
            if (grantedPermissions == -1) {
                PrivilegeBits pb = getGrantedPrivilegeBits(REPOSITORY_PERMISSION_PATH, EntryPredicate.create(null));
                grantedPermissions = PrivilegeBits.calculatePermissions(pb, PrivilegeBits.EMPTY, true);
            }
            return grantedPermissions;
        }

        private void refresh() {
            grantedPermissions = -1;
        }
    }

    private final class RegularTreePermission extends AbstractTreePermission {

        RegularTreePermission(@NotNull Tree tree, @NotNull TreeType type) {
            super(tree, type);
        }

        @Override
        PrincipalBasedPermissionProvider getPermissionProvider() {
            return PrincipalBasedPermissionProvider.this;
        }
    }

    private final class VersionTreePermission extends AbstractTreePermission implements VersionConstants {

        private final Tree versionTree;

        VersionTreePermission(@NotNull Tree versionTree, @NotNull Tree versionableTree) {
            super(versionableTree, TreeType.VERSION);
            this.versionTree = versionTree;
        }

        @Override
        PrincipalBasedPermissionProvider getPermissionProvider() {
            return PrincipalBasedPermissionProvider.this;
        }

        @Override
        public @NotNull TreePermission getChildPermission(@NotNull String childName, @NotNull NodeState childState) {
            Tree childVersionTree = mgrProvider.getTreeProvider().createReadOnlyTree(versionTree, childName, childState);
            Tree childVersionableTree;
            String primaryType = NodeStateUtils.getPrimaryTypeName(childState);
            if (VERSION_NODE_NAMES.contains(childName) || NT_VERSION.equals(primaryType)) {
                // permissions of the original versionable node still apply to versioning related structure inside the
                // version histore tree (labels, references, the frozen node itself)
                childVersionableTree = getTree();
            } else {
                // internals of the frozen node -> build new child tree for evaluation
                childVersionableTree = getTree().getChild(childName);
            }
            return new VersionTreePermission(childVersionTree, childVersionableTree);
        }
    }
}
