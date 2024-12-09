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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.security.authorization.ProviderCtx;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.GroupPrincipals;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.guava.common.collect.Iterators.concat;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission.ALL;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission.EMPTY;

final class CompiledPermissionImpl implements CompiledPermissions, PermissionConstants {

    private static final Logger log = LoggerFactory.getLogger(CompiledPermissionImpl.class);

    private static final Map<Long, PrivilegeBits> READ_BITS = Map.of(
            Permissions.READ, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ),
            Permissions.READ_NODE, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_NODES),
            Permissions.READ_PROPERTY, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_PROPERTIES),
            Permissions.READ_ACCESS_CONTROL, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));

    private final String workspaceName;
    private final ReadPolicy readPolicy;
    private final PermissionStore store;
    private final PermissionEntryProvider userStore;
    private final PermissionEntryProvider groupStore;

    private final TreeTypeProvider typeProvider;
    private final ProviderCtx providerCtx;

    private Root root;
    private ReadOnlyVersionManager versionManager;
    private PrivilegeBitsProvider bitsProvider;

    private CompiledPermissionImpl(@NotNull Set<Principal> principals,
                                   @NotNull Root root,
                                   @NotNull String workspaceName,
                                   @NotNull PermissionStore store,
                                   @NotNull ConfigurationParameters options,
                                   @NotNull Context ctx,
                                   @NotNull ProviderCtx providerCtx) {
        this.root = root;
        this.workspaceName = workspaceName;
        this.providerCtx = providerCtx;

        bitsProvider = new PrivilegeBitsProvider(root);

        Set<String> readPaths = options.getConfigValue(PARAM_READ_PATHS, DEFAULT_READ_PATHS);
        readPolicy = (readPaths.isEmpty()) ? EmptyReadPolicy.INSTANCE : new DefaultReadPolicy(readPaths);

        // setup
        this.store = store;
        Set<String> userNames = new HashSet<>(principals.size());
        Set<String> groupNames = new HashSet<>(principals.size());
        for (Principal principal : principals) {
            if (GroupPrincipals.isGroup(principal)) {
                groupNames.add(principal.getName());
            } else {
                userNames.add(principal.getName());
            }
        }

        if (!userNames.isEmpty()) {
            userStore = new PermissionEntryProviderImpl(store, userNames, options);
        } else {
            userStore = null;
        }
        if (!groupNames.isEmpty()) {
            groupStore = new PermissionEntryProviderImpl(store, groupNames, options);
        } else {
            groupStore = null;
        }

        typeProvider = new TreeTypeProvider(ctx);
    }

    @NotNull
    static CompiledPermissions create(@NotNull Root root,
                                      @NotNull String workspaceName,
                                      @NotNull PermissionStore store,
                                      @NotNull Set<Principal> principals,
                                      @NotNull ConfigurationParameters options,
                                      @NotNull Context ctx,
                                      @NotNull ProviderCtx providerCtx) {
        Tree permissionsTree = PermissionUtil.getPermissionsRoot(root, workspaceName);
        if (!permissionsTree.exists() || principals.isEmpty()) {
            return NoPermissions.getInstance();
        } else {
            return new CompiledPermissionImpl(principals, root, workspaceName, store, options, ctx, providerCtx);
        }
    }

    //------------------------------------------------< CompiledPermissions >---
    @Override
    public void refresh(@NotNull Root root, @NotNull String workspaceName) {
        this.root = root;
        this.bitsProvider = new PrivilegeBitsProvider(root);
        this.versionManager = null;

        store.flush(root);
        if (userStore != null) {
            userStore.flush();
        }
        if (groupStore != null) {
            groupStore.flush();
        }
    }

    @NotNull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return repositoryPermissions -> {
            EntryPredicate predicate = EntryPredicate.create();
            return hasPermissions(getEntryIterator(predicate), predicate, repositoryPermissions, null);
        };
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreePermission parentPermission) {
        return getTreePermission(tree, typeProvider.getType(tree, getParentType(parentPermission)), parentPermission);
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreeType type, @NotNull TreePermission parentPermission) {
        if (tree.isRoot()) {
            return createRootPermission(tree);
        }
        if (parentPermission instanceof VersionTreePermission) {
            return ((VersionTreePermission) parentPermission).createChildPermission(tree);
        } else if (parentPermission instanceof RepoPolicyTreePermission) {
            return ((RepoPolicyTreePermission)parentPermission).getChildPermission();
        }
        switch (type) {
            case HIDDEN:
                return ALL;
            case VERSION:
                if (ReadOnlyVersionManager.isVersionStoreTree(tree)) {
                    return new TreePermissionImpl(tree, TreeType.VERSION, parentPermission);
                } else {
                    Tree versionableTree = getVersionManager().getVersionable(tree, workspaceName);
                    if (versionableTree == null) {
                        log.warn("Cannot retrieve versionable node for {}", tree.getPath());
                        return EMPTY;
                    } else {
                        /*
                         * NOTE: may return wrong results in case of restrictions
                         * that would match the path of the versionable node
                         * (or item in the subtree) but that item no longer exists
                         * -> evaluation by path might be more accurate (-> see #isGranted)
                         */
                        return new VersionTreePermission(tree, buildVersionDelegatee(versionableTree), providerCtx.getTreeProvider());
                    }
                }
            case ACCESS_CONTROL:
                if (AccessControlConstants.REP_REPO_POLICY.equals(tree.getName())) {
                     return new RepoPolicyTreePermission(getRepositoryPermission());
                } else {
                     return new TreePermissionImpl(tree, type, parentPermission);
                }
            case INTERNAL:
                return InternalTreePermission.INSTANCE;
            default:
                return new TreePermissionImpl(tree, type, parentPermission);
        }
    }

    @NotNull
    private TreePermission buildVersionDelegatee(@NotNull Tree versionableTree) {
        while (!versionableTree.exists()) {
            versionableTree = versionableTree.getParent();
        }
        if (versionableTree.isRoot()) {
            return createRootPermission(versionableTree);
        }

        TreeType type = typeProvider.getType(versionableTree);
        switch (type) {
            case HIDDEN : return ALL;
            case INTERNAL : return InternalTreePermission.INSTANCE;
            // case VERSION is never expected here
            default:
                return new TreePermissionImpl(versionableTree, type, buildParentPermission(versionableTree));
        }
    }

    @NotNull
    private TreePermission buildParentPermission(@NotNull Tree tree) {
        List<Tree> trees = new ArrayList<>();
        while (!tree.isRoot()) {
            tree = tree.getParent();
            trees.add(0, tree);
        }
        TreePermission pp = EMPTY;
        TreeType type = TreeType.DEFAULT;
        for (Tree tr : trees) {
            type = typeProvider.getType(tr, type);
            pp = new TreePermissionImpl(tr, type, pp);
        }
        return pp;
    }

    @Override
    public boolean isGranted(@NotNull Tree tree, @Nullable PropertyState property, long permissions) {
        TreeType type = typeProvider.getType(tree);
        switch (type) {
            case HIDDEN:
                return true;
            case VERSION:
                Tree evalTree = getEvaluationTree(tree);
                if (evalTree == null) {
                    // unable to determine the location of the versionable item -> deny access.
                    return false;
                }
                if (evalTree.exists()) {
                    return internalIsGranted(evalTree, property, permissions);
                } else {
                    // the versionable node does not exist (anymore) in this workspace
                    // use best effort calculation based on the item path.
                    String path = evalTree.getPath();
                    if (property != null) {
                        path = PathUtils.concat(path, property.getName());
                    }
                    return isGranted(path, property != null, permissions);
                }
            case INTERNAL:
                return false;
            default:
                return internalIsGranted(tree, property, permissions);
        }
    }

    @Override
    public boolean isGranted(@NotNull String path, boolean isProperty, long permissions) {
        EntryPredicate predicate = EntryPredicate.create(path, isProperty, Permissions.respectParentPermissions(permissions));
        return hasPermissions(getEntryIterator(predicate), predicate, permissions, path);
    }

    @Override
    public boolean isGranted(@NotNull String path, long permissions) {
        EntryPredicate predicate = EntryPredicate.create(path, Permissions.respectParentPermissions(permissions));
        return hasPermissions(getEntryIterator(predicate), predicate, permissions, path);
    }

    @NotNull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        return bitsProvider.getPrivilegeNames(internalGetPrivileges(tree));
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @NotNull String... privilegeNames) {
        return internalGetPrivileges(tree).includes(bitsProvider.getBits(privilegeNames));
    }

    //------------------------------------------------------------< private >---

    private boolean internalIsGranted(@NotNull Tree tree, @Nullable PropertyState property, long permissions) {
        EntryPredicate predicate = EntryPredicate.create(tree, property, Permissions.respectParentPermissions(permissions));
        return hasPermissions(getEntryIterator(predicate), predicate, permissions, tree.getPath());
    }

    private boolean hasPermissions(@NotNull Iterator<PermissionEntry> entries,
                                   @NotNull EntryPredicate predicate,
                                   long permissions, @Nullable String path) {
        // calculate readable paths if the given permissions includes any read permission.
        boolean isReadable = Permissions.diff(Permissions.READ, permissions) != Permissions.READ && readPolicy.isReadablePath(path, false);
        if (!entries.hasNext() && !isReadable) {
            return false;
        }

        boolean respectParent = (path != null) && Permissions.respectParentPermissions(permissions);

        long allows = (isReadable) ? Permissions.READ : Permissions.NO_PERMISSION;
        long denies = Permissions.NO_PERMISSION;

        PrivilegeBits allowBits = PrivilegeBits.getInstance();
        if (isReadable) {
            allowBits.add(bitsProvider.getBits(PrivilegeConstants.JCR_READ));
        }
        PrivilegeBits denyBits = PrivilegeBits.getInstance();
        PrivilegeBits parentAllowBits;
        PrivilegeBits parentDenyBits;
        String parentPath;

        if (respectParent) {
            parentAllowBits = PrivilegeBits.getInstance();
            parentDenyBits = PrivilegeBits.getInstance();
            parentPath = PermissionUtil.getParentPathOrNull(path);
        } else {
            parentAllowBits = PrivilegeBits.EMPTY;
            parentDenyBits = PrivilegeBits.EMPTY;
            parentPath = null;
        }

        while (entries.hasNext()) {
            PermissionEntry entry = entries.next();
            if (respectParent && (parentPath != null)) {
                boolean matchesParent = entry.matchesParent(parentPath);
                if (matchesParent) {
                    if (entry.isAllow) {
                        parentAllowBits.addDifference(entry.privilegeBits, parentDenyBits);
                    } else {
                        parentDenyBits.addDifference(entry.privilegeBits, parentAllowBits);
                    }
                }
            }

            if (entry.isAllow) {
                if (!respectParent || predicate.test(entry, false)) {
                    allowBits.addDifference(entry.privilegeBits, denyBits);
                }
                long ap = PrivilegeBits.calculatePermissions(allowBits, parentAllowBits, true);
                allows |= Permissions.diff(ap, denies);
                if ((allows | ~permissions) == -1) {
                    return true;
                }
            } else {
                if (!respectParent || predicate.test(entry, false)) {
                    denyBits.addDifference(entry.privilegeBits, allowBits);
                }
                long dp = PrivilegeBits.calculatePermissions(denyBits, parentDenyBits, false);
                denies |= Permissions.diff(dp, allows);
                if (Permissions.includes(denies, permissions)) {
                    return false;
                }
            }
        }

        return (allows | ~permissions) == -1;
    }

    @NotNull
    private PrivilegeBits internalGetPrivileges(@Nullable Tree tree) {
        if (tree == null) {
            return getPrivilegeBits(null);
        }

        TreeType type = typeProvider.getType(tree);
        switch (type) {
            case HIDDEN:
                return PrivilegeBits.EMPTY;
            case VERSION:
                Tree evalTree = getEvaluationTree(tree);
                if (evalTree == null || !evalTree.exists()) {
                    // unable to determine the location of the versionable item -> deny access.
                    return PrivilegeBits.EMPTY;
                }  else {
                    return getPrivilegeBits(evalTree);
                }
            case INTERNAL:
                return PrivilegeBits.EMPTY;
            default:
                return getPrivilegeBits(tree);
        }
    }

    @NotNull
    private PrivilegeBits getPrivilegeBits(@Nullable Tree tree) {
        EntryPredicate pred = (tree == null)
                ? EntryPredicate.create()
                : EntryPredicate.create(tree, null, false);
        Iterator<PermissionEntry> entries = getEntryIterator(pred);

        PrivilegeBits allowBits = PrivilegeBits.getInstance();
        PrivilegeBits denyBits = PrivilegeBits.getInstance();

        while (entries.hasNext()) {
            PermissionEntry entry = entries.next();
            if (entry.isAllow) {
                allowBits.addDifference(entry.privilegeBits, denyBits);
            } else {
                denyBits.addDifference(entry.privilegeBits, allowBits);
            }
        }

        // special handling for paths that are always readable
        if (tree != null && readPolicy.isReadableTree(tree, false)) {
            allowBits.add(bitsProvider.getBits(PrivilegeConstants.JCR_READ));
        }
        return allowBits;
    }

    @NotNull
    private Iterator<PermissionEntry> getEntryIterator(@NotNull EntryPredicate predicate) {
        Iterator<PermissionEntry> userEntries = Collections.emptyIterator();
        Iterator<PermissionEntry> groupEntries = Collections.emptyIterator();
        if (userStore != null) {
            userEntries = userStore.getEntryIterator(predicate);
        }
        if (groupStore != null) {
            groupEntries = groupStore.getEntryIterator(predicate);
        }
        return concat(userEntries, groupEntries);
    }

    @Nullable
    private Tree getEvaluationTree(@NotNull Tree versionStoreTree) {
        if (ReadOnlyVersionManager.isVersionStoreTree(versionStoreTree)) {
            return versionStoreTree;
        } else {
            return getVersionManager().getVersionable(versionStoreTree, workspaceName);
        }
    }

    @NotNull
    private ReadOnlyVersionManager getVersionManager() {
        if (versionManager == null) {
            versionManager = ReadOnlyVersionManager.getInstance(root, NamePathMapper.DEFAULT);
        }
        return versionManager;
    }

    @NotNull
    private static TreeType getParentType(@NotNull TreePermission parentPermission) {
        if (parentPermission instanceof TreePermissionImpl) {
            return ((TreePermissionImpl) parentPermission).type;
        } else if (parentPermission == TreePermission.EMPTY) {
            return TreeType.DEFAULT;
        } else if (parentPermission == InternalTreePermission.INSTANCE) {
            return TreeType.INTERNAL;
        } else if (parentPermission instanceof VersionTreePermission) {
            return TreeType.VERSION;
        } else if (parentPermission instanceof RepoPolicyTreePermission) {
            return TreeType.ACCESS_CONTROL;
        } else {
            throw new IllegalArgumentException("Illegal TreePermission implementation.");
        }
    }

    @NotNull
    private TreePermissionImpl createRootPermission(@NotNull Tree rootTree) {
        return new TreePermissionImpl(rootTree, TreeType.DEFAULT, EMPTY);
    }

    private final class TreePermissionImpl implements TreePermission {

        private final Tree tree;
        private final TreePermissionImpl parent;

        private final TreeType type;
        private final boolean isReadableTree;

        private Collection<PermissionEntry> userEntries;
        private Collection<PermissionEntry> groupEntries;

        private boolean skipped;
        private ReadStatus readStatus;

        private TreePermissionImpl(@NotNull Tree tree, @NotNull TreeType type, @NotNull TreePermission parentPermission) {
            this.tree = tree;
            this.type = type;
            if (parentPermission instanceof TreePermissionImpl) {
                parent = (TreePermissionImpl) parentPermission;
            } else {
                parent = null;
            }
            isReadableTree = readPolicy.isReadableTree(tree, parent);
        }

        //-------------------------------------------------< TreePermission >---
        @NotNull
        @Override
        public TreePermission getChildPermission(@NotNull String childName, @NotNull NodeState childState) {
            Tree childTree = providerCtx.getTreeProvider().createReadOnlyTree(tree, childName, childState);
            return getTreePermission(childTree, typeProvider.getType(childTree, type), this);
        }

        @Override
        public boolean canRead() {
            boolean isAcTree = isAcTree();
            if (!isAcTree && isReadableTree) {
                return true;
            }
            if (readStatus == null) {
                readStatus = ReadStatus.DENY_THIS;

                long permission = (isAcTree) ? Permissions.READ_ACCESS_CONTROL : Permissions.READ_NODE;
                PrivilegeBits requiredBits = READ_BITS.get(permission);

                Iterator<PermissionEntry> it = getIterator(null, permission);
                while (it.hasNext()) {
                    PermissionEntry entry = it.next();
                    if (entry.privilegeBits.includes(requiredBits)) {
                        readStatus = ReadStatus.create(entry, permission, skipped);
                        break;
                    } else if (permission == Permissions.READ_NODE &&
                            entry.privilegeBits.includes(READ_BITS.get(Permissions.READ_PROPERTY))) {
                        skipped = true;
                    }
                }
            }
            return readStatus.allowsThis();
        }

        @Override
        public boolean canRead(@NotNull PropertyState property) {
            boolean isAcTree = isAcTree();
            if (!isAcTree && isReadableTree) {
                return true;
            }
            if (readStatus != null && readStatus.allowsProperties()) {
                return true;
            }

            long permission = (isAcTree) ? Permissions.READ_ACCESS_CONTROL : Permissions.READ_PROPERTY;
            Iterator<PermissionEntry> it = getIterator(property, permission);
            while (it.hasNext()) {
                PermissionEntry entry = it.next();
                if (entry.privilegeBits.includes(READ_BITS.get(permission))) {
                    return entry.isAllow;
                }
            }
            return false;
        }

        @Override
        public boolean canReadAll() {
            return readStatus != null && readStatus.allowsAll();
        }

        @Override
        public boolean canReadProperties() {
            return readStatus != null && readStatus.allowsProperties();
        }

        @Override
        public boolean isGranted(long permissions) {
            EntryPredicate predicate = EntryPredicate.create(tree, null, Permissions.respectParentPermissions(permissions));
            Iterator<PermissionEntry> it = getIterator(predicate);
            return hasPermissions(it, predicate, permissions, tree.getPath());
        }

        @Override
        public boolean isGranted(long permissions, @NotNull PropertyState property) {
            EntryPredicate predicate = EntryPredicate.create(tree, property, Permissions.respectParentPermissions(permissions));
            Iterator<PermissionEntry> it = getIterator(predicate);
            return hasPermissions(it, predicate, permissions, tree.getPath());
        }

        //--------------------------------------------------------< private >---
        @NotNull
        private Iterator<PermissionEntry> getIterator(@Nullable PropertyState property, long permissions) {
            EntryPredicate predicate = EntryPredicate.create(tree, property, Permissions.respectParentPermissions(permissions));
            return getIterator(predicate);
        }

        @NotNull
        private Iterator<PermissionEntry> getIterator(@NotNull EntryPredicate predicate) {
            Iterator<PermissionEntry> userIt = Collections.emptyIterator();
            Iterator<PermissionEntry> groupIt = Collections.emptyIterator();
            if (userStore != null) {
                userIt = new LazyIterator(this, true, predicate);
            }
            if (groupStore != null) {
                groupIt = new LazyIterator(this, false, predicate);
            }
            return concat(userIt, groupIt);
        }

        @NotNull
        private Iterator<PermissionEntry> getUserEntries() {
            if (userEntries == null) {
                userEntries = userStore != null ? userStore.getEntries(tree) : Collections.emptyList();
            }
            return userEntries.iterator();
        }

        @NotNull
        private Iterator<PermissionEntry> getGroupEntries() {
            if (groupEntries == null) {
                groupEntries = groupStore != null ? groupStore.getEntries(tree) : Collections.emptyList();
            }
            return groupEntries.iterator();
        }

        private boolean isAcTree() {
            return type == TreeType.ACCESS_CONTROL;
        }
    }

    private static final class LazyIterator extends AbstractLazyIterator<PermissionEntry> {

        private final TreePermissionImpl treePermission;
        private final boolean isUser;

        private final EntryPredicate predicate;

        // the ordered permission entries at a given path in the hierarchy
        private Iterator<PermissionEntry> nextEntries = Collections.emptyIterator();

        private TreePermissionImpl tp;

        private LazyIterator(@NotNull TreePermissionImpl treePermission, boolean isUser, @NotNull EntryPredicate predicate) {
            this.treePermission = treePermission;
            this.isUser = isUser;
            this.predicate = predicate;

            tp = treePermission;
        }

        @Nullable
        @Override
        protected PermissionEntry getNext() {
            PermissionEntry next = null;
            while (next == null) {
                if (nextEntries.hasNext()) {
                    PermissionEntry pe = nextEntries.next();
                    if (predicate.test(pe)) {
                        next = pe;
                    } else {
                        treePermission.skipped  = true;
                    }
                } else {
                    if (tp == null) {
                        break;
                    }
                    nextEntries = (isUser) ? tp.getUserEntries() : tp.getGroupEntries();
                    tp = tp.parent;
                }
            }
            return next;
        }
    }

    private interface ReadPolicy {

        boolean isReadableTree(@NotNull Tree tree, @Nullable TreePermissionImpl parent);
        boolean isReadableTree(@NotNull Tree tree, boolean exactMatch);
        boolean isReadablePath(@Nullable String treePath, boolean exactMatch);
    }

    private static final class EmptyReadPolicy implements ReadPolicy {

        private static final ReadPolicy INSTANCE = new EmptyReadPolicy();

        private EmptyReadPolicy() {}

        @Override
        public boolean isReadableTree(@NotNull Tree tree, @Nullable TreePermissionImpl parent) {
            return false;
        }

        @Override
        public boolean isReadableTree(@NotNull Tree tree, boolean exactMatch) {
            return false;
        }

        @Override
        public boolean isReadablePath(@Nullable String treePath, boolean exactMatch) {
            return false;
        }
    }

    private static final class DefaultReadPolicy implements ReadPolicy {

        private final String[] readPaths;
        private final String[] altReadPaths;
        private final boolean isDefaultPaths;

        private DefaultReadPolicy(@NotNull Set<String> readPaths) {
            this.readPaths = readPaths.toArray(new String[0]);
            altReadPaths = new String[readPaths.size()];
            int i = 0;
            for (String p : this.readPaths) {
                altReadPaths[i++] = p + '/';
            }
            // optimize evaluation for default setup where all readable paths
            // are located underneath /jcr:system
            isDefaultPaths = (readPaths.size() == DEFAULT_READ_PATHS.size()) && readPaths.containsAll(DEFAULT_READ_PATHS);
        }

        @Override
        public boolean isReadableTree(@NotNull Tree tree, @Nullable TreePermissionImpl parent) {
            if (parent != null) {
                if (parent.isReadableTree) {
                    return true;
                } else if (!isDefaultPaths || parent.tree.getName().equals(JcrConstants.JCR_SYSTEM)) {
                    return isReadableTree(tree, true);
                } else  {
                    return false;
                }
            } else {
                return isReadableTree(tree, true);
            }
        }

        @Override
        public boolean isReadableTree(@NotNull Tree tree, boolean exactMatch) {
            return isReadablePath(tree.getPath(), exactMatch);
        }

        @Override
        public boolean isReadablePath(@Nullable String treePath, boolean exactMatch) {
            if (treePath != null) {
                for (String path : readPaths) {
                    if (treePath.equals(path)) {
                        return true;
                    }
                }
                if (!exactMatch) {
                    for (String path : altReadPaths) {
                        if (treePath.startsWith(path)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }
}
