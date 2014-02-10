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
import java.security.acl.Group;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.tree.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterators.concat;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission.ALL;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission.EMPTY;

/**
 * TODO: WIP
 * FIXME: decide on where to filter out hidden items (OAK-753)
 */
final class CompiledPermissionImpl implements CompiledPermissions, PermissionConstants {

    private static final Logger log = LoggerFactory.getLogger(CompiledPermissionImpl.class);

    private static final Map<Long, PrivilegeBits> READ_BITS = ImmutableMap.of(
            Permissions.READ, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ),
            Permissions.READ_NODE, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_NODES),
            Permissions.READ_PROPERTY, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_PROPERTIES),
            Permissions.READ_ACCESS_CONTROL, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ_ACCESS_CONTROL));

    private ImmutableRoot root;

    private final String workspaceName;

    private final ReadPolicy readPolicy;

    private PermissionStoreImpl store;
    private final PermissionEntryProvider userStore;
    private final PermissionEntryProvider groupStore;

    private PrivilegeBitsProvider bitsProvider;

    private final TreeTypeProvider typeProvider;

    private CompiledPermissionImpl(@Nonnull PermissionEntryCache.Local cache,
                                   @Nonnull Set<Principal> principals,
                                   @Nonnull ImmutableRoot root, @Nonnull String workspaceName,
                                   @Nonnull RestrictionProvider restrictionProvider,
                                   @Nonnull AuthorizationConfiguration acConfig) {
        this.root = root;
        this.workspaceName = workspaceName;

        bitsProvider = new PrivilegeBitsProvider(root);
        Set<String> readPaths = acConfig.getParameters().getConfigValue(PARAM_READ_PATHS, DEFAULT_READ_PATHS);
        readPolicy = (readPaths.isEmpty()) ? EmptyReadPolicy.INSTANCE : new DefaultReadPolicy(readPaths);

        // setup
        store = new PermissionStoreImpl(root, workspaceName, restrictionProvider);
        Set<String> userNames = new HashSet<String>(principals.size());
        Set<String> groupNames = new HashSet<String>(principals.size());
        for (Principal principal : principals) {
            if (principal instanceof Group) {
                groupNames.add(principal.getName());
            } else {
                userNames.add(principal.getName());
            }
        }

        userStore = new PermissionEntryProviderImpl(store, cache, userNames);
        groupStore = new PermissionEntryProviderImpl(store, cache, groupNames);

        typeProvider = new TreeTypeProvider(acConfig.getContext());
    }

    static CompiledPermissions create(@Nonnull ImmutableRoot root, @Nonnull String workspaceName,
                                      @Nonnull Set<Principal> principals,
                                      @Nonnull AuthorizationConfiguration acConfig,
                                      @Nonnull PermissionEntryCache.Local cache) {
        Tree permissionsTree = PermissionUtil.getPermissionsRoot(root, workspaceName);
        if (!permissionsTree.exists() || principals.isEmpty()) {
            return NoPermissions.getInstance();
        } else {
            return new CompiledPermissionImpl(cache, principals, root, workspaceName, acConfig.getRestrictionProvider(), acConfig);
        }
    }

    //------------------------------------------------< CompiledPermissions >---
    @Override
    public void refresh(@Nonnull ImmutableRoot root, @Nonnull String workspaceName) {
        this.root = root;
        this.bitsProvider = new PrivilegeBitsProvider(root);
        store.flush(root);
        userStore.flush();
        groupStore.flush();
    }

    @Override
    public RepositoryPermission getRepositoryPermission() {
        return new RepositoryPermission() {
            @Override
            public boolean isGranted(long repositoryPermissions) {
                return hasPermissions(getEntryIterator(new EntryPredicate()), repositoryPermissions, null);
            }
        };
    }

    @Override
    public TreePermission getTreePermission(@Nonnull ImmutableTree tree, @Nonnull TreePermission parentPermission) {
        if (tree.isRoot()) {
            return new TreePermissionImpl(tree, TreeTypeProvider.TYPE_DEFAULT, EMPTY);
        }
        int parentType = getParentType(parentPermission);
        int type = typeProvider.getType(tree, parentType);
        switch (type) {
            case TreeTypeProvider.TYPE_HIDDEN:
                // TODO: OAK-753 decide on where to filter out hidden items.
                return ALL;
            case TreeTypeProvider.TYPE_VERSION:
                String ntName = TreeUtil.getPrimaryTypeName(tree);
                if (ntName == null) {
                    return EMPTY;
                }
                if (VersionConstants.VERSION_STORE_NT_NAMES.contains(ntName) || VersionConstants.NT_ACTIVITY.equals(ntName)) {
                    return new TreePermissionImpl(tree, TreeTypeProvider.TYPE_VERSION, parentPermission);
                } else {
                    ImmutableTree versionableTree = getVersionableTree(tree);
                    if (versionableTree == null) {
                        log.warn("Cannot retrieve versionable node for " + tree.getPath());
                        return EMPTY;
                    } else {
                        // TODO: may return wrong results in case of restrictions
                        // TODO that would match the path of the versionable node
                        // TODO (or item in the subtree) but that item no longer exists
                        // TODO -> evaluation by path would be more accurate (-> see #isGranted)
                        while (!versionableTree.exists()) {
                            versionableTree = versionableTree.getParent();
                        }
                        TreePermission pp = getParentPermission(versionableTree, TreeTypeProvider.TYPE_VERSION);
                        return new TreePermissionImpl(versionableTree, TreeTypeProvider.TYPE_VERSION, pp);
                    }
                }
            case TreeTypeProvider.TYPE_INTERNAL:
                return EMPTY;
            default:
                return new TreePermissionImpl(tree, type, parentPermission);
        }
    }

    @Nonnull
    private TreePermission getParentPermission(@Nonnull ImmutableTree tree, int type) {
        List<ImmutableTree> trees = new ArrayList<ImmutableTree>();
        while (!tree.isRoot()) {
            tree = tree.getParent();
            if (tree.exists()) {
                trees.add(0, tree);
            }
        }
        TreePermission pp = EMPTY;
        for (ImmutableTree tr : trees) {
            pp = new TreePermissionImpl(tr, type, pp);
        }
        return pp;
    }

    @Override
    public boolean isGranted(@Nonnull ImmutableTree tree, @Nullable PropertyState property, long permissions) {
        int type = typeProvider.getType(tree);
        switch (type) {
            case TreeTypeProvider.TYPE_HIDDEN:
                // TODO: OAK-753 decide on where to filter out hidden items.
                return true;
            case TreeTypeProvider.TYPE_VERSION:
                Tree versionableTree = getVersionableTree(tree);
                if (versionableTree == null) {
                    // unable to determine the location of the versionable item -> deny access.
                    return false;
                }
                if (versionableTree.exists()) {
                    return internalIsGranted(versionableTree, property, permissions);
                } else {
                    // versionable node does not exist (anymore) in this workspace;
                    // use best effort calculation based on the item path.
                    String path = versionableTree.getPath();
                    if (property != null) {
                        path = PathUtils.concat(path, property.getName());
                    }
                    return isGranted(path, permissions);
                }
            case TreeTypeProvider.TYPE_INTERNAL:
                return false;
            default:
                return internalIsGranted(tree, property, permissions);
        }
    }

    @Override
    public boolean isGranted(@Nonnull String path, long permissions) {
        Iterator<PermissionEntry> it = getEntryIterator(new EntryPredicate(path, Permissions.respectParentPermissions(permissions)));
        return hasPermissions(it, permissions, path);
    }

    @Override
    public Set<String> getPrivileges(@Nullable ImmutableTree tree) {
        return bitsProvider.getPrivilegeNames(internalGetPrivileges(tree));
    }

    @Override
    public boolean hasPrivileges(@Nullable ImmutableTree tree, @Nonnull String... privilegeNames) {
        return internalGetPrivileges(tree).includes(bitsProvider.getBits(privilegeNames));
    }

    //------------------------------------------------------------< private >---

    private boolean internalIsGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
        Iterator<PermissionEntry> it = getEntryIterator(tree, property, permissions);
        return hasPermissions(it, permissions, tree.getPath());
    }

    private boolean hasPermissions(@Nonnull Iterator<PermissionEntry> entries,
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
                allowBits.addDifference(entry.privilegeBits, denyBits);
                long ap = PrivilegeBits.calculatePermissions(allowBits, parentAllowBits, true);
                allows |= Permissions.diff(ap, denies);
                if ((allows | ~permissions) == -1) {
                    return true;
                }
            } else {
                denyBits.addDifference(entry.privilegeBits, allowBits);
                long dp = PrivilegeBits.calculatePermissions(denyBits, parentDenyBits, false);
                denies |= Permissions.diff(dp, allows);
                if (Permissions.includes(denies, permissions)) {
                    return false;
                }
            }
        }

        return (allows | ~permissions) == -1;
    }

    @Nonnull
    private PrivilegeBits internalGetPrivileges(@Nullable ImmutableTree tree) {
        int type = (tree == null) ? TreeTypeProvider.TYPE_DEFAULT : typeProvider.getType(tree);
        switch (type) {
            case TreeTypeProvider.TYPE_HIDDEN:
                return PrivilegeBits.EMPTY;
            case TreeTypeProvider.TYPE_VERSION:
                Tree versionableTree = getVersionableTree(tree);
                if (versionableTree == null || !versionableTree.exists()) {
                    // unable to determine the location of the versionable item -> deny access.
                    // TODO : add proper handling for cases where the versionable node does not exist (anymore)
                    return PrivilegeBits.EMPTY;
                }  else {
                    return getPrivilegeBits(versionableTree);
                }
            case TreeTypeProvider.TYPE_INTERNAL:
                return PrivilegeBits.EMPTY;
            default:
                return getPrivilegeBits(tree);
        }
    }

    @Nonnull
    private PrivilegeBits getPrivilegeBits(@Nullable Tree tree) {
        EntryPredicate pred = (tree == null)
                ? new EntryPredicate()
                : new EntryPredicate(tree, null, false);
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

    @Nonnull
    private Iterator<PermissionEntry> getEntryIterator(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
        return getEntryIterator(new EntryPredicate(tree, property, Permissions.respectParentPermissions(permissions)));
    }

    @Nonnull
    private Iterator<PermissionEntry> getEntryIterator(@Nonnull EntryPredicate predicate) {
        Iterator<PermissionEntry> userEntries = userStore.getEntryIterator(predicate);
        Iterator<PermissionEntry> groupEntries = groupStore.getEntryIterator(predicate);
        return concat(userEntries, groupEntries);
    }

    @CheckForNull
    private ImmutableTree getVersionableTree(@Nonnull ImmutableTree versionStoreTree) {
        String relPath = "";
        String versionablePath = null;
        Tree t = versionStoreTree;
        while (t.exists() && !t.isRoot() && !VersionConstants.VERSION_STORE_ROOT_NAMES.contains(t.getName())) {
            String ntName = TreeUtil.getPrimaryTypeName(t);
            if (VersionConstants.JCR_FROZENNODE.equals(t.getName()) && t != versionStoreTree) {
                relPath = PathUtils.relativize(t.getPath(), versionStoreTree.getPath());
            } else if (JcrConstants.NT_VERSIONHISTORY.equals(ntName)) {
                PropertyState prop = t.getProperty(workspaceName);
                if (prop != null) {
                    versionablePath = PathUtils.concat(prop.getValue(Type.PATH), relPath);
                }
                return (versionablePath == null) ? null : root.getTree(versionablePath);
            } else if (VersionConstants.NT_CONFIGURATION.equals(ntName)) {
                String rootId = TreeUtil.getString(t, VersionConstants.JCR_ROOT);
                if (rootId != null) {
                    versionablePath = new IdentifierManager(root).getPath(rootId);
                    return (versionablePath == null) ? null : root.getTree(versionablePath);
                } else {
                    log.error("Missing mandatory property jcr:root with configuration node.");
                    return null;
                }
            } else if (VersionConstants.NT_ACTIVITY.equals(ntName)) {
                return versionStoreTree;
            }
            t = t.getParent();
        }

        // intermediate node in the version, configuration or activity store that
        // matches none of the special conditions checked above -> regular permission eval.
        return versionStoreTree;
    }

    private static int getParentType(@Nonnull TreePermission parentPermission) {
        if (parentPermission instanceof TreePermissionImpl) {
            return ((TreePermissionImpl) parentPermission).type;
        } else if (parentPermission == TreePermission.EMPTY) {
            return TreeTypeProvider.TYPE_DEFAULT;
        } else {
            throw new IllegalArgumentException("Illegal TreePermission implementation.");
        }
    }

    private final class TreePermissionImpl implements TreePermission {

        private final ImmutableTree tree;
        private final TreePermissionImpl parent;

        private final int type;
        private final boolean readableTree;

        private Collection<PermissionEntry> userEntries;
        private Collection<PermissionEntry> groupEntries;

        private boolean skipped;
        private ReadStatus readStatus;

        private TreePermissionImpl(ImmutableTree tree, int treeType, TreePermission parentPermission) {
            this.tree = tree;
            if (parentPermission instanceof TreePermissionImpl) {
                parent = (TreePermissionImpl) parentPermission;
            } else {
                parent = null;
            }
            readableTree = readPolicy.isReadableTree(tree, parent);
            type = treeType;
        }

        //-------------------------------------------------< TreePermission >---
        @Override
        public TreePermission getChildPermission(String childName, NodeState childState) {
            ImmutableTree childTree = new ImmutableTree(tree, childName, childState);
            return getTreePermission(childTree, this);
        }

        @Override
        public boolean canRead() {
            boolean isAcTree = isAcTree();
            if (!isAcTree && readableTree) {
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
        public boolean canRead(@Nonnull PropertyState property) {
            boolean isAcTree = isAcTree();
            if (!isAcTree && readableTree) {
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
                    return (entry.isAllow);
                }
            }
            return false;
        }

        @Override
        public boolean canReadAll() {
            // TODO: best effort approach to detect full read-access within a given tree.
            return readStatus != null && readStatus.allowsAll();
        }

        @Override
        public boolean canReadProperties() {
                // TODO: best effort approach to detect full read-property permission
            return readStatus != null && readStatus.allowsProperties();
        }

        @Override
        public boolean isGranted(long permissions) {
            return hasPermissions(getIterator(null, permissions), permissions, tree.getPath());
        }

        @Override
        public boolean isGranted(long permissions, @Nonnull PropertyState property) {
            return hasPermissions(getIterator(property, permissions), permissions, tree.getPath());
        }

        //--------------------------------------------------------< private >---
        private Iterator<PermissionEntry> getIterator(@Nullable PropertyState property, long permissions) {
            EntryPredicate predicate = new EntryPredicate(tree, property, Permissions.respectParentPermissions(permissions));
            return concat(new LazyIterator(this, true, predicate), new LazyIterator(this, false, predicate));
        }

        private Iterator<PermissionEntry> getUserEntries() {
            if (userEntries == null) {
                userEntries = userStore.getEntries(tree);
            }
            return userEntries.iterator();
        }

        private Iterator<PermissionEntry> getGroupEntries() {
            if (groupEntries == null) {
                groupEntries = groupStore.getEntries(tree);
            }
            return groupEntries.iterator();
        }

        private boolean isAcTree() {
            return type == TreeTypeProvider.TYPE_AC;
        }
    }

    private static final class LazyIterator extends AbstractLazyIterator<PermissionEntry> {

        private final TreePermissionImpl treePermission;
        private final boolean isUser;

        private final EntryPredicate predicate;

        // the ordered permission entries at a given path in the hierarchy
        private Iterator<PermissionEntry> nextEntries = Iterators.emptyIterator();

        private TreePermissionImpl tp;

        private LazyIterator(@Nonnull TreePermissionImpl treePermission, boolean isUser, @Nonnull EntryPredicate predicate) {
            this.treePermission = treePermission;
            this.isUser = isUser;
            this.predicate = predicate;

            tp = treePermission;
        }

        @Override
        protected PermissionEntry getNext() {
            PermissionEntry next = null;
            while (next == null) {
                if (nextEntries.hasNext()) {
                    PermissionEntry pe = nextEntries.next();
                    if (predicate.apply(pe)) {
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

        boolean isReadableTree(@Nonnull Tree tree, @Nullable TreePermissionImpl parent);
        boolean isReadableTree(@Nonnull Tree tree, boolean exactMatch);
        boolean isReadablePath(@Nullable String treePath, boolean exactMatch);
    }

    private static final class EmptyReadPolicy implements ReadPolicy {

        private static final ReadPolicy INSTANCE = new EmptyReadPolicy();

        private EmptyReadPolicy() {}

        @Override
        public boolean isReadableTree(@Nonnull Tree tree, @Nullable TreePermissionImpl parent) {
            return false;
        }

        public boolean isReadableTree(@Nonnull Tree tree, boolean exactMatch) {
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

        private DefaultReadPolicy(Set<String> readPaths) {
            this.readPaths = readPaths.toArray(new String[readPaths.size()]);
            altReadPaths = new String[readPaths.size()];
            int i = 0;
            for (String p : this.readPaths) {
                altReadPaths[i++] = p + '/';
            }
            // optimize evaluation for default setup where all readable paths
            // are located underneath /jcr:system
            isDefaultPaths = (readPaths.size() == DEFAULT_READ_PATHS.size()) && readPaths.containsAll(DEFAULT_READ_PATHS);
        }

        public boolean isReadableTree(@Nonnull Tree tree, @Nullable TreePermissionImpl parent) {
            if (parent != null) {
                if (parent.readableTree) {
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

        public boolean isReadableTree(@Nonnull Tree tree, boolean exactMatch) {
            String targetPath = tree.getPath();
            for (String path : readPaths) {
                if (targetPath.equals(path)) {
                    return true;
                }
            }
            if (!exactMatch) {
                for (String path : altReadPaths) {
                    if (targetPath.startsWith(path)) {
                        return true;
                    }
                }
            }
            return false;
        }

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
