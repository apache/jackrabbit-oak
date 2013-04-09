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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * TODO: WIP
 */
class CompiledPermissionImpl implements CompiledPermissions, PermissionConstants {

    private final Set<Principal> principals;
    private final RestrictionProvider restrictionProvider;
    private final Map<String, ImmutableTree> trees;

    private PrivilegeBitsProvider bitsProvider;
    private Map<Key, PermissionEntry> repoEntries;
    private Map<Key, PermissionEntry> userEntries;
    private Map<Key, PermissionEntry> groupEntries;

    CompiledPermissionImpl(@Nonnull Set<Principal> principals,
                           @Nonnull ImmutableTree permissionsTree,
                           @Nonnull PrivilegeBitsProvider bitsProvider,
                           @Nonnull RestrictionProvider restrictionProvider) {
        checkArgument(!principals.isEmpty());
        this.principals = principals;
        this.restrictionProvider = restrictionProvider;
        this.bitsProvider = bitsProvider;
        this.trees = new HashMap<String, ImmutableTree>(principals.size());
        buildEntries(permissionsTree);
    }

    void refresh(@Nonnull ImmutableTree permissionsTree,
                 @Nonnull PrivilegeBitsProvider bitsProvider) {
        this.bitsProvider = bitsProvider;
        boolean refresh = false;
        // test if a permission has been added for those principals that didn't have one before
        if (trees.size() != principals.size()) {
            for (Principal principal : principals) {
                if (!trees.containsKey(principal.getName()) && getPrincipalRoot(permissionsTree, principal) != null) {
                    refresh = true;
                    break;
                }
            }
        }
        // test if any of the trees has been modified in the mean time
        if (!refresh) {
            for (Map.Entry<String, ImmutableTree> entry : trees.entrySet()) {
                ImmutableTree t = entry.getValue();
                ImmutableTree t2 = permissionsTree.getChild(t.getName());
                if (t2 != null && !t.equals(t2)) {
                    refresh = true;
                    break;
                }
            }
        }

        if (refresh) {
            buildEntries(permissionsTree);
        }
    }

    //------------------------------------------------< CompiledPermissions >---
    @Override
    public ReadStatus getReadStatus(@Nonnull Tree tree, @Nullable PropertyState property) {
        long permission = (property == null) ? Permissions.READ_NODE : Permissions.READ_PROPERTY;
        Iterator<PermissionEntry> it = getEntryIterator(tree, property);
        while (it.hasNext()) {
            PermissionEntry entry = it.next();
            if (entry.readStatus != null) {
                return entry.readStatus;
            } else if (entry.privilegeBits.includesRead(permission)) {
                return (entry.isAllow) ? ReadStatus.ALLOW_THIS : ReadStatus.DENY_THIS;
            }
        }
        return ReadStatus.DENY_THIS;
    }

    @Override
    public boolean isGranted(long permissions) {
        return hasPermissions(repoEntries.values().iterator(), permissions, null, null);
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
        return hasPermissions(getEntryIterator(tree, property), permissions, tree, null);
    }

    @Override
    public boolean isGranted(@Nonnull String path, long permissions) {
        return hasPermissions(getEntryIterator(path), permissions, null, path);
    }

    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        return bitsProvider.getPrivilegeNames(getPrivilegeBits(tree));
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, String... privilegeNames) {
        return getPrivilegeBits(tree).includes(bitsProvider.getBits(privilegeNames));
    }

    //------------------------------------------------------------< private >---
    @CheckForNull
    private static ImmutableTree getPrincipalRoot(ImmutableTree permissionsTree, Principal principal) {
        return permissionsTree.getChild(Text.escapeIllegalJcrChars(principal.getName()));
    }

    private void buildEntries(@Nullable ImmutableTree permissionsTree) {
        if (permissionsTree == null) {
            repoEntries = Collections.emptyMap();
            userEntries = Collections.emptyMap();
            groupEntries = Collections.emptyMap();
        } else {
            EntriesBuilder builder = new EntriesBuilder();
            for (Principal principal : principals) {
                ImmutableTree t = getPrincipalRoot(permissionsTree, principal);
                if (t != null) {
                    trees.put(principal.getName(), t);
                    builder.addEntries(principal, t, restrictionProvider);
                }
            }
            repoEntries = builder.getRepoEntries();
            userEntries = builder.getUserEntries();
            groupEntries = builder.getGroupEntries();
            buildReadStatus(Iterables.<PermissionEntry>concat(userEntries.values(), groupEntries.values()));
        }
    }

    private static void buildReadStatus(Iterable<PermissionEntry> permissionEntries) {
        // TODO
    }

    private boolean hasPermissions(@Nonnull Iterator<PermissionEntry> entries,
                                   long permissions, @Nullable Tree tree, @Nullable String path) {
        if (!entries.hasNext()) {
            return false;
        }

        boolean respectParent = (tree != null || path != null) &&
                (Permissions.includes(permissions, Permissions.ADD_NODE) ||
                Permissions.includes(permissions, Permissions.REMOVE_NODE) ||
                Permissions.includes(permissions, Permissions.MODIFY_CHILD_NODE_COLLECTION));

        long allows = Permissions.NO_PERMISSION;
        long denies = Permissions.NO_PERMISSION;

        PrivilegeBits allowBits = PrivilegeBits.getInstance();
        PrivilegeBits denyBits = PrivilegeBits.getInstance();
        PrivilegeBits parentAllowBits;
        PrivilegeBits parentDenyBits;

        Tree parent;
        String parentPath;

        if (respectParent) {
            parentAllowBits = PrivilegeBits.getInstance();
            parentDenyBits = PrivilegeBits.getInstance();
            parent = (tree != null) ? tree.getParent() : null;
            parentPath = (path != null) ? Strings.emptyToNull(Text.getRelativeParent(path, 1)) : null;
        } else {
            parentAllowBits = PrivilegeBits.EMPTY;
            parentDenyBits = PrivilegeBits.EMPTY;
            parent = null;
            parentPath = null;
        }

        while (entries.hasNext()) {
            PermissionEntry entry = entries.next();
            if (respectParent && (parent != null || parentPath != null)) {
                boolean matchesParent = (parent != null) ? entry.matches(parent, null) : entry.matches(parentPath);
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
        return false;
    }

    private PrivilegeBits getPrivilegeBits(@Nullable Tree tree) {
        Iterator<PermissionEntry> entries = (tree == null) ?
                repoEntries.values().iterator() :
                getEntryIterator(tree, null);

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
        return allowBits;
    }

    private Iterator<PermissionEntry> getEntryIterator(@Nonnull Tree tree, @Nullable PropertyState property) {
        return Iterators.concat(new EntryIterator(userEntries, tree, property), new EntryIterator(groupEntries, tree, property));
    }

    private Iterator<PermissionEntry> getEntryIterator(@Nonnull String path) {
        return Iterators.concat(new EntryIterator(userEntries, path), new EntryIterator(groupEntries, path));
    }

    private static final class Key implements Comparable<Key> {

        private final String path;
        private final int depth;
        private final long index;

        private Key(Tree tree) {
            path = Strings.emptyToNull(TreeUtil.getString(tree, REP_ACCESS_CONTROLLED_PATH));
            depth = (path == null) ? 0 : PathUtils.getDepth(path);
            index = checkNotNull(tree.getProperty(REP_INDEX).getValue(Type.LONG)).longValue();
        }

        @Override
        public int compareTo(Key key) {
            checkNotNull(key);
            if (Objects.equal(path, key.path)) {
                if (index == key.index) {
                    return 0;
                } else if (index <                                                                                                                                                                                 key.index) {
                    return 1;
                } else {
                    return -1;
                }
            } else {
                if (depth == key.depth) {
                    return path.compareTo(key.path);
                } else {
                    return (depth < key.depth) ? 1 : -1;
                }
            }
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(path, index);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (o instanceof Key) {
                Key other = (Key) o;
                return index == other.index && Objects.equal(path, other.path);
            }
            return false;
        }
    }

    private static final class PermissionEntry {

        private final boolean isAllow;
        private final PrivilegeBits privilegeBits;
        private final String path;
        private final RestrictionPattern restriction;

        private ReadStatus readStatus;
        private PermissionEntry next;

        private PermissionEntry(String accessControlledPath, Tree entryTree, RestrictionProvider restrictionsProvider) {
            isAllow = (PREFIX_ALLOW == entryTree.getName().charAt(0));
            privilegeBits = PrivilegeBits.getInstance(entryTree.getProperty(REP_PRIVILEGE_BITS));
            path = accessControlledPath;
            restriction = restrictionsProvider.getPattern(accessControlledPath, entryTree);
        }

        private boolean matches(@Nonnull Tree tree, @Nullable PropertyState property) {
            String treePath = tree.getPath();
            if (Text.isDescendantOrEqual(path, treePath)) {
                return restriction.matches(tree, property);
            } else {
                return false;
            }
        }

        private boolean matches(@Nonnull String treePath) {
            if (Text.isDescendantOrEqual(path, treePath)) {
                return restriction.matches(treePath);
            } else {
                return false;
            }
        }
    }

    /**
     * Collects permission entries for different principals and asserts they are
     * in the correct order for proper and efficient evaluation.
     */
    private static final class EntriesBuilder {

        private ImmutableSortedMap.Builder<Key, PermissionEntry> repoEntries = ImmutableSortedMap.naturalOrder();
        private ImmutableSortedMap.Builder<Key, PermissionEntry> userEntries = ImmutableSortedMap.naturalOrder();
        private ImmutableSortedMap.Builder<Key, PermissionEntry> groupEntries = ImmutableSortedMap.naturalOrder();

        private void addEntries(@Nonnull Principal principal,
                                @Nonnull Tree principalRoot,
                                @Nonnull RestrictionProvider restrictionProvider) {
            for (Tree entryTree : principalRoot.getChildren()) {
                Key key = new Key(entryTree);
                PermissionEntry entry = new PermissionEntry(key.path, entryTree, restrictionProvider);
                if (!entry.privilegeBits.isEmpty()) {
                    if (key.path == null) {
                        repoEntries.put(key, entry);
                    } else if (principal instanceof Group) {
                        groupEntries.put(key, entry);
                    } else {
                        userEntries.put(key, entry);
                    }
                }
            }
        }

        private Map<Key, PermissionEntry> getRepoEntries() {
            return repoEntries.build();
        }

        private Map<Key, PermissionEntry> getUserEntries() {
            return getEntries(userEntries);
        }

        private Map<Key, PermissionEntry> getGroupEntries() {
            return getEntries(groupEntries);
        }

        private static Map<Key, PermissionEntry> getEntries(ImmutableSortedMap.Builder builder) {
            Map<Key, PermissionEntry> entryMap = builder.build();
            Set<Map.Entry<Key, PermissionEntry>> toProcess = new HashSet<Map.Entry<Key, PermissionEntry>>();
            for (Map.Entry<Key, PermissionEntry> entry : entryMap.entrySet()) {
                Key currentKey = entry.getKey();
                Iterator<Map.Entry<Key,PermissionEntry>> it = toProcess.iterator();
                while (it.hasNext()) {
                    Map.Entry<Key,PermissionEntry> before = it.next();
                    Key beforeKey = before.getKey();
                    if (Text.isDescendantOrEqual(currentKey.path, beforeKey.path)) {
                        before.getValue().next = entry.getValue();
                        it.remove();
                    }
                }
                toProcess.add(entry);
            }
            return entryMap;
        }
    }

    private static class EntryIterator implements Iterator<PermissionEntry> {

        private final Iterator<PermissionEntry> it;

        private PermissionEntry latestEntry;

        private EntryIterator(@Nonnull Map<Key, PermissionEntry> entries,
                              @Nonnull final Tree tree, @Nullable final PropertyState property) {
            it = Iterators.transform(
                    Iterators.filter(entries.entrySet().iterator(), new EntryPredicate(tree, property)),
                    new EntryFunction());
        }

        private EntryIterator(@Nonnull Map<Key, PermissionEntry> entries,
                              @Nonnull final String path) {
            it = Iterators.transform(
                    Iterators.filter(entries.entrySet().iterator(), new EntryPredicate(path)),
                    new EntryFunction());
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public PermissionEntry next() {
            if (latestEntry != null && latestEntry.next != null) {
                // skip entries on the iterator
                while (it.hasNext()) {
                    if (it.next() == latestEntry.next) {
                        break;
                    }
                }
                latestEntry = latestEntry.next;
            } else {
                latestEntry = it.next();
            }
            return latestEntry;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class EntryPredicate implements Predicate<Map.Entry<Key, PermissionEntry>> {

        private final Tree tree;
        private final PropertyState property;
        private final String path;
        private final int depth;

        private EntryPredicate(@Nonnull Tree tree, @Nullable PropertyState property) {
            this.tree = tree;
            this.property = property;
            this.path = tree.getPath();
            this.depth = PathUtils.getDepth(path);
        }

        private EntryPredicate(@Nonnull String path) {
            this.tree = null;
            this.property = null;
            this.path = path;
            this.depth = PathUtils.getDepth(path);
        }

        @Override
        public boolean apply(@Nullable Map.Entry<Key, PermissionEntry> entry) {
            if (entry == null) {
                return false;
            }
            if (depth < entry.getKey().depth) {
                return false;
            } else if (tree != null) {
                return entry.getValue().matches(tree, property);
            } else {
                return entry.getValue().matches(path);
            }
        }
    }

    private static class EntryFunction implements Function<Map.Entry<Key, PermissionEntry>, PermissionEntry> {
        @Override
        public PermissionEntry apply(Map.Entry<Key, PermissionEntry> input) {
            return input.getValue();
        }
    }
}
