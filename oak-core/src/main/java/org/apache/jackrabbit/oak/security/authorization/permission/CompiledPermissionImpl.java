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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Longs;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
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

    private final Map<String, Tree> userTrees;
    private final Map<String, Tree> groupTrees;

    private final Set<String> readPaths;

    private PrivilegeBitsProvider bitsProvider;

    CompiledPermissionImpl(@Nonnull Set<Principal> principals,
                           @Nonnull ImmutableTree permissionsTree,
                           @Nonnull PrivilegeBitsProvider bitsProvider,
                           @Nonnull RestrictionProvider restrictionProvider,
                           @Nonnull Set<String> readPaths) {
        checkArgument(!principals.isEmpty());
        this.principals = principals;
        this.restrictionProvider = restrictionProvider;
        this.bitsProvider = bitsProvider;
        this.readPaths = readPaths;

        userTrees = new HashMap<String, Tree>(principals.size());
        groupTrees = new HashMap<String, Tree>(principals.size());
        if (permissionsTree.exists()) {
            for (Principal principal : principals) {
                Tree t = getPrincipalRoot(permissionsTree, principal);
                if (t.exists()) {
                    Map<String, Tree> target = getTargetMap(principal);
                    target.put(principal.getName(), t);
                }
            }
        }
    }

    //------------------------------------------------< CompiledPermissions >---
    @Override
    public void refresh(@Nonnull ImmutableTree permissionsTree,
                 @Nonnull PrivilegeBitsProvider bitsProvider) {
        this.bitsProvider = bitsProvider;
        // test if a permission has been added for those principals that didn't have one before
        for (Principal principal : principals) {
            Map<String, Tree> target = getTargetMap(principal);
            Tree principalRoot = getPrincipalRoot(permissionsTree, principal);
            String pName = principal.getName();
            if (principalRoot.exists()) {
                if (!target.containsKey(pName) || !principalRoot.equals(target.get(pName))) {
                    target.put(pName, principalRoot);
                }
            } else {
                target.remove(pName);
            }
        }
    }

    @Override
    public ReadStatus getReadStatus(@Nonnull Tree tree, @Nullable PropertyState property) {
        if (isReadablePath(tree, null)) {
            return ReadStatus.ALLOW_ALL_REGULAR;
        }
        long permission = (property == null) ? Permissions.READ_NODE : Permissions.READ_PROPERTY;
        Iterator<PermissionEntry> it = getEntryIterator(new EntryPredicate(tree, property));
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
        return hasPermissions(getEntryIterator(new EntryPredicate()), permissions, null, null);
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
        Iterator<PermissionEntry> it = getEntryIterator(new EntryPredicate(tree, property));
        return hasPermissions(it, permissions, tree, null);
    }

    @Override
    public boolean isGranted(@Nonnull String path, long permissions) {
        Iterator<PermissionEntry> it = getEntryIterator(new EntryPredicate(path));
        return hasPermissions(it, permissions, null, path);
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
    @Nonnull
    private static Tree getPrincipalRoot(Tree permissionsTree, Principal principal) {
        return permissionsTree.getChild(Text.escapeIllegalJcrChars(principal.getName()));
    }

    @Nonnull
    private Map<String, Tree> getTargetMap(Principal principal) {
        return (principal instanceof Group) ? groupTrees : userTrees;
    }

    private boolean hasPermissions(@Nonnull Iterator<PermissionEntry> entries,
                                   long permissions, @Nullable Tree tree, @Nullable String path) {
        // calculate readable paths if the given permissions includes any read permission.
        boolean isReadable = Permissions.diff(Permissions.READ, permissions) != Permissions.READ && isReadablePath(tree, path);
        if (!entries.hasNext() && !isReadable) {
            return false;
        }

        boolean respectParent = (tree != null || path != null) &&
                (Permissions.includes(permissions, Permissions.ADD_NODE) ||
                Permissions.includes(permissions, Permissions.REMOVE_NODE) ||
                Permissions.includes(permissions, Permissions.MODIFY_CHILD_NODE_COLLECTION));

        long allows = (isReadable) ? Permissions.READ : Permissions.NO_PERMISSION;
        long denies = Permissions.NO_PERMISSION;

        PrivilegeBits allowBits = PrivilegeBits.getInstance();
        if (isReadable) {
            allowBits.add(bitsProvider.getBits(PrivilegeConstants.JCR_READ));
        }
        PrivilegeBits denyBits = PrivilegeBits.getInstance();
        PrivilegeBits parentAllowBits;
        PrivilegeBits parentDenyBits;
        String parentPath = null;

        if (respectParent) {
            parentAllowBits = PrivilegeBits.getInstance();
            parentDenyBits = PrivilegeBits.getInstance();
            if (path != null || tree != null) {
                parentPath = PermissionUtil.getParentPathOrNull((path != null) ? path : tree.getPath());
            }
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
    private PrivilegeBits getPrivilegeBits(@Nullable Tree tree) {
        EntryPredicate pred = (tree == null) ? new EntryPredicate() : new EntryPredicate(tree, null);
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
        if (isReadablePath(tree, null)) {
            allowBits.add(bitsProvider.getBits(PrivilegeConstants.JCR_READ));
        }
        return allowBits;
    }

    @Nonnull
    private Iterator<PermissionEntry> getEntryIterator(@Nonnull EntryPredicate predicate) {
        Iterator<PermissionEntry> userEntries = (userTrees.isEmpty()) ?
                Iterators.<PermissionEntry>emptyIterator() :
                new EntryIterator(userTrees, predicate);
        Iterator<PermissionEntry> groupEntries = (groupTrees.isEmpty()) ?
                Iterators.<PermissionEntry>emptyIterator():
                new EntryIterator(groupTrees, predicate);
        return Iterators.concat(userEntries, groupEntries);
    }

    private boolean isReadablePath(@Nullable Tree tree, @Nullable String treePath) {
        if (!readPaths.isEmpty()) {
            String targetPath = (tree != null) ? tree.getPath() : treePath;
            if (targetPath != null) {
                for (String path : readPaths) {
                    if (Text.isDescendantOrEqual(path, targetPath)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static final class PermissionEntry {

        private final boolean isAllow;
        private final PrivilegeBits privilegeBits;
        private final long index;
        private final String path;
        private final RestrictionPattern restriction;

        private ReadStatus readStatus = null; // TODO

        private PermissionEntry(Tree entryTree, RestrictionProvider restrictionsProvider) {
            isAllow = entryTree.getProperty(REP_IS_ALLOW).getValue(Type.BOOLEAN);
            privilegeBits = PrivilegeBits.getInstance(entryTree.getProperty(REP_PRIVILEGE_BITS));
            index = checkNotNull(entryTree.getProperty(REP_INDEX).getValue(Type.LONG)).longValue();
            path = Strings.emptyToNull(TreeUtil.getString(entryTree, REP_ACCESS_CONTROLLED_PATH));
            restriction = restrictionsProvider.getPattern(path, entryTree);
        }

        private boolean matches(@Nonnull Tree tree, @Nullable PropertyState property) {
            return restriction.matches(tree, property);
        }

        private boolean matches(@Nonnull String treePath) {
            return restriction.matches(treePath);
        }

        private boolean matches() {
            return restriction.matches();
        }

        private boolean matchesParent(@Nonnull String parentPath) {
            if (Text.isDescendantOrEqual(path, parentPath)) {
                return restriction.matches(parentPath);
            } else {
                return false;
            }
        }
    }

    private class EntryIterator implements Iterator<PermissionEntry> {

        private final Collection<Tree> principalTrees;
        private final EntryPredicate predicate;

        // the next oak path for which to retrieve permission entries
        private String path;
        // the ordered permission entries at a given path in the hierarchy
        private Iterator<PermissionEntry> nextEntries = Iterators.emptyIterator();
        // the next permission entry
        private PermissionEntry next;

        private EntryIterator(@Nonnull Map<String, Tree> principalTrees,
                              @Nonnull EntryPredicate predicate) {
            this.principalTrees = principalTrees.values();
            this.predicate = predicate;
            this.path = Strings.nullToEmpty(predicate.path);
            next = seekNext();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public PermissionEntry next() {
            if (next == null) {
                throw new NoSuchElementException();
            }

            PermissionEntry pe = next;
            next = seekNext();
            return pe;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @CheckForNull
        private PermissionEntry seekNext() {
            // calculate the ordered entries for the next hierarchy level.
            while (!nextEntries.hasNext() && path != null) {
                nextEntries = getNextEntries();
                path = PermissionUtil.getParentPathOrNull(path);
            }

            if (nextEntries.hasNext()) {
                return nextEntries.next();
            } else {
                return null;
            }
        }

        @Nonnull
        private Iterator<PermissionEntry> getNextEntries() {
            ImmutableSortedSet.Builder<PermissionEntry> entries = new ImmutableSortedSet.Builder(new EntryComparator());
            for (Tree principalRoot : principalTrees) {
                String name = PermissionUtil.getEntryName(path);
                Tree parent = principalRoot;
                while (parent.hasChild(name)) {
                    parent = parent.getChild(name);
                    PermissionEntry pe = new PermissionEntry(parent, restrictionProvider);
                    if (predicate.apply(pe)) {
                        entries.add(pe);
                    }
                }
            }
            return entries.build().iterator();
        }
    }

    private static final class EntryComparator implements Comparator<PermissionEntry> {
        @Override
        public int compare(@Nonnull PermissionEntry entry,
                           @Nonnull PermissionEntry otherEntry) {
            return Longs.compare(otherEntry.index, entry.index);
        }
    }

    private static final class EntryPredicate implements Predicate<PermissionEntry> {

        private final Tree tree;
        private final PropertyState property;
        private final String path;

        private EntryPredicate(@Nonnull Tree tree, @Nullable PropertyState property) {
            this.tree = tree;
            this.property = property;
            this.path = tree.getPath();
        }

        private EntryPredicate(@Nonnull String path) {
            this.tree = null;
            this.property = null;
            this.path = path;
        }

        private EntryPredicate() {
            this.tree = null;
            this.property = null;
            this.path = null;
        }

        @Override
        public boolean apply(@Nullable PermissionEntry entry) {
            if (entry == null) {
                return false;
            }
            if (tree != null) {
                return entry.matches(tree, property);
            } else if (path != null) {
                return entry.matches(path);
            } else {
                return entry.matches();
            }
        }
    }
}
