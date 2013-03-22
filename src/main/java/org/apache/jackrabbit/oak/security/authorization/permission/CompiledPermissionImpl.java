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
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
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
                if (t2 != null && !t.getNodeState().equals(t2.getNodeState())) {
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
        // FIXME
        long permission = (property == null) ? Permissions.READ_NODE : Permissions.READ_PROPERTY;
        for (PermissionEntry entry : filterEntries(tree, property)) {
            if (entry.privilegeBits.includesRead(permission)) {
                return ReadStatus.ALLOW_THIS;
            }
        }
        return ReadStatus.DENY_THIS;
    }

    @Override
    public boolean isGranted(long permissions) {
        return hasPermissions(null, permissions, repoEntries.values());
    }

    @Override
    public boolean isGranted(Tree tree, long permissions) {
        return hasPermissions(tree, permissions, filterEntries(tree, null));
    }

    @Override
    public boolean isGranted(Tree parent, PropertyState property, long permissions) {
        return hasPermissions(parent, permissions, filterEntries(parent, property));
    }

    @Override
    public boolean isGranted(String path, long permissions) {
        // TODO
        return false;
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
            repoEntries = builder.repoEntries.build();
            userEntries = builder.userEntries.build();
            groupEntries = builder.groupEntries.build();
        }
    }

    private Iterable<PermissionEntry> filterEntries(final @Nonnull Tree tree,
                                                    final @Nullable PropertyState property) {
        return Iterables.filter(
                Iterables.concat(userEntries.values(), groupEntries.values()),
                new Predicate<PermissionEntry>() {
                    @Override
                    public boolean apply(@Nullable PermissionEntry entry) {
                        return entry != null && entry.matches(tree, property);
                    }
                });
    }

    private boolean hasPermissions(@Nullable Tree tree,
                                   long permissions,
                                   Iterable<PermissionEntry> entries) {
        // TODO
        return false;
    }


    private PrivilegeBits getPrivilegeBits(@Nullable Tree tree) {
        // TODO
        return PrivilegeBits.EMPTY;
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
                } else if (index < key.index) {
                    return -1;
                } else {
                    return 1;
                }
            } else {
                if (depth == key.depth) {
                    return path.compareTo(key.path);
                } else {
                    return (depth < key.depth) ? -1 : 1;
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

        private PermissionEntry(String accessControlledPath, Tree entryTree, RestrictionProvider restrictionsProvider) {
            isAllow = (PREFIX_ALLOW == entryTree.getName().charAt(0));
            privilegeBits = PrivilegeBits.getInstance(entryTree.getProperty(REP_PRIVILEGE_BITS));
            this.path = accessControlledPath;
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
                    } else {
                        if (principal instanceof Group) {
                            groupEntries.put(key, entry);
                        } else {
                            userEntries.put(key, entry);
                        }
                    }
                }
            }
        }
    }
}
