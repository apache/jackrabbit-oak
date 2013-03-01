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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * TODO
 */
class CompiledPermissionImpl implements CompiledPermissions, PermissionConstants {

    private final Set<Principal> principals;
    private final RestrictionProvider restrictionProvider;
    private final Map<String, ReadOnlyTree> trees;

    private PrivilegeBitsProvider bitsProvider;
    private Map<Key, PermissionEntry> userEntries;
    private Map<Key, PermissionEntry> groupEntries;

    CompiledPermissionImpl(@Nonnull Set<Principal> principals,
                           @Nonnull ReadOnlyTree permissionsTree,
                           @Nonnull PrivilegeBitsProvider bitsProvider,
                           @Nonnull RestrictionProvider restrictionProvider) {
        checkArgument(!principals.isEmpty());
        this.principals = principals;
        this.restrictionProvider = restrictionProvider;
        this.trees = new HashMap<String, ReadOnlyTree>(principals.size());
        refresh(permissionsTree, bitsProvider);
    }

    void refresh(@Nonnull ReadOnlyTree permissionsTree,
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
            for (Map.Entry<String, ReadOnlyTree> entry : trees.entrySet()) {
                ReadOnlyTree t = entry.getValue();
                ReadOnlyTree t2 = permissionsTree.getChild(t.getName());
                // TODO: OAK-660 or equivalent comparision
                if (!t.equals(t2)) {
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
    public boolean canRead(Tree tree) {
        return isGranted(tree, Permissions.READ_NODE);
    }

    @Override
    public boolean canRead(Tree tree, PropertyState property) {
        return isGranted(tree, property, Permissions.READ_PROPERTY);
    }

    @Override
    public boolean isGranted(long permissions) {
        // TODO: only evaluate entries that are defined for the "" path.
        return false;
    }

    @Override
    public boolean isGranted(Tree tree, long permissions) {
        return hasPermissions(tree, null, permissions);
    }

    @Override
    public boolean isGranted(Tree parent, PropertyState property, long permissions) {
        return hasPermissions(parent, property, permissions);
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

    private void buildEntries(@Nullable ReadOnlyTree permissionsTree) {
        if (permissionsTree == null) {
            userEntries = Collections.emptyMap();
            groupEntries = Collections.emptyMap();
        } else {
            EntriesBuilder builder = new EntriesBuilder();
            for (Principal principal : principals) {
                ReadOnlyTree t = getPrincipalRoot(permissionsTree, principal);
                if (t != null) {
                    trees.put(principal.getName(), t);
                    builder.addEntry(principal, t, restrictionProvider);
                }
            }
            userEntries = builder.userEntries.build();
            groupEntries = builder.groupEntries.build();
        }
    }

    @CheckForNull
    private static ReadOnlyTree getPrincipalRoot(ReadOnlyTree permissionsTree, Principal principal) {
        return permissionsTree.getChild(Text.escapeIllegalJcrChars(principal.getName()));
    }

    private boolean hasPermissions(@Nonnull Tree tree, @Nullable PropertyState property,
                                   long permissions) {
        // TODO
        return false;
    }


    private PrivilegeBits getPrivilegeBits(@Nullable Tree tree) {
        // TODO
        return PrivilegeBits.EMPTY;
    }

    private static final class Key implements Comparable<Key> {

        private String path;
        private long index;

        private Key(Tree tree) {
            path = Strings.emptyToNull(TreeUtil.getString(tree, REP_ACCESS_CONTROLLED_PATH));
            index = tree.getProperty(REP_INDEX).getValue(Type.LONG);
        }

        @Override
        public int compareTo(Key key) {
            // TODO
            return 0;
        }
    }

    private static final class PermissionEntry {

        private final boolean isAllow;
        private final PrivilegeBits privilegeBits;
        private final Set<Restriction> restrictions;

        private PermissionEntry(String accessControlledPath, Tree entryTree, RestrictionProvider restrictionsProvider) {
            isAllow = (PREFIX_ALLOW == entryTree.getName().charAt(0));
            privilegeBits = PrivilegeBits.getInstance(entryTree.getProperty(REP_PRIVILEGE_BITS));
            restrictions = restrictionsProvider.readRestrictions(accessControlledPath, entryTree);
        }
    }

    /**
     * Collects permission entries for different principals and asserts they are
     * in the correct order for proper and efficient evaluation.
     */
    private static final class EntriesBuilder {

        private ImmutableSortedMap.Builder<Key, PermissionEntry> userEntries = ImmutableSortedMap.naturalOrder();
        private ImmutableSortedMap.Builder<Key, PermissionEntry> groupEntries = ImmutableSortedMap.naturalOrder();

        private void addEntry(@Nonnull Principal principal,
                              @Nonnull Tree entryTree,
                              @Nonnull RestrictionProvider restrictionProvider) {
            Key key = new Key(entryTree);
            PermissionEntry entry = new PermissionEntry(key.path, entryTree, restrictionProvider);
            if (!entry.privilegeBits.isEmpty()) {
                if (principal instanceof Group) {
                    groupEntries.put(key, entry);
                } else {
                    userEntries.put(key, entry);
                }
            }
        }
    }
}
