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
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSortedMap;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.Permissions;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * TODO
 */
class CompiledPermissionImpl implements CompiledPermissions, AccessControlConstants {

    private final Set<Principal> principals;
    private final PrivilegeBitsProvider bitsProvider;

    private final Map<Key, Entry> userEntries;
    private final Map<Key, Entry> groupEntries;

    CompiledPermissionImpl(@Nonnull Set<Principal> principals,
                           @Nonnull PrivilegeBitsProvider bitsProvider,
                           @Nonnull ReadOnlyTree permissionsTree) {
        this.principals = checkNotNull(principals);
        this.bitsProvider = bitsProvider;

        EntriesBuilder builder = new EntriesBuilder();
        for (Principal principal : principals) {
            Tree t = permissionsTree.getChild(Text.escapeIllegalJcrChars(principal.getName()));
            if (t != null) {
                builder.addEntry(principal, t);
            }
        }
        userEntries = builder.userEntries.build();
        groupEntries = builder.groupEntries.build();
    }

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
            path = tree.getProperty("rep:accessControlledPath").getValue(Type.STRING);
            index = tree.getProperty("rep:index").getValue(Type.LONG);
        }

        @Override
        public int compareTo(Key key) {
            // TODO
            return 0;
        }
    }

    private static final class Entry {

        private final boolean isAllow;
        private final PrivilegeBits privilegeBits;
        private final List<String> restrictions;

        private Entry(Tree entryTree) {
            isAllow = ('a' == entryTree.getName().charAt(0));
            privilegeBits = PrivilegeBits.getInstance(entryTree.getProperty(REP_PRIVILEGES));
            restrictions = null; // TODO
        }
    }

    private static final class EntriesBuilder {

        private ImmutableSortedMap.Builder<Key, Entry> userEntries = ImmutableSortedMap.naturalOrder();
        private ImmutableSortedMap.Builder<Key, Entry> groupEntries = ImmutableSortedMap.naturalOrder();

        private void addEntry(@Nonnull Principal principal, @Nonnull Tree entryTree) {
            Entry entry = new Entry(entryTree);
            if (entry.privilegeBits.isEmpty()) {
                Key key = new Key(entryTree);
                if (principal instanceof Group) {
                    groupEntries.put(key, entry);
                } else {
                    userEntries.put(key, entry);
                }
            }
        }
    }
}
