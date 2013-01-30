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

import com.google.common.collect.ImmutableSortedMap;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.Permissions;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.apache.jackrabbit.util.Text;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * TODO
 */
public class CompiledPermissionImpl implements CompiledPermissions, AccessControlConstants {

    private final ReadOnlyTree permissionsTree;
    private final Set<Principal> principals;

    private Map<Key, Entry> userEntries;
    private Map<Key, Entry> groupEntries;

    public CompiledPermissionImpl(@Nonnull ReadOnlyTree permissionsTree, @Nonnull Set<Principal> principals) {
        this.permissionsTree = permissionsTree;
        this.principals = checkNotNull(principals);

        EntriesBuilder builder = new EntriesBuilder();
        for (Principal principal : principals) {
            Tree t = permissionsTree.getChild(Text.escapeIllegalJcrChars(principal.getName()));
            builder.addEntry(principal, t);
        }
        userEntries = builder.userEntries.build();
        groupEntries = builder.groupEntries.build();
    }

    @Override
    public boolean canRead(Tree tree) {
        return isGranted(Permissions.READ_NODE, tree);
    }

    @Override
    public boolean canRead(Tree tree, PropertyState property) {
        return isGranted(Permissions.READ_PROPERTY, tree, property);
    }

    @Override
    public boolean isGranted(long permissions) {
        // TODO
        return false;
    }

    @Override
    public boolean isGranted(long permissions, Tree tree) {
        // TODO
        return false;
    }

    @Override
    public boolean isGranted(long permissions, Tree parent, PropertyState property) {
        // TODO
        return false;
    }

    //------------------------------------------------------------< private >---

    private static final class Key implements Comparable<Key> {

        private String path;
        private long order;

        private Key(NodeUtil node) {
            path = node.getString("path", "");
            order = node.getLong("order", -1);
        }

        @Override
        public int compareTo(Key key) {
            // TODO
            return 0;
        }
    }

    private static final class Entry {

        private final boolean isAllow;
        private final String[] privilegeNames;
        private final List<String> restrictions;
        private final long permissions;

        Entry(NodeUtil node) {
            isAllow = node.hasPrimaryNodeTypeName(NT_REP_GRANT_ACE);
            privilegeNames = node.getStrings(REP_PRIVILEGES);
            restrictions = null; // TODO

            permissions = node.getLong("permissions", Permissions.NO_PERMISSION);
        }
    }

    private static final class EntriesBuilder {

        ImmutableSortedMap.Builder<Key, Entry> userEntries = ImmutableSortedMap.naturalOrder();
        ImmutableSortedMap.Builder<Key, Entry> groupEntries = ImmutableSortedMap.naturalOrder();

        private void addEntry(Principal principal, Tree entryTree) {
            NodeUtil node = new NodeUtil(entryTree);
            Entry entry = new Entry(node);
            if (entry.permissions != Permissions.NO_PERMISSION) {
                Key key = new Key(node);
                if (principal instanceof Group) {
                    groupEntries.put(key, entry);
                } else {
                    userEntries.put(key, entry);
                }
            }
        }
    }
}
