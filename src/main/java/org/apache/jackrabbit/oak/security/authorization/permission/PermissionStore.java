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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ReadStatus;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionPattern;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.util.Text;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Longs;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The Permission store reads the principal based access control permissions. One store is currently used to handle
 * 1 set of principal trees (so the compiled permissions use 2 stores, one for the user principals and one for the
 * group principals)
 */
class PermissionStore implements PermissionConstants {

    private final Map<String, Tree> principalTrees;

    private final RestrictionProvider restrictionProvider;

    private final Map<String, Collection<PermissionEntry>> cache = new HashMap<String, Collection<PermissionEntry>>();

    PermissionStore(Map<String, Tree> principalTrees, RestrictionProvider restrictionProvider) {
        this.principalTrees = principalTrees;
        this.restrictionProvider = restrictionProvider;
    }

    @Nonnull
    private Collection<PermissionEntry> getEntries(String path) {
        Collection<PermissionEntry> ret = cache.get(path);
        if (ret == null) {
            ret = new TreeSet<PermissionEntry>();
            String name = PermissionUtil.getEntryName(path);
            for (Map.Entry<String, Tree> principalRoot : principalTrees.entrySet()) {
                Tree child = principalRoot.getValue().getChild(name);
                if (child.exists()) {
                    if (PermissionUtil.checkACLPath(child, path)) {
                        loadPermissionsFromNode(path, ret, child);
                    } else {
                        // check for child node
                        for (Tree node: child.getChildren()) {
                            if (PermissionUtil.checkACLPath(node, path)) {
                                loadPermissionsFromNode(path, ret, node);
                            }
                        }
                    }
                }
            }
            //cache.put(path, ret);
        }
        return ret;
    }

    private void loadPermissionsFromNode(String path, Collection<PermissionEntry> ret, Tree node) {
        for (Tree ace: node.getChildren()) {
            if (ace.getName().charAt(0) != 'c') {
                ret.add(new PermissionEntry(path, ace, restrictionProvider));
            }
        }
    }

    public Iterator<PermissionEntry> getEntryIterator(@Nonnull EntryPredicate predicate) {
        if (principalTrees.isEmpty()) {
            return Iterators.emptyIterator();
        }
        return new EntryIterator(predicate);
    }

    public void flush() {
        cache.clear();
    }

    private class EntryIterator implements Iterator<PermissionEntry> {

        private final EntryPredicate predicate;

        // the next oak path for which to retrieve permission entries
        private String path;

        // the ordered permission entries at a given path in the hierarchy
        private Iterator<PermissionEntry> nextEntries;

        // the next permission entry
        private PermissionEntry next;

        private EntryIterator(@Nonnull EntryPredicate predicate) {
            this.predicate = predicate;
            this.path = Strings.nullToEmpty(predicate.path);
        }

        @Override
        public boolean hasNext() {
            if (next == null) {
                // lazy initialization
                if (nextEntries == null) {
                    nextEntries = Iterators.emptyIterator();
                    seekNext();
                }
            }
            return next != null;
        }

        @Override
        public PermissionEntry next() {
            if (next == null) {
                throw new NoSuchElementException();
            }

            PermissionEntry pe = next;
            seekNext();
            return pe;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @CheckForNull
        private void seekNext() {
            for (next = null; next == null;) {
                if (nextEntries.hasNext()) {
                    PermissionEntry pe = nextEntries.next();
                    if (predicate.apply(pe)) {
                        next = pe;
                    }
                } else {
                    if (path == null) {
                        break;
                    }
                    nextEntries = getEntries(path).iterator();
                    path = PermissionUtil.getParentPathOrNull(path);
                }
            }
        }
    }

    public static final class EntryPredicate implements Predicate<PermissionEntry> {

        private final Tree tree;
        private final PropertyState property;
        private final String path;

        public EntryPredicate(@Nonnull Tree tree, @Nullable PropertyState property) {
            this.tree = tree;
            this.property = property;
            this.path = tree.getPath();
        }

        public EntryPredicate(@Nonnull String path) {
            this.tree = null;
            this.property = null;
            this.path = path;
        }

        public EntryPredicate() {
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


    public static final class PermissionEntry implements Comparable<PermissionEntry> {

        /**
         * flag controls if this is an allow or deny entry
         */
        public final boolean isAllow;

        /**
         * the privilege bits
         */
        public final PrivilegeBits privilegeBits;

        /**
         * the index (order) of the original ACE in the ACL.
         */
        public final int index;

        /**
         * the access controlled (node) path
         */
        public final String path;

        /**
         * the restriction pattern for this entry
         */
        public final RestrictionPattern restriction;

        /**
         * pre-evaluated read status
         */
        public ReadStatus readStatus = null; // TODO

        private PermissionEntry(String path, Tree entryTree, RestrictionProvider restrictionsProvider) {
            this.path = path;
            isAllow = entryTree.getProperty(REP_IS_ALLOW).getValue(Type.BOOLEAN);
            privilegeBits = PrivilegeBits.getInstance(entryTree.getProperty(REP_PRIVILEGE_BITS));
            index = (int) (long) checkNotNull(entryTree.getProperty(REP_INDEX).getValue(Type.LONG));
            restriction = restrictionsProvider.getPattern(path, entryTree);
        }

        public boolean matches(@Nonnull Tree tree, @Nullable PropertyState property) {
            return restriction == RestrictionPattern.EMPTY || restriction.matches(tree, property);
        }

        public boolean matches(@Nonnull String treePath) {
            return restriction == RestrictionPattern.EMPTY || restriction.matches(treePath);
        }

        public boolean matches() {
            return restriction == RestrictionPattern.EMPTY || restriction.matches();
        }

        public boolean matchesParent(@Nonnull String parentPath) {
            return Text.isDescendantOrEqual(path, parentPath) && (restriction == RestrictionPattern.EMPTY || restriction.matches(parentPath));
        }

        @Override
        public int compareTo(PermissionEntry o) {
            final int anotherVal = o.index;
            // reverse order
            return (index<anotherVal ? 1 : (index==anotherVal ? 0 : -1));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PermissionEntry that = (PermissionEntry) o;

            if (index != that.index) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return index;
        }
    }

    private static final class EntryComparator implements Comparator<PermissionEntry> {
        @Override
        public int compare(@Nonnull PermissionEntry entry,
                           @Nonnull PermissionEntry otherEntry) {
            return Longs.compare(otherEntry.index, entry.index);
        }
    }

}
