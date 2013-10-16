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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.util.TreeUtil;

/**
 * The Permission store reads the principal based access control permissions.
 * One store is currently used to handle 1 set of principal trees (so the compiled
 * permissions use 2 stores, one for the user principals and one for the
 * group principals).
 */
class PermissionStore implements PermissionConstants {

    private static final long MAX_SIZE = 250;

    private final Map<String, Tree> principalTrees;
    private final RestrictionProvider restrictionProvider;
    private Map<String, Collection<PermissionEntry>> pathEntryMap;

    private PermissionStore(@Nonnull Map<String, Tree> principalTrees,
                            @Nonnull RestrictionProvider restrictionProvider,
                            boolean doCreateMap) {
        this.principalTrees = principalTrees;
        this.restrictionProvider = restrictionProvider;
        this.pathEntryMap = (doCreateMap) ?
                createMap(principalTrees.values(), restrictionProvider) : null;
    }

    static PermissionStore create(@Nonnull Map<String, Tree> principalTrees,
                                  @Nonnull RestrictionProvider restrictionProvider) {
        long cnt = 0;
        if (!principalTrees.isEmpty()) {
            Iterator<Tree> treeItr = principalTrees.values().iterator();
            while (treeItr.hasNext() && cnt < MAX_SIZE) {
                Tree t = treeItr.next();
                cnt += t.getChildrenCount(MAX_SIZE);
            }
        }
        return new PermissionStore(principalTrees, restrictionProvider, (cnt < MAX_SIZE));
    }

    public void flush() {
        if (pathEntryMap != null) {
            pathEntryMap = createMap(principalTrees.values(), restrictionProvider);
        }
    }

    public Iterator<PermissionEntry> getEntryIterator(@Nonnull EntryPredicate predicate) {
        if (principalTrees.isEmpty()) {
            return Iterators.emptyIterator();
        } else {
            return new EntryIterator(predicate);
        }
    }

    public Collection<PermissionEntry> getEntries(@Nonnull Tree tree) {
        if (principalTrees.isEmpty()) {
            return Collections.emptyList();
        } else if (pathEntryMap != null) {
            Collection<PermissionEntry> entries = pathEntryMap.get(tree.getPath());
            return (entries != null) ? entries : Collections.<PermissionEntry>emptyList();
        } else {
            return (tree.hasChild(AccessControlConstants.REP_POLICY)) ?
                    getEntries(tree.getPath()) :
                    Collections.<PermissionEntry>emptyList();
        }
    }

    @Nonnull
    private Collection<PermissionEntry> getEntries(@Nonnull String path) {
        Collection<PermissionEntry> ret = new TreeSet<PermissionEntry>();
        String name = PermissionUtil.getEntryName(path);
        for (Map.Entry<String, Tree> principalRoot : principalTrees.entrySet()) {
            Tree parent = principalRoot.getValue();
            if (parent.hasChild(name)) {
                Tree child = parent.getChild(name);
                if (PermissionUtil.checkACLPath(child, path)) {
                    loadPermissionEntries(path, ret, child, restrictionProvider);
                } else {
                    // check for child node
                    for (Tree node : child.getChildren()) {
                        if (PermissionUtil.checkACLPath(node, path)) {
                            loadPermissionEntries(path, ret, node, restrictionProvider);
                        }
                    }
                }
            }
        }
        return ret;
    }

    private static Map<String, Collection<PermissionEntry>> createMap(@Nonnull Collection<Tree> principalTrees,
                                                                      @Nonnull RestrictionProvider restrictionProvider) {
        Map<String, Collection<PermissionEntry>> pathEntryMap = new HashMap<String, Collection<PermissionEntry>>();
        for (Tree principalTree : principalTrees) {
            for (Tree entryTree : principalTree.getChildren()) {
                loadPermissionEntries(entryTree, pathEntryMap, restrictionProvider);
            }
        }
        return pathEntryMap;
    }

    private static void loadPermissionEntries(@Nonnull Tree tree,
                                              @Nonnull Map<String, Collection<PermissionEntry>> pathEntryMap,
                                              @Nonnull RestrictionProvider restrictionProvider) {
        String path = TreeUtil.getString(tree, REP_ACCESS_CONTROLLED_PATH);
        Collection<PermissionEntry> entries = pathEntryMap.get(path);
        if (entries == null) {
            entries = new TreeSet<PermissionEntry>();
            pathEntryMap.put(path, entries);
        }
        for (Tree child : tree.getChildren()) {
            if (child.getName().charAt(0) == 'c') {
                loadPermissionEntries(child, pathEntryMap, restrictionProvider);
            } else {
                entries.add(new PermissionEntry(path, child, restrictionProvider));
            }
        }
    }

    private static void loadPermissionEntries(@Nonnull String path,
                                              @Nonnull Collection<PermissionEntry> ret,
                                              @Nonnull Tree tree,
                                              @Nonnull RestrictionProvider restrictionProvider) {
        for (Tree ace : tree.getChildren()) {
            if (ace.getName().charAt(0) != 'c') {
                ret.add(new PermissionEntry(path, ace, restrictionProvider));
            }
        }
    }

    private final class EntryIterator extends AbstractEntryIterator {

        private final EntryPredicate predicate;

        // the next oak path for which to retrieve permission entries
        private String path;

        private EntryIterator(@Nonnull EntryPredicate predicate) {
            this.predicate = predicate;
            this.path = Strings.nullToEmpty(predicate.getPath());
        }

        @CheckForNull
        protected void seekNext() {
            for (next = null; next == null; ) {
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
}
