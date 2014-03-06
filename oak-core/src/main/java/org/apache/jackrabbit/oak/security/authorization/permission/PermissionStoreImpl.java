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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.util.TreeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PermissionStoreImpl}...
 */
public class PermissionStoreImpl implements PermissionStore {

    /**
     * default logger
     */
    private static final Logger log = LoggerFactory.getLogger(PermissionStoreImpl.class);

    private Tree permissionsTree;

    private final String workspaceName;

    private final RestrictionProvider restrictionProvider;

    private final Map<String, Tree> principalTreeMap = new HashMap<String, Tree>();

    public PermissionStoreImpl(Root root, String workspaceName, RestrictionProvider restrictionProvider) {
        this.permissionsTree = PermissionUtil.getPermissionsRoot(root, workspaceName);
        this.workspaceName = workspaceName;
        this.restrictionProvider = restrictionProvider;
    }

    protected void flush(Root root) {
        this.permissionsTree = PermissionUtil.getPermissionsRoot(root, workspaceName);
        this.principalTreeMap.clear();
    }

    @CheckForNull
    private Tree getPrincipalRoot(@Nonnull String principalName) {
        if (principalTreeMap.containsKey(principalName)) {
            return principalTreeMap.get(principalName);
        } else {
            Tree principalRoot = PermissionUtil.getPrincipalRoot(permissionsTree, principalName);
            if (!principalRoot.exists()) {
                principalRoot = null;
            }
            principalTreeMap.put(principalName, principalRoot);
            return principalRoot;
        }
    }

    @Override
    @CheckForNull
    public Collection<PermissionEntry> load(@Nullable Collection<PermissionEntry> entries, @Nonnull String principalName, @Nonnull String path) {
        Tree principalRoot = getPrincipalRoot(principalName);
        if (principalRoot != null) {
            String name = PermissionUtil.getEntryName(path);
            if (principalRoot.hasChild(name)) {
                Tree child = principalRoot.getChild(name);
                if (PermissionUtil.checkACLPath(child, path)) {
                    entries = loadPermissionEntries(path, entries, child, restrictionProvider);
                } else {
                    // check for child node
                    for (Tree node : child.getChildren()) {
                        if (PermissionUtil.checkACLPath(node, path)) {
                            entries = loadPermissionEntries(path, entries, node, restrictionProvider);
                        }
                    }
                }
            }
        }
        return entries == null || entries.isEmpty() ? null : entries;
    }

    @Override
    public void load(@Nonnull Map<String, Collection<PermissionEntry>> entries, @Nonnull String principalName) {
        Tree principalRoot = getPrincipalRoot(principalName);
        if (principalRoot != null) {
            for (Tree entryTree : principalRoot.getChildren()) {
                loadPermissionEntries(entryTree, entries, restrictionProvider);
            }
        }
    }

    @Override
    public long getNumEntries(@Nonnull String principalName, long max) {
        // we ignore the hash-collisions here
        Tree tree = getPrincipalRoot(principalName);
        return tree == null ? 0 : tree.getChildrenCount(max);
    }

    @Override
    @Nonnull
    public PrincipalPermissionEntries load(@Nonnull String principalName) {
        long t0 = System.nanoTime();
        PrincipalPermissionEntries ret = new PrincipalPermissionEntries(principalName);
        Tree principalRoot = getPrincipalRoot(principalName);
        if (principalRoot != null) {
            for (Tree entryTree : principalRoot.getChildren()) {
                loadPermissionEntries(entryTree, ret.getEntries(), restrictionProvider);
            }
        }
        ret.setFullyLoaded(true);
        long t1 = System.nanoTime();
        if (log.isDebugEnabled()) {
            log.debug(String.format("loaded %d entries in %.2fus for %s.%n", ret.getEntries().size(), (t1 - t0) / 1000.0, principalName));
        }
        return ret;
    }

    private static void loadPermissionEntries(@Nonnull Tree tree,
                                              @Nonnull Map<String, Collection<PermissionEntry>> pathEntryMap,
                                              @Nonnull RestrictionProvider restrictionProvider) {
        String path = TreeUtil.getString(tree, PermissionConstants.REP_ACCESS_CONTROLLED_PATH);
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

    @CheckForNull
    private static Collection<PermissionEntry> loadPermissionEntries(@Nonnull String path,
                                              @Nullable Collection<PermissionEntry> ret,
                                              @Nonnull Tree tree,
                                              @Nonnull RestrictionProvider restrictionProvider) {
        for (Tree ace : tree.getChildren()) {
            if (ace.getName().charAt(0) != 'c') {
                if (ret == null) {
                    ret = new TreeSet<PermissionEntry>();
                }
                ret.add(new PermissionEntry(path, ace, restrictionProvider));
            }
        }
        return ret;
    }
}