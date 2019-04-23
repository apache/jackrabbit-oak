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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.JcrAllUtil;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PermissionStoreImpl}...
 */
class PermissionStoreImpl implements PermissionStore, PermissionConstants {

    /**
     * default logger
     */
    private static final Logger log = LoggerFactory.getLogger(PermissionStoreImpl.class);

    private final String permissionRootName;

    private final RestrictionProvider restrictionProvider;

    private final Map<String, Tree> principalTreeMap = new HashMap<>();

    private Tree permissionsTree;
    private PrivilegeBitsProvider bitsProvider;

    PermissionStoreImpl(@NotNull Root root, @NotNull String permissionRootName, @NotNull RestrictionProvider restrictionProvider) {
        this.permissionRootName = permissionRootName;
        this.restrictionProvider = restrictionProvider;
        reset(root);
    }

    @Override
    public void flush(@NotNull Root root) {
        principalTreeMap.clear();
        reset(root);
    }

    private void reset(@NotNull Root root) {
        permissionsTree = PermissionUtil.getPermissionsRoot(root, permissionRootName);
        bitsProvider = new PrivilegeBitsProvider(root);
    }

    //----------------------------------------------------< PermissionStore >---
    @Nullable
    @Override
    public Collection<PermissionEntry> load(@NotNull String principalName, @NotNull String path) {
        Tree principalRoot = getPrincipalRoot(principalName);
        Collection<PermissionEntry> entries = null;
        if (principalRoot != null) {
            String name = PermissionUtil.getEntryName(path);
            if (principalRoot.hasChild(name)) {
                Tree child = principalRoot.getChild(name);
                if (PermissionUtil.checkACLPath(child, path)) {
                    entries = loadPermissionEntries(path, child);
                } else {
                    // check for child node : there may at most be one child for
                    // the given path.
                    for (Tree node : child.getChildren()) {
                        if (PermissionUtil.checkACLPath(node, path)) {
                            entries = loadPermissionEntries(path, node);
                            break;
                        }
                    }
                }
            }
        }
        return entries;
    }

    @NotNull
    @Override
    public NumEntries getNumEntries(@NotNull String principalName, long max) {
        Tree tree = getPrincipalRoot(principalName);
        if (tree == null) {
            return NumEntries.ZERO;
        } else {
            // if rep:numPermissions is present it contains the exact number of
            // access controlled nodes for the given principal name.
            // if this property is missing (backward compat) we use the old
            // mechanism and use child-cnt with a max value to get a rough idea
            // about the magnitude (note: this approximation ignores the hash-collisions)
            long l = TreeUtil.getLong(tree, REP_NUM_PERMISSIONS, -1);
            return (l >= 0) ? NumEntries.valueOf(l, true) : NumEntries.valueOf(tree.getChildrenCount(max), false);
        }
    }

    @Override
    @NotNull
    public PrincipalPermissionEntries load(@NotNull String principalName) {
        long t0 = System.nanoTime();
        PrincipalPermissionEntries ret = new PrincipalPermissionEntries();
        Tree principalRoot = getPrincipalRoot(principalName);
        if (principalRoot != null) {
            for (Tree entryTree : principalRoot.getChildren()) {
                loadPermissionEntries(entryTree, ret);
            }
        }
        ret.setFullyLoaded(true);
        long t1 = System.nanoTime();
        if (log.isDebugEnabled()) {
            log.debug(String.format("loaded %d entries in %.2fus for %s.%n", ret.getSize(), (t1 - t0) / 1000.0, principalName));
        }
        return ret;
    }

    //------------------------------------------------------------< private >---
    @Nullable
    private Tree getPrincipalRoot(@NotNull String principalName) {
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

    private void loadPermissionEntries(@NotNull Tree tree,
                                       @NotNull PrincipalPermissionEntries principalPermissionEntries) {
        String path = TreeUtil.getString(tree, PermissionConstants.REP_ACCESS_CONTROLLED_PATH);
        if (path != null) {
            Collection<PermissionEntry> entries = principalPermissionEntries.getEntriesByPath(path);
            if (entries == null) {
                entries = new TreeSet<>();
                principalPermissionEntries.putEntriesByPath(path, entries);
            }
            for (Tree child : tree.getChildren()) {
                if (child.getName().charAt(0) == 'c') {
                    loadPermissionEntries(child, principalPermissionEntries);
                } else {
                    entries.add(createPermissionEntry(path, child));
                }
            }
        } else {
            log.error("Permission entry at '{}' without rep:accessControlledPath property.", tree.getPath());
        }
    }

    @NotNull
    private Collection<PermissionEntry> loadPermissionEntries(@NotNull String path,
                                                              @NotNull Tree tree) {
        Collection<PermissionEntry> ret = new TreeSet<>();
        for (Tree ace : tree.getChildren()) {
            if (ace.getName().charAt(0) != 'c') {
                ret.add(createPermissionEntry(path, ace));
            }
        }
        return ret;
    }

    @NotNull
    private PermissionEntry createPermissionEntry(@NotNull String path,
                                                  @NotNull Tree entryTree) {
        PropertyState ps = entryTree.getProperty(REP_PRIVILEGE_BITS);
        PrivilegeBits bits = JcrAllUtil.getPrivilegeBits(ps, bitsProvider);
        boolean isAllow = TreeUtil.getBoolean(entryTree, REP_IS_ALLOW);
        return new PermissionEntry(path,
                isAllow,
                Integer.parseInt(entryTree.getName()),
                bits,
                restrictionProvider.getPattern(path, entryTree));
    }
}
