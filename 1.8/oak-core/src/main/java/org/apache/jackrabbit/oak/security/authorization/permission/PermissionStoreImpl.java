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
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
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

    private final Map<String, Tree> principalTreeMap = new HashMap<String, Tree>();

    private Tree permissionsTree;
    private PrivilegeBits allBits;

    PermissionStoreImpl(Root root, String permissionRootName, RestrictionProvider restrictionProvider) {
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
        allBits = new PrivilegeBitsProvider(root).getBits(PrivilegeConstants.JCR_ALL);
    }

    //----------------------------------------------------< PermissionStore >---
    @Override
    @Nullable
    public Collection<PermissionEntry> load(@Nullable Collection<PermissionEntry> entries, @NotNull String principalName, @NotNull String path) {
        Tree principalRoot = getPrincipalRoot(principalName);
        if (principalRoot != null) {
            String name = PermissionUtil.getEntryName(path);
            if (principalRoot.hasChild(name)) {
                Tree child = principalRoot.getChild(name);
                if (PermissionUtil.checkACLPath(child, path)) {
                    entries = loadPermissionEntries(path, entries, child);
                } else {
                    // check for child node
                    for (Tree node : child.getChildren()) {
                        if (PermissionUtil.checkACLPath(node, path)) {
                            entries = loadPermissionEntries(path, entries, node);
                        }
                    }
                }
            }
        }
        return entries == null || entries.isEmpty() ? null : entries;
    }

    @Override
    public long getNumEntries(@NotNull String principalName, long max) {
        // we ignore the hash-collisions here
        Tree tree = getPrincipalRoot(principalName);
        return tree == null ? 0 : tree.getChildrenCount(max);
    }

    @Override
    @NotNull
    public PrincipalPermissionEntries load(@NotNull String principalName) {
        long t0 = System.nanoTime();
        PrincipalPermissionEntries ret = new PrincipalPermissionEntries();
        Tree principalRoot = getPrincipalRoot(principalName);
        if (principalRoot != null) {
            for (Tree entryTree : principalRoot.getChildren()) {
                loadPermissionEntries(entryTree, ret.getEntries());
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
                                       @NotNull Map<String, Collection<PermissionEntry>> pathEntryMap) {
        String path = TreeUtil.getString(tree, PermissionConstants.REP_ACCESS_CONTROLLED_PATH);
        if (path != null) {
            Collection<PermissionEntry> entries = pathEntryMap.get(path);
            if (entries == null) {
                entries = new TreeSet<PermissionEntry>();
                pathEntryMap.put(path, entries);
            }
            for (Tree child : tree.getChildren()) {
                if (child.getName().charAt(0) == 'c') {
                    loadPermissionEntries(child, pathEntryMap);
                } else {
                    entries.add(createPermissionEntry(path, child));
                }
            }
        } else {
            log.error("Permission entry at '{}' without rep:accessControlledPath property.", tree.getPath());
        }
    }

    @Nullable
    private Collection<PermissionEntry> loadPermissionEntries(@NotNull String path,
                                                              @Nullable Collection<PermissionEntry> ret,
                                                              @NotNull Tree tree) {
        for (Tree ace : tree.getChildren()) {
            if (ace.getName().charAt(0) != 'c') {
                if (ret == null) {
                    ret = new TreeSet<PermissionEntry>();
                }
                ret.add(createPermissionEntry(path, ace));
            }
        }
        return ret;
    }

    @NotNull
    private PermissionEntry createPermissionEntry(@NotNull String path,
                                                  @NotNull Tree entryTree) {
        PropertyState ps = entryTree.getProperty(REP_PRIVILEGE_BITS);
        PrivilegeBits bits = (isJcrAll(ps)) ? allBits : PrivilegeBits.getInstance(ps);
        boolean isAllow = TreeUtil.getBoolean(entryTree, REP_IS_ALLOW);
        return new PermissionEntry(path,
                isAllow,
                Integer.parseInt(entryTree.getName()),
                bits,
                restrictionProvider.getPattern(path, entryTree));
    }

    private static boolean isJcrAll(@Nullable PropertyState property) {
        return property != null && property.count() == 1 && property.getValue(Type.LONG, 0) == DYNAMIC_ALL_BITS;
    }
}
