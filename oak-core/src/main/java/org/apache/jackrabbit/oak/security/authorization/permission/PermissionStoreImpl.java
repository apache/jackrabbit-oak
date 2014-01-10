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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.util.TreeUtil;

/**
 * {@code PermissionStoreImpl}...
 */
public class PermissionStoreImpl implements PermissionStore {

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
    public void load(@Nonnull Collection<PermissionEntry> entries, @Nonnull String principalName, @Nonnull String path) {
        Tree principalRoot = getPrincipalRoot(principalName);
        if (principalRoot == null) {
            return;
        }
        String name = PermissionUtil.getEntryName(path);
        if (principalRoot.hasChild(name)) {
            Tree child = principalRoot.getChild(name);
            if (PermissionUtil.checkACLPath(child, path)) {
                loadPermissionEntries(path, entries, child, restrictionProvider);
            } else {
                // check for child node
                for (Tree node : child.getChildren()) {
                    if (PermissionUtil.checkACLPath(node, path)) {
                        loadPermissionEntries(path, entries, node, restrictionProvider);
                    }
                }
            }
        }
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
    public boolean hasPermissionEntries(@Nonnull String principalName) {
        return getPrincipalRoot(principalName) != null;
    }

    @Override
    public long getNumEntries(@Nonnull String principalName) {
        Tree tree = getPrincipalRoot(principalName);
        return tree == null ? 0 : PermissionUtil.getNumPermissions(tree);
    }

    @Override
    public long getModCount(@Nonnull String principalName) {
        Tree principalRoot = getPrincipalRoot(principalName);
        if (principalRoot != null) {
            PropertyState ps = principalRoot.getProperty(PermissionConstants.REP_MOD_COUNT);
            if (ps != null) {
                return ps.getValue(Type.LONG);
            }
        }
        return 0;
    }

    @Override
    @Nonnull
    public PrincipalPermissionEntries load(@Nonnull String principalName) {
        PrincipalPermissionEntries ret = new PrincipalPermissionEntries(principalName);
        Tree principalRoot = getPrincipalRoot(principalName);
        if (principalRoot != null) {
            for (Tree entryTree : principalRoot.getChildren()) {
                loadPermissionEntries(entryTree, ret.getEntries(), restrictionProvider);
            }
            PropertyState ps = principalRoot.getProperty(PermissionConstants.REP_MOD_COUNT);
            ret.setModCount(ps == null ? -1 : ps.getValue(Type.LONG));
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
}