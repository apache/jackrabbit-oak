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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Session;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.ControlFlag;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CugPermissionProvider implements PermissionProvider, AggregatedPermissionProvider, CugConstants {

    private static final Logger log = LoggerFactory.getLogger(CugPermissionProvider.class);

    private static final Set<String> READ_NAMES = ImmutableSet.of(
            Session.ACTION_READ,
            Permissions.PERMISSION_NAMES.get(Permissions.READ),
            Permissions.PERMISSION_NAMES.get(Permissions.READ_NODE),
            Permissions.PERMISSION_NAMES.get(Permissions.READ_PROPERTY));

    private static final Set<String> READ_PRIVILEGE_NAMES = ImmutableSet.of(
            PrivilegeConstants.JCR_READ,
            PrivilegeConstants.REP_READ_NODES,
            PrivilegeConstants.REP_READ_PROPERTIES
    );

    private final Root root;
    private final Set<String> principalNames;

    private final ControlFlag flag;
    private final Context ctx;

    private final String[] supportedPaths;
    private final String[] supportedAltPaths;

    private Root immutableRoot;

    CugPermissionProvider(@Nonnull Root root,
                          @Nonnull Set<Principal> principals,
                          @Nonnull String[] supportedPaths,
                          @Nonnull ControlFlag flag,
                          @Nonnull Context ctx) {
        this.root = root;

        immutableRoot = RootFactory.createReadOnlyRoot(root);
        principalNames = new HashSet<String>(principals.size());
        for (Principal p : Iterables.filter(principals, Predicates.notNull())) {
            principalNames.add(p.getName());
        }

        this.flag = flag;

        this.supportedPaths = supportedPaths;
        supportedAltPaths = new String[supportedPaths.length];
        int i = 0;
        for (String p : supportedPaths) {
            supportedAltPaths[i++] = p + '/';
        }

        this.ctx = ctx;
    }

    //---------------------------------------< AggregatedPermissionProvider >---

    public ControlFlag getFlag() {
        return flag;
    }

    public boolean handles(String path, String jcrAction) {
        return isReadAction(jcrAction) && includesCug(immutableRoot.getTree(path), path);
    }

    public boolean handles(Tree tree) {
        return includesCug(tree, tree.getPath());
    }

    public boolean handles(Tree tree, long permission) {
        return isRead(permission) && includesCug(tree, tree.getPath());
    }

    @Override
    public boolean handles(@Nonnull TreePermission treePermission, long permission) {
        if (isRead(permission)) {
            return treePermission instanceof CugTreePermission;
        } else {
            return false;
        }
    }

    public boolean handlesRepositoryPermissions() {
        return false;
    }

    //-------------------------------------------------< PermissionProvider >---
    public void refresh() {
        immutableRoot = RootFactory.createReadOnlyRoot(root);
    }

    public Set<String> getPrivileges(Tree tree) {
        if (canRead(tree)) {
            return READ_PRIVILEGE_NAMES;
        } else {
            return Collections.emptySet();
        }
    }

    public boolean hasPrivileges(Tree tree, String... privilegeNames) {
        for (String privilegeName : privilegeNames) {
            if (!READ_PRIVILEGE_NAMES.contains(privilegeName)) {
                return false;
            }
        }
        return canRead(tree);
    }

    public RepositoryPermission getRepositoryPermission() {
        throw new UnsupportedOperationException("Not supported");
    }

    public TreePermission getTreePermission(Tree tree, TreePermission parentPermission) {
        Tree immutableTree = getImmutableTree(tree);
        if (ctx.definesContextRoot(immutableTree)) {
            return TreePermission.EMPTY;
        }
        if (parentPermission instanceof CugTreePermission) {
            if (hasCug(immutableTree)) {
                return createCugPermission(immutableTree);
            } else {
                return new CugTreePermission(immutableTree, ((CugTreePermission) parentPermission).allow);
            }
        } else if (parentPermission == TreePermission.EMPTY && !immutableTree.isRoot()) {
            return TreePermission.EMPTY;
        } else {
            String path = immutableTree.getPath();
            if (includes(path)) {
                return createCugPermission(immutableTree);
            } else if (mayContainCug(path)) {
                return new EmptyCugPermission(immutableTree);
            } else {
                return TreePermission.EMPTY;
            }
        }
    }

    public boolean isGranted(Tree tree, PropertyState property, long permissions) {
        if (isRead(permissions)) {
            return canRead(tree);
        } else {
            return false;
        }
    }

    public boolean isGranted(String oakPath, String jcrActions) {
        TreeLocation location = TreeLocation.create(immutableRoot, oakPath);
        boolean isAcContent = ctx.definesLocation(location);
        long permissions = Permissions.getPermissions(jcrActions, location, isAcContent);

        if (isRead(permissions)) {
            PropertyState property = location.getProperty();
            Tree tree = (property == null) ? location.getTree() : location.getParent().getTree();
            while (tree == null && !PathUtils.denotesRoot(location.getPath())) {
                location = location.getParent();
                tree = location.getTree();
            }
            if (tree != null) {
                return isGranted(tree, property, permissions);
            }
        }

        return false;
    }

    //--------------------------------------------------------------------------
    private static boolean isRead(long permission) {
        return permission == Permissions.READ_NODE || permission == Permissions.READ_PROPERTY || permission == Permissions.READ;
    }

    private static boolean isReadAction(@Nonnull String jcrAction) {
        return READ_NAMES.contains(jcrAction);
    }

    private static boolean hasCug(@Nonnull Tree tree) {
        return tree.exists() && tree.hasChild(REP_CUG_POLICY);
    }

    private boolean includes(@Nonnull String path) {
        for (String p : supportedAltPaths) {
            if (p.startsWith(path)) {
                return true;
            }
        }
        for (String p : supportedPaths) {
            if (p.equals(path)) {
                return true;
            }
        }
        return false;
    }

    private boolean includesCug(@Nonnull Tree tree, @Nonnull String path) {
        return getCugRoot(tree, path) != null;
    }

    @CheckForNull
    private Tree getCugRoot(@Nonnull Tree tree, @Nonnull String path) {
        if (!includes(path)) {
            return null;
        }
        Tree immutableTree = getImmutableTree(tree);
        if (hasCug(immutableTree)) {
            return immutableTree;
        }
        while (!immutableTree.isRoot()) {
            immutableTree = immutableTree.getParent();
            if (hasCug(immutableTree)) {
                return immutableTree;
            }
        }
        return null;
    }

    private boolean mayContainCug(@Nonnull String path) {
        String path2 = path + '/';
        for (String sp : supportedPaths) {
            if (path.equals(sp) || sp.startsWith(path2)) {
                return true;
            }
        }
        return false;
    }

    private boolean canRead(@Nonnull Tree tree) {
        Tree immutableTree = getImmutableTree(tree);
        if (ctx.definesTree(immutableTree)) {
            // cug defining access control content is not accessible
            return false;
        }
        Tree cugRoot = getCugRoot(immutableTree, immutableTree.getPath());
        return cugRoot != null && createCugPermission(cugRoot).canRead();
    }

    private Tree getImmutableTree(@Nonnull Tree tree) {
        return immutableRoot.getTree(tree.getPath());
    }

    private TreePermission createCugPermission(@Nonnull Tree tree) {
        Tree cugTree = tree.getChild(REP_CUG_POLICY);
        if (CugUtil.definesCug(cugTree)) {
            PropertyState pNameState = cugTree.getProperty(REP_PRINCIPAL_NAMES);
            if (pNameState != null) {
                boolean allow = false;
                for (String pName : pNameState.getValue(Type.STRINGS)) {
                    if (principalNames.contains(pName)) {
                        allow = true;
                        break;
                    }
                }
                return new CugTreePermission(tree, allow);
            } else {
                log.warn("Tree at {0} doesn't represent a valid CUG.", cugTree.getPath());
            }
        }
        return new EmptyCugPermission(tree);
    }

    //--------------------------------------------------------------------------
    private final class EmptyCugPermission implements TreePermission {

        private Tree tree;

        private EmptyCugPermission(@Nonnull Tree tree) {
            this.tree = tree;
        }

        @Override
        public TreePermission getChildPermission(String childName, NodeState childState) {
            return getTreePermission(tree.getChild(childName), this);
        }

        @Override
        public boolean canRead() {
            return false;
        }

        @Override
        public boolean canRead(@Nonnull PropertyState property) {
            return false;
        }

        @Override
        public boolean canReadAll() {
            return false;
        }

        @Override
        public boolean canReadProperties() {
            return false;
        }

        @Override
        public boolean isGranted(long permissions) {
            return false;
        }

        @Override
        public boolean isGranted(long permissions, @Nonnull PropertyState property) {
            return false;
        }
    }

    private final class CugTreePermission implements TreePermission {

        private final Tree tree;
        private final boolean allow;

        private CugTreePermission(@Nonnull Tree tree, boolean allow) {
            this.tree = tree;
            this.allow = allow;
        }

        @Override
        public TreePermission getChildPermission(String childName, NodeState childState) {
            return getTreePermission(tree.getChild(childName), this);
        }

        @Override
        public boolean canRead() {
            return allow;
        }

        @Override
        public boolean canRead(@Nonnull PropertyState property) {
            return allow;
        }

        @Override
        public boolean canReadAll() {
            return false;
        }

        @Override
        public boolean canReadProperties() {
            return false;
        }

        @Override
        public boolean isGranted(long permissions) {
            return allow && permissions == Permissions.READ_NODE;
        }

        @Override
        public boolean isGranted(long permissions, @Nonnull PropertyState property) {
            return allow && permissions == Permissions.READ_PROPERTY;
        }
    }
}