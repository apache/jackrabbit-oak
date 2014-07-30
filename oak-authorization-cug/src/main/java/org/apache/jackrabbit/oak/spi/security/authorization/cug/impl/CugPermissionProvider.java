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
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.plugins.tree.ImmutableTree;
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

    private ImmutableRoot immutableRoot;

    CugPermissionProvider(@Nonnull Root root,
                          @Nonnull Set<Principal> principals,
                          @Nonnull String[] supportedPaths,
                          @Nonnull ControlFlag flag,
                          @Nonnull Context ctx) {
        this.root = root;
        this.immutableRoot = ImmutableRoot.getInstance(root);
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
        immutableRoot = ImmutableRoot.getInstance(root);
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
        ImmutableTree immutableTree = getImmutableTree(tree);
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
        long diff = Permissions.diff(permissions, Permissions.READ);
        if (diff != Permissions.NO_PERMISSION) {
            return false;
        } else {
            return canRead(tree);
        }
    }

    public boolean isGranted(String oakPath, String jcrActions) {
        TreeLocation location = TreeLocation.create(immutableRoot, oakPath);
        boolean isAcContent = ctx.definesLocation(location);
        long permissions = Permissions.getPermissions(jcrActions, location, isAcContent);

        PropertyState property = location.getProperty();
        Tree tree = (property == null) ? location.getTree() : location.getParent().getTree();
        while (tree == null && !PathUtils.denotesRoot(location.getPath())) {
            location = location.getParent();
            tree = location.getTree();
        }
        if (tree != null) {
            return isGranted(tree, property, permissions);
        } else {
            return false;
        }
    }

    //--------------------------------------------------------------------------
    private static boolean isRead(long permission) {
        return permission == Permissions.READ_NODE || permission == Permissions.READ_PROPERTY || permission == Permissions.READ;
    }

    private static boolean isReadAction(@Nonnull String jcrAction) {
        return READ_NAMES.contains(jcrAction);
    }

    private static boolean hasCug(@Nonnull ImmutableTree tree) {
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
    private ImmutableTree getCugRoot(@Nonnull Tree tree, @Nonnull String path) {
        if (!includes(path)) {
            return null;
        }
        ImmutableTree immutableTree = getImmutableTree(tree);
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
        ImmutableTree immutableTree = getImmutableTree(tree);
        if (ctx.definesTree(immutableTree)) {
            return false;
        }
        ImmutableTree cugRoot = getCugRoot(immutableTree, immutableTree.getPath());
        return cugRoot != null && createCugPermission(cugRoot).canRead();
    }

    private ImmutableTree getImmutableTree(@Nonnull Tree tree) {
        if (tree instanceof ImmutableTree) {
            return (ImmutableTree) tree;
        } else {
            return immutableRoot.getTree(tree.getPath());
        }
    }

    private TreePermission createCugPermission(@Nonnull ImmutableTree tree) {
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

        private ImmutableTree tree;

        private EmptyCugPermission(@Nonnull ImmutableTree tree) {
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

        private final ImmutableTree tree;
        private final boolean allow;

        private CugTreePermission(@Nonnull ImmutableTree tree, boolean allow) {
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