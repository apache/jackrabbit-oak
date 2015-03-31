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
import javax.annotation.Nullable;
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
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.util.TreeUtil;
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

    private static final Set<PrivilegeBits> READ_PRIVILEGE_BITS = ImmutableSet.of(
            PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ),
            PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_NODES),
            PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_PROPERTIES)
    );

    private final Root root;
    private final Set<String> principalNames;

    private final Context ctx;

    private final SupportedPaths supportedPaths;

    private Root immutableRoot;

    CugPermissionProvider(@Nonnull Root root,
                          @Nonnull Set<Principal> principals,
                          @Nonnull Set<String> supportedPaths,
                          @Nonnull Context ctx) {
        this.root = root;

        immutableRoot = RootFactory.createReadOnlyRoot(root);
        principalNames = new HashSet<String>(principals.size());
        for (Principal p : Iterables.filter(principals, Predicates.notNull())) {
            principalNames.add(p.getName());
        }

        this.supportedPaths = new SupportedPaths(supportedPaths);
        this.ctx = ctx;
    }

    //---------------------------------------< AggregatedPermissionProvider >---
    @Override
    public boolean handles(@Nonnull String path, @Nonnull String jcrAction) {
        return isReadAction(jcrAction) && includesCug(immutableRoot.getTree(path), path);
    }

    @Override
    public boolean handles(@Nonnull Tree tree, @Nonnull PrivilegeBits privilegeBits) {
        return READ_PRIVILEGE_BITS.contains(privilegeBits) && includesCug(tree, tree.getPath());
    }

    @Override
    public boolean handles(@Nonnull Tree tree, long permission) {
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

    @Override
    public boolean handlesRepositoryPermissions() {
        return false;
    }

    //-------------------------------------------------< PermissionProvider >---
    @Override
    public void refresh() {
        immutableRoot = RootFactory.createReadOnlyRoot(root);
    }

    @Nonnull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        if (tree != null && canRead(tree)) {
            return READ_PRIVILEGE_NAMES;
        } else {
            return Collections.emptySet();
        }
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
        if (tree == null) {
            return false;
        }
        for (String privilegeName : privilegeNames) {
            if (!READ_PRIVILEGE_NAMES.contains(privilegeName)) {
                return false;
            }
        }
        return canRead(tree);
    }

    @Nonnull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Nonnull
    @Override
    public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
        Tree immutableTree = getImmutableTree(tree);
        if (parentPermission == TreePermission.EMPTY && !immutableTree.isRoot() || isAcContent(immutableTree, true)) {
            return TreePermission.EMPTY;
        }

        TreePermission tp;
        if (parentPermission instanceof CugTreePermission) {
            tp =  new CugTreePermission(immutableTree, ((CugTreePermission) parentPermission).allow);
            if (hasCug(immutableTree)) {
                // a new (nested) cug starts off here
                tp = createCugPermission(immutableTree, tp);
            }
        } else {
            String path = immutableTree.getPath();
            if (supportedPaths.includes(path)) {
                tp =  createCugPermission(immutableTree, null);
            } else if (supportedPaths.mayContainCug(path)) {
                tp =  new EmptyCugPermission(immutableTree);
            } else {
                tp = TreePermission.EMPTY;
            }
        }
        return tp;
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, PropertyState property, long permissions) {
        if (isRead(permissions)) {
            return canRead(tree);
        } else {
            return false;
        }
    }

    @Override
    public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
        TreeLocation location = TreeLocation.create(immutableRoot, oakPath);
        boolean isAcContent = isAcContent(location);
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

    private boolean isAcContent(@Nonnull Tree tree, boolean testForCtxRoot) {
        // FIXME: this should also take other ac-configurations into considerations
        return (testForCtxRoot) ? ctx.definesContextRoot(tree) : ctx.definesTree(tree);
    }

    private boolean isAcContent(@Nonnull TreeLocation location) {
        // FIXME: this should also take other ac-configurations into considerations
        return ctx.definesLocation(location);
    }

    private boolean includesCug(@Nonnull Tree tree, @Nonnull String path) {
        return getCugRoot(tree, path) != null;
    }

    /**
     * Returns the {@code tree} that holds a CUG policy in the ancestry of the
     * given {@code tree} with the specified {@code path} or {@code null} if no
     * such tree exists and thus no CUG is effective at the specified path.
     *
     * @param tree The target tree.
     * @param path The path of the given target tree.
     * @return the {@code tree} holding the CUG policy that effects the specified
     * path or {@code null} if no such policy exists.
     */
    @CheckForNull
    private Tree getCugRoot(@Nonnull Tree tree, @Nonnull String path) {
        if (!supportedPaths.includes(path)) {
            return null;
        }
        Tree immutableTree = getImmutableTree(tree);
        if (hasCug(immutableTree)) {
            return immutableTree;
        }
        String parentPath;
        while (!immutableTree.isRoot()) {
            parentPath = PathUtils.getParentPath(path);
            if (!supportedPaths.includes(parentPath)) {
                break;
            }
            immutableTree = immutableTree.getParent();
            if (hasCug(immutableTree)) {
                return immutableTree;
            }
        }
        return null;
    }

    private boolean canRead(@Nonnull Tree tree) {
        Tree immutableTree = getImmutableTree(tree);
        if (isAcContent(immutableTree, false)) {
            // cug defining access control content is not accessible
            return false;
        }
        Tree cugRoot = getCugRoot(immutableTree, immutableTree.getPath());
        return cugRoot != null && createCugPermission(cugRoot, null).canRead();
    }

    @Nonnull
    private Tree getImmutableTree(@Nonnull Tree tree) {
        return TreeUtil.isReadOnlyTree(tree) ? tree : immutableRoot.getTree(tree.getPath());
    }

    @Nonnull
    private TreePermission createCugPermission(@Nonnull Tree tree, @Nullable TreePermission fallback) {
        Tree cugTree = tree.getChild(REP_CUG_POLICY);
        if (CugUtil.definesCug(cugTree)) {
            PropertyState princNamesState = cugTree.getProperty(REP_PRINCIPAL_NAMES);
            if (princNamesState != null) {
                boolean allow = false;
                for (String pName : princNamesState.getValue(Type.STRINGS)) {
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
        return (fallback == null) ? new EmptyCugPermission(tree) : fallback;
    }

    //--------------------------------------------------------------------------

    /**
     * Same as {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission#EMPTY}
     * from a permission point of view but indicating that
     */
    private final class EmptyCugPermission implements TreePermission {

        private Tree tree;

        private EmptyCugPermission(@Nonnull Tree tree) {
            this.tree = tree;
        }

        @Nonnull
        @Override
        public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
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

    /**
     * {@code TreePermission} implementation for all items located with a CUG.
     */
    private final class CugTreePermission implements TreePermission {

        private final Tree tree;
        private final boolean allow;

        private CugTreePermission(@Nonnull Tree tree, boolean allow) {
            this.tree = tree;
            this.allow = allow;
        }

        @Nonnull
        @Override
        public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
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