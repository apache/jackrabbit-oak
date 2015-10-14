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
package org.apache.jackrabbit.oak.security.authorization.composite;

import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionUtil;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;

/**
 * Permission provider implementation that aggregates a list of different
 * provider implementations. Note, that the implementations *must* implement
 * the {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider}
 * interface.
 *
 * TODO This is work in progress (OAK-1268)
 */
class CompositePermissionProvider implements PermissionProvider {

    private final Root root;
    private final List<AggregatedPermissionProvider> pps;
    private final Context ctx;

    private final RepositoryPermission repositoryPermission;

    private Root immutableRoot;
    private PrivilegeBitsProvider privilegeBitsProvider;

    CompositePermissionProvider(@Nonnull Root root, @Nonnull List<AggregatedPermissionProvider> pps, @Nonnull Context acContext) {
        this.root = root;
        this.pps = pps;
        this.ctx = acContext;

        repositoryPermission = new CompositeRepositoryPermission();
        immutableRoot = RootFactory.createReadOnlyRoot(root);
        privilegeBitsProvider = new PrivilegeBitsProvider(immutableRoot);
    }

    //-------------------------------------------------< PermissionProvider >---
    @Override
    public void refresh() {
        immutableRoot = RootFactory.createReadOnlyRoot(root);
        privilegeBitsProvider = new PrivilegeBitsProvider(immutableRoot);

        for (PermissionProvider pp : pps) {
            pp.refresh();
        }
    }

    @Nonnull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        Tree immutableTree = PermissionUtil.getImmutableTree(tree, immutableRoot);

        PrivilegeBits result = PrivilegeBits.getInstance();
        PrivilegeBits denied = PrivilegeBits.getInstance();

        for (AggregatedPermissionProvider aggregatedPermissionProvider : pps) {
            PrivilegeBits supported = aggregatedPermissionProvider.supportedPrivileges(immutableTree, null).modifiable();
            if (doEvaluate(supported)) {
                PrivilegeBits granted = privilegeBitsProvider.getBits(aggregatedPermissionProvider.getPrivileges(immutableTree));
                // add the granted privileges to the result
                if (!granted.isEmpty()) {
                    result.add(granted);
                }
                // update the set of denied privs by comparing the granted privs
                // with the complete set of supported privileges
                denied.add(supported.diff(granted));
            }
        }
        // subtract all denied privileges from the result
        if (!denied.isEmpty()) {
            result.diff(denied);
        }
        return privilegeBitsProvider.getPrivilegeNames(result);
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
        Tree immutableTree = PermissionUtil.getImmutableTree(tree, immutableRoot);
        PrivilegeBits privilegeBits = privilegeBitsProvider.getBits(privilegeNames);
        if (privilegeBits.isEmpty()) {
            return true;
        }

        boolean hasPrivileges = false;
        PrivilegeBits coveredPrivs = PrivilegeBits.getInstance();

        for (AggregatedPermissionProvider aggregatedPermissionProvider : pps) {
            PrivilegeBits supported = aggregatedPermissionProvider.supportedPrivileges(immutableTree, privilegeBits);
            if (doEvaluate(supported)) {
                Set<String> supportedNames = privilegeBitsProvider.getPrivilegeNames(supported);
                hasPrivileges = aggregatedPermissionProvider.hasPrivileges(immutableTree, supportedNames.toArray(new String[supportedNames.size()]));
                coveredPrivs.add(supported);

                if (!hasPrivileges) {
                    break;
                }
            }
        }
        return hasPrivileges && coveredPrivs.includes(privilegeBits);
    }

    @Nonnull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return repositoryPermission;
    }

    @Nonnull
    @Override
    public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
        ImmutableTree immutableTree = (ImmutableTree) PermissionUtil.getImmutableTree(tree, immutableRoot);
        if (tree.isRoot()) {
            return new CompositeTreePermission(immutableTree, new CompositeTreePermission(pps));
        } else {
            if (!(parentPermission instanceof CompositeTreePermission)) {
                throw new IllegalArgumentException("Illegal parent permission instance. Expected CompositeTreePermission.");
            }
            return new CompositeTreePermission(immutableTree, (CompositeTreePermission) parentPermission);
        }
    }

    @Override
    public boolean isGranted(@Nonnull Tree parent, @Nullable PropertyState property, long permissions) {
        Tree immParent = PermissionUtil.getImmutableTree(parent, immutableRoot);

        boolean isGranted = false;
        long coveredPermissions = Permissions.NO_PERMISSION;

        for (AggregatedPermissionProvider aggregatedPermissionProvider : pps) {
            long supportedPermissions = aggregatedPermissionProvider.supportedPermissions(immParent, property, permissions);
            if (doEvaluate(supportedPermissions)) {
                isGranted = aggregatedPermissionProvider.isGranted(immParent, property, supportedPermissions);
                coveredPermissions |= supportedPermissions;

                if (!isGranted) {
                    break;
                }
            }
        }
        return isGranted && coveredPermissions == permissions;
    }

    @Override
    public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
        TreeLocation location = TreeLocation.create(immutableRoot, oakPath);
        boolean isAcContent = ctx.definesLocation(location);

        long permissions = Permissions.getPermissions(jcrActions, location, isAcContent);

        PropertyState property = location.getProperty();
        Tree tree = (property == null) ? location.getTree() : location.getParent().getTree();

        if (tree != null) {
            return isGranted(tree, property, permissions);
        } else {
            boolean isGranted = false;
            long coveredPermissions = Permissions.NO_PERMISSION;

            for (AggregatedPermissionProvider aggregatedPermissionProvider : pps) {
                long supportedPermissions = aggregatedPermissionProvider.supportedPermissions(location, permissions);
                if (doEvaluate(supportedPermissions)) {
                    isGranted = aggregatedPermissionProvider.isGranted(location, supportedPermissions);
                    coveredPermissions |= supportedPermissions;

                    if (!isGranted) {
                        break;
                    }
                }
            }
            return isGranted && coveredPermissions == permissions;
        }
    }

    //------------------------------------------------------------< private >---

    static boolean doEvaluate(long supportedPermissions) {
        return supportedPermissions != Permissions.NO_PERMISSION;
    }

    private static boolean doEvaluate(PrivilegeBits supportedPrivileges) {
        return !supportedPrivileges.isEmpty();
    }

    //-----------------------------------------------< RepositoryPermission >---
    /**
     * {@code RepositoryPermission} implementation that wraps multiple implementations.
     */
    private final class CompositeRepositoryPermission implements RepositoryPermission {

        @Override
        public boolean isGranted(long repositoryPermissions) {
            boolean isGranted = false;
            long coveredPermissions = Permissions.NO_PERMISSION;

            for (AggregatedPermissionProvider aggregatedPermissionProvider : pps) {
                long supportedPermissions = aggregatedPermissionProvider.supportedPermissions((Tree) null, null, repositoryPermissions);
                if (doEvaluate(supportedPermissions)) {
                    isGranted = aggregatedPermissionProvider.getRepositoryPermission().isGranted(supportedPermissions);
                    coveredPermissions |= supportedPermissions;
                    if (!isGranted) {
                        break;
                    }
                }
            }
            return isGranted && coveredPermissions == repositoryPermissions;
        }
    }
}
