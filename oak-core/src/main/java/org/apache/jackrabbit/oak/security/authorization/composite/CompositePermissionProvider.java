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
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionUtil;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;

import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.AND;

/**
 * Permission provider implementation that aggregates a list of different
 * provider implementations. Note, that the aggregated provider implementations
 * *must* implement the
 * {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider}
 * interface.
 */
class CompositePermissionProvider implements AggregatedPermissionProvider {

    private final Root root;
    private final AggregatedPermissionProvider[] pps;
    private final Context ctx;
    private final CompositionType compositionType;
    private final RootProvider rootProvider;

    private final RepositoryPermission repositoryPermission;

    private Root immutableRoot;
    private PrivilegeBitsProvider privilegeBitsProvider;
    private TreeTypeProvider typeProvider;

    CompositePermissionProvider(@Nonnull Root root, @Nonnull List<AggregatedPermissionProvider> pps,
                                @Nonnull Context acContext, @Nonnull CompositionType compositionType,
                                @Nonnull RootProvider rootProvider) {
        this.root = root;
        this.pps = pps.toArray(new AggregatedPermissionProvider[pps.size()]);
        this.ctx = acContext;
        this.compositionType = compositionType;
        this.rootProvider = rootProvider;

        repositoryPermission = new CompositeRepositoryPermission(this.pps, this.compositionType);
        immutableRoot = rootProvider.createReadOnlyRoot(root);
        privilegeBitsProvider = new PrivilegeBitsProvider(immutableRoot);
        typeProvider = new TreeTypeProvider(ctx);
    }

    //-------------------------------------------------< PermissionProvider >---
    @Override
    public void refresh() {
        immutableRoot = rootProvider.createReadOnlyRoot(root);
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
                if (compositionType == AND) {
                    // update the set of denied privs by comparing the granted privs
                    // with the complete set of supported privileges
                    denied.add(supported.diff(granted));
                }
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
                if (compositionType == AND) {
                    hasPrivileges = aggregatedPermissionProvider.hasPrivileges(immutableTree,
                            supportedNames.toArray(new String[supportedNames.size()]));
                    if (!hasPrivileges) {
                        return false;
                    }
                    coveredPrivs.add(supported);

                } else {
                    // evaluate one by one so we can aggregate fragments of
                    // supported privileges
                    for (String p : supportedNames) {
                        if (aggregatedPermissionProvider.hasPrivileges(immutableTree, p)) {
                            PrivilegeBits granted = privilegeBitsProvider.getBits(p);
                            coveredPrivs.add(granted);
                            hasPrivileges = true;
                        }
                    }
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
            return CompositeTreePermission.create(immutableTree, typeProvider, pps, compositionType);
        } else if (parentPermission instanceof CompositeTreePermission) {
            return CompositeTreePermission.create(immutableTree, ((CompositeTreePermission) parentPermission));
        } else {
            return parentPermission.getChildPermission(immutableTree.getName(), immutableTree.getNodeState());
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
                if (compositionType == AND) {
                    isGranted = aggregatedPermissionProvider.isGranted(immParent, property, supportedPermissions);
                    if (!isGranted) {
                        return false;
                    }
                    coveredPermissions |= supportedPermissions;
                } else {
                    for (long p : Permissions.aggregates(permissions)) {
                        if (aggregatedPermissionProvider.isGranted(immParent, property, p)) {
                            coveredPermissions |= p;
                            isGranted = true;
                        }
                    }
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
        return isGranted(location, permissions);
    }

    //------------------------------------------------------------< private >---

    private static boolean doEvaluate(long supportedPermissions) {
        return supportedPermissions != Permissions.NO_PERMISSION;
    }

    private static boolean doEvaluate(PrivilegeBits supportedPrivileges) {
        return !supportedPrivileges.isEmpty();
    }

    //-----------------------------------------------< RepositoryPermission >---
    /**
     * {@code RepositoryPermission} implementation that wraps multiple implementations.
     */
    private final static class CompositeRepositoryPermission implements RepositoryPermission {

        private final AggregatedPermissionProvider[] pps;

        private final CompositionType compositionType;

        public CompositeRepositoryPermission(@Nonnull AggregatedPermissionProvider[] pps,
                @Nonnull CompositionType compositionType) {
            this.pps = pps;
            this.compositionType = compositionType;
        }

        @Override
        public boolean isGranted(long repositoryPermissions) {
            boolean isGranted = false;
            long coveredPermissions = Permissions.NO_PERMISSION;

            for (AggregatedPermissionProvider aggregatedPermissionProvider : pps) {
                long supportedPermissions = aggregatedPermissionProvider.supportedPermissions((Tree) null, null, repositoryPermissions);
                if (doEvaluate(supportedPermissions)) {
                    RepositoryPermission rp = aggregatedPermissionProvider.getRepositoryPermission();
                    if (compositionType == AND) {
                        isGranted = rp.isGranted(supportedPermissions);
                        if (!isGranted) {
                            return false;
                        }
                        coveredPermissions |= supportedPermissions;
                    } else {
                        for (long p : Permissions.aggregates(repositoryPermissions)) {
                            if (rp.isGranted(p)) {
                                coveredPermissions |= p;
                                isGranted = true;
                            }
                        }
                    }
                }
            }
            return isGranted && coveredPermissions == repositoryPermissions;
        }
    }

    //---------------------------------------< AggregatedPermissionProvider >---

    @Nonnull
    @Override
    public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
        PrivilegeBits result = PrivilegeBits.getInstance();
        for (AggregatedPermissionProvider aggregatedPermissionProvider : pps) {
            PrivilegeBits supported = aggregatedPermissionProvider.supportedPrivileges(tree, privilegeBits);
            result.add(supported);
        }
        return result;
    }

    @Override
    public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
        return supportedPermissions((aggregatedPermissionProvider) -> aggregatedPermissionProvider
                .supportedPermissions(tree, property, permissions));
    }

    @Override
    public long supportedPermissions(TreeLocation location, long permissions) {
        return supportedPermissions((aggregatedPermissionProvider) -> aggregatedPermissionProvider
                .supportedPermissions(location, permissions));
    }

    @Override
    public long supportedPermissions(TreePermission treePermission, PropertyState property, long permissions) {
        return supportedPermissions((aggregatedPermissionProvider) -> aggregatedPermissionProvider
                .supportedPermissions(treePermission, property, permissions));
    }

    private long supportedPermissions(Function<AggregatedPermissionProvider, Long> supported) {
        long coveredPermissions = Permissions.NO_PERMISSION;
        for (AggregatedPermissionProvider aggregatedPermissionProvider : pps) {
            long supportedPermissions = supported.apply(aggregatedPermissionProvider);
            coveredPermissions |= supportedPermissions;
        }
        return coveredPermissions;
    }

    @Override
    public boolean isGranted(@Nonnull TreeLocation location, long permissions) {
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
                    if (compositionType == AND) {
                        isGranted = aggregatedPermissionProvider.isGranted(location, supportedPermissions);
                        if (!isGranted) {
                            return false;
                        }
                        coveredPermissions |= supportedPermissions;
                    } else {
                        for (long p : Permissions.aggregates(permissions)) {
                            if (aggregatedPermissionProvider.isGranted(location, p)) {
                                coveredPermissions |= p;
                                isGranted = true;
                            }
                        }
                    }
                }
            }
            return isGranted && coveredPermissions == permissions;
        }
    }

    @Nonnull
    @Override
    public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreeType type,
            @Nonnull TreePermission parentPermission) {
        ImmutableTree immutableTree = (ImmutableTree) PermissionUtil.getImmutableTree(tree, immutableRoot);
        if (tree.isRoot()) {
            return CompositeTreePermission.create(immutableTree, typeProvider, pps, compositionType);
        } else if (parentPermission instanceof CompositeTreePermission) {
            return CompositeTreePermission.create(immutableTree, ((CompositeTreePermission) parentPermission), type);
        } else {
            return parentPermission.getChildPermission(immutableTree.getName(), immutableTree.getNodeState());
        }
    }
}
