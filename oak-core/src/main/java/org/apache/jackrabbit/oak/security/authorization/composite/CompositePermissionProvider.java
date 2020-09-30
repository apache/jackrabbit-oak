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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeProvider;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.function.Function;

/**
 * Permission provider implementation that aggregates a list of different
 * provider implementations. Note, that the aggregated provider implementations
 * *must* implement the
 * {@link org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider}
 * interface.
 */
abstract class CompositePermissionProvider implements AggregatedPermissionProvider {

    private final Root root;
    private final AggregatedPermissionProvider[] pps;
    private final Context ctx;
    private final RootProvider rootProvider;
    private final TreeProvider treeProvider;

    private final RepositoryPermission repositoryPermission;

    private Root immutableRoot;
    private PrivilegeBitsProvider privilegeBitsProvider;
    private TreeTypeProvider typeProvider;

    CompositePermissionProvider(@NotNull Root root, @NotNull List<AggregatedPermissionProvider> pps,
                                @NotNull Context acContext, @NotNull RootProvider rootProvider, @NotNull TreeProvider treeProvider) {
        this.root = root;
        this.pps = pps.toArray(new AggregatedPermissionProvider[0]);
        this.ctx = acContext;
        this.rootProvider = rootProvider;
        this.treeProvider = treeProvider;

        repositoryPermission = createRepositoryPermission();
        immutableRoot = rootProvider.createReadOnlyRoot(root);
        privilegeBitsProvider = new PrivilegeBitsProvider(immutableRoot);
        typeProvider = new TreeTypeProvider(ctx);
    }

    static CompositePermissionProvider create(@NotNull Root root, @NotNull List<AggregatedPermissionProvider> pps,
                                              @NotNull Context acContext, @NotNull CompositionType compositionType,
                                              @NotNull RootProvider rootProvider, @NotNull TreeProvider treeProvider) {
        if (compositionType == CompositionType.AND) {
            return new CompositePermissionProviderAnd(root, pps, acContext, rootProvider, treeProvider);
        } else {
            return new CompositePermissionProviderOr(root, pps, acContext, rootProvider, treeProvider);
        }
    }

    @NotNull
    abstract CompositionType getCompositeType();

    @NotNull
    abstract RepositoryPermission createRepositoryPermission();

    @NotNull
    Root getImmutableRoot() {
        return immutableRoot;
    }

    @NotNull
    PrivilegeBitsProvider getBitsProvider() {
        return privilegeBitsProvider;
    }

    @NotNull
    AggregatedPermissionProvider[] getPermissionProviders() {
        return pps;
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

    @NotNull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return repositoryPermission;
    }

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreePermission parentPermission) {
        Tree readOnlyTree = PermissionUtil.getReadOnlyTree(tree, immutableRoot);
        if (tree.isRoot()) {
            return CompositeTreePermission.create(readOnlyTree, treeProvider, typeProvider, pps, getCompositeType());
        } else if (parentPermission instanceof CompositeTreePermission) {
            return CompositeTreePermission.create(readOnlyTree, treeProvider, ((CompositeTreePermission) parentPermission));
        } else {
            return parentPermission.getChildPermission(readOnlyTree.getName(), treeProvider.asNodeState(readOnlyTree));
        }
    }

    @Override
    public boolean isGranted(@NotNull String oakPath, @NotNull String jcrActions) {
        TreeLocation location = TreeLocation.create(immutableRoot, oakPath);
        boolean isAcContent = ctx.definesLocation(location);

        long permissions = Permissions.getPermissions(jcrActions, location, isAcContent);
        return isGranted(location, permissions);
    }

    //---------------------------------------< AggregatedPermissionProvider >---

    @NotNull
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
    public long supportedPermissions(@NotNull TreeLocation location, long permissions) {
        return supportedPermissions((aggregatedPermissionProvider) -> aggregatedPermissionProvider
                .supportedPermissions(location, permissions));
    }

    @Override
    public long supportedPermissions(@NotNull TreePermission treePermission, @Nullable PropertyState property, long permissions) {
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

    @NotNull
    @Override
    public TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreeType type,
            @NotNull TreePermission parentPermission) {
        Tree immutableTree = PermissionUtil.getReadOnlyTree(tree, immutableRoot);
        if (tree.isRoot()) {
            return CompositeTreePermission.create(immutableTree, treeProvider, typeProvider, pps, getCompositeType());
        } else if (parentPermission instanceof CompositeTreePermission) {
            return CompositeTreePermission.create(immutableTree, treeProvider, ((CompositeTreePermission) parentPermission), type);
        } else {
            return parentPermission.getChildPermission(immutableTree.getName(), treeProvider.asNodeState(immutableTree));
        }
    }
}
