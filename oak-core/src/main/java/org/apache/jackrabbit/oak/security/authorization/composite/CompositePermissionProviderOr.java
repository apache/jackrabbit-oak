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
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType;
import org.apache.jackrabbit.oak.security.authorization.permission.PermissionUtil;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Set;

import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.OR;

/**
 * Permission provider implementation that aggregates a list of different
 * provider implementations. Note, that the aggregated provider implementations
 * *must* implement the
 * {@link AggregatedPermissionProvider}
 * interface.
 */
final class CompositePermissionProviderOr extends CompositePermissionProvider {

    CompositePermissionProviderOr(@NotNull Root root, @NotNull List<AggregatedPermissionProvider> pps,
                                  @NotNull Context acContext, @NotNull RootProvider rootProvider,
                                  @NotNull TreeProvider treeProvider) {
        super(root, pps, acContext, rootProvider, treeProvider);
    }

    @NotNull
    @Override
    CompositionType getCompositeType() {
        return OR;
    }

    @NotNull
    @Override
    RepositoryPermission createRepositoryPermission() {
        return new CompositeRepositoryPermission();
    }

    @Override
    boolean hasPrivileges(@Nullable Tree immutableTree, @NotNull PrivilegeBitsProvider bitsProvider, @NotNull PrivilegeBits privilegeBits, @NotNull PrivilegeBits coveredPrivileges) {
        boolean hasPrivileges = false;
        for (AggregatedPermissionProvider aggregatedPermissionProvider : getPermissionProviders()) {
            PrivilegeBits supported = aggregatedPermissionProvider.supportedPrivileges(immutableTree, privilegeBits);
            if (Util.doEvaluate(supported)) {
                Set<String> supportedNames = bitsProvider.getPrivilegeNames(supported);
                // evaluate one by one so we can aggregate fragments of
                // supported privileges
                for (String p : supportedNames) {
                    if (aggregatedPermissionProvider.hasPrivileges(immutableTree, p)) {
                        PrivilegeBits granted = bitsProvider.getBits(p);
                        coveredPrivileges.add(granted);
                        hasPrivileges = true;
                    }
                }
            }
        }
        return hasPrivileges;
    }

    //-------------------------------------------------< PermissionProvider >---

    @NotNull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        Tree immutableTree = PermissionUtil.getReadOnlyTreeOrNull(tree, getImmutableRoot());

        PrivilegeBits result = PrivilegeBits.getInstance();
        PrivilegeBitsProvider bitsProvider = getBitsProvider();
        for (AggregatedPermissionProvider aggregatedPermissionProvider : getPermissionProviders()) {
            PrivilegeBits supported = aggregatedPermissionProvider.supportedPrivileges(immutableTree, null).modifiable();
            if (Util.doEvaluate(supported)) {
                PrivilegeBits granted = bitsProvider.getBits(aggregatedPermissionProvider.getPrivileges(immutableTree));
                // add the granted privileges to the result
                if (!granted.isEmpty()) {
                    result.add(granted);
                }
            }
        }
        return getBitsProvider().getPrivilegeNames(result);
    }

    @Override
    public boolean isGranted(@NotNull Tree parent, @Nullable PropertyState property, long permissions) {
        Tree immParent = PermissionUtil.getReadOnlyTree(parent, getImmutableRoot());

        boolean isGranted = false;
        long coveredPermissions = Permissions.NO_PERMISSION;
        for (AggregatedPermissionProvider aggregatedPermissionProvider : getPermissionProviders()) {
            long supportedPermissions = aggregatedPermissionProvider.supportedPermissions(immParent, property, permissions);
            if (Util.doEvaluate(supportedPermissions)) {
                for (long p : Permissions.aggregates(supportedPermissions)) {
                    if (aggregatedPermissionProvider.isGranted(immParent, property, p)) {
                        coveredPermissions |= p;
                        isGranted = true;
                    }
                }
            }
        }
        return isGranted && coveredPermissions == permissions;
    }

    //---------------------------------------< AggregatedPermissionProvider >---

    @Override
    public boolean isGranted(@NotNull TreeLocation location, long permissions) {
        PropertyState property = location.getProperty();
        Tree tree = (property == null) ? location.getTree() : location.getParent().getTree();

        if (tree != null) {
            return isGranted(tree, property, permissions);
        } else {
            boolean isGranted = false;
            long coveredPermissions = Permissions.NO_PERMISSION;

            for (AggregatedPermissionProvider aggregatedPermissionProvider : getPermissionProviders()) {
                long supportedPermissions = aggregatedPermissionProvider.supportedPermissions(location, permissions);
                if (Util.doEvaluate(supportedPermissions)) {
                    for (long p : Permissions.aggregates(supportedPermissions)) {
                        if (aggregatedPermissionProvider.isGranted(location, p)) {
                            coveredPermissions |= p;
                            isGranted = true;
                        }
                    }
                }
            }
            return isGranted && coveredPermissions == permissions;
        }
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

            for (AggregatedPermissionProvider aggregatedPermissionProvider : getPermissionProviders()) {
                long supportedPermissions = aggregatedPermissionProvider.supportedPermissions((Tree) null, null, repositoryPermissions);
                if (Util.doEvaluate(supportedPermissions)) {
                    RepositoryPermission rp = aggregatedPermissionProvider.getRepositoryPermission();
                    for (long p : Permissions.aggregates(supportedPermissions)) {
                        if (rp.isGranted(p)) {
                            coveredPermissions |= p;
                            isGranted = true;
                        }
                    }
                }
            }
            return isGranted && coveredPermissions == repositoryPermissions;
        }
    }
}
