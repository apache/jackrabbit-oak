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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.RootFactory;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;

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
    private final CompositeRepositoryPermission repositoryPermission;

    private Root immutableRoot;
    private PrivilegeBitsProvider privilegeBitsProvider;

    CompositePermissionProvider(@Nonnull Root root, @Nonnull List<AggregatedPermissionProvider> pps) {
        this.root = root;
        this.pps = pps;

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
    public Set<String> getPrivileges(@Nullable final Tree tree) {
        PrivilegeBits result = null;
        for (AggregatedPermissionProvider pp : filter(tree)) {
            PrivilegeBits privs = privilegeBitsProvider.getBits(pp.getPrivileges(tree));
            if (result == null) {
                result = PrivilegeBits.getInstance();
                result.add(privs);
            } else {
                // FIXME: only retain privs that are handled by prev. pp (and thus are denied)
                result.retain(privs);
            }
        }
        return privilegeBitsProvider.getPrivilegeNames(result);
    }

    @Override
    public boolean hasPrivileges(@Nullable final Tree tree, @Nonnull String... privilegeNames) {
        for (final String privName : privilegeBitsProvider.getAggregatedPrivilegeNames(privilegeNames)) {
            Iterable<AggregatedPermissionProvider> providers = Iterables.filter(pps, new Predicate<AggregatedPermissionProvider>() {
                @Override
                public boolean apply(AggregatedPermissionProvider pp) {
                    // the permissionprovider is never null
                    return (tree == null) ? pp.handlesRepositoryPermissions() : pp.handles(tree, privilegeBitsProvider.getBits(privName));
                }
            });
            for (AggregatedPermissionProvider pp : providers) {
                if (!pp.hasPrivileges(tree, privName)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Nonnull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return repositoryPermission;
    }

    @Nonnull
    @Override
    public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
        ImmutableTree immTree = (tree instanceof ImmutableTree) ? (ImmutableTree) tree : (ImmutableTree) immutableRoot.getTree(tree.getPath());
        if (tree.isRoot()) {
            return new CompositeTreePermission(immTree, new CompositeTreePermission());
        } else {
            if (!(parentPermission instanceof CompositeTreePermission)) {
                throw new IllegalArgumentException("Illegal parent permission instance. Expected CompositeTreePermission.");
            }
            return new CompositeTreePermission(immTree, (CompositeTreePermission) parentPermission);
        }
    }

    @Override
    public boolean isGranted(@Nonnull Tree parent, @Nullable PropertyState property, long permissions) {
        if (Permissions.isAggregate(permissions)) {
            for (final long permission : Permissions.aggregates(permissions)) {
                if (!grantsPermission(parent, property, permission, filter(parent, permission))) {
                    return false;
                }
            }
            return true;
        } else {
            return grantsPermission(parent, property, permissions, filter(parent, permissions));
        }
    }

    @Override
    public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
        final String[] actions = Text.explode(jcrActions, ',', false);
        switch (actions.length) {
            case 0: return true;
            case 1:
                return grantsAction(oakPath, actions[0], filter(oakPath, actions[0]));
            default:
                for (final String action : actions) {
                    if (!grantsAction(oakPath, action, filter(oakPath, action))) {
                        return false;
                    }
                }
                return true;
        }
    }

    //--------------------------------------------------------------------------

    private Iterable<AggregatedPermissionProvider> filter(@Nonnull final Tree tree, final long permissions) {
        return Iterables.filter(pps, new Predicate<AggregatedPermissionProvider>() {
            @Override
            public boolean apply(AggregatedPermissionProvider pp) {
                // the permissionprovider is never null
                return pp.handles(tree, permissions);
            }
        });
    }

    private Iterable<AggregatedPermissionProvider> filter(@Nonnull final String oakPath, final String actions) {
        return Iterables.filter(pps, new Predicate<AggregatedPermissionProvider>() {
            @Override
            public boolean apply(AggregatedPermissionProvider pp) {
                // the permissionprovider is never null
                return pp.handles(oakPath, actions);
            }
        });
    }

    private Iterable<AggregatedPermissionProvider> filter(@Nullable final Tree tree) {
        return Iterables.filter(pps, new Predicate<AggregatedPermissionProvider>() {
            @Override
            public boolean apply(AggregatedPermissionProvider pp) {
                // the permissionprovider is never null
                return (tree != null) || pp.handlesRepositoryPermissions();
            }
        });
    }

    private static boolean grantsPermission(@Nonnull final Tree parent,
                                            @Nullable PropertyState property,
                                            final long permission,
                                            @Nonnull Iterable<AggregatedPermissionProvider> providers) {
        Iterator<AggregatedPermissionProvider> it = providers.iterator();
        boolean isGranted = false;
        while (it.hasNext()) {
            AggregatedPermissionProvider pp = it.next();
            isGranted = pp.isGranted(parent, property, permission);
            if (!isGranted) {
                break;
            }
        }
        return isGranted;
    }

    private static boolean grantsAction(@Nonnull final String oakPath,
                                        @Nonnull final String action,
                                        @Nonnull Iterable<AggregatedPermissionProvider> providers) {
        Iterator<AggregatedPermissionProvider> it = providers.iterator();
        boolean isGranted = false;
        while (it.hasNext()) {
            AggregatedPermissionProvider pp = it.next();
            isGranted = pp.isGranted(oakPath, action);
            if (!isGranted) {
                return false;
            }
        }
        return isGranted;
    }

    private static boolean grantsRepoPermission(long permission, @Nonnull Iterable<AggregatedPermissionProvider> providers) {
        Iterator<AggregatedPermissionProvider> it = providers.iterator();
        boolean isGranted = false;
        while (it.hasNext()) {
            AggregatedPermissionProvider pp = it.next();
            isGranted = pp.getRepositoryPermission().isGranted(permission);
            if (!isGranted) {
                return false;
            }

        }
        return isGranted;
    }

    //--------------------------------------------------------------------------

    private final class CompositeTreePermission implements TreePermission {

        private final ImmutableTree tree;
        private final CompositeTreePermission parentPermission;

        private final Map<AggregatedPermissionProvider, TreePermission> map;

        private Boolean canRead;

        private CompositeTreePermission() {
            tree = null;
            parentPermission = null;

            map = ImmutableMap.of();
        }

        private CompositeTreePermission(@Nonnull final ImmutableTree tree, @Nonnull CompositeTreePermission parentPermission) {
            this.tree = tree;
            this.parentPermission = parentPermission;

            map = new LinkedHashMap<AggregatedPermissionProvider, TreePermission>(pps.size());
            for (AggregatedPermissionProvider provider : pps) {
                TreePermission tp = provider.getTreePermission(tree, getParentPermission(provider));
                map.put(provider, tp);
            }
        }

        @Nonnull
        @Override
        public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
            ImmutableTree childTree = new ImmutableTree(tree, childName, childState);
            return new CompositeTreePermission(childTree, this);
        }

        @Override
        public boolean canRead() {
            if (canRead == null) {
                canRead = false;
                for (Map.Entry<AggregatedPermissionProvider, TreePermission> entry : map.entrySet()) {
                    TreePermission tp = entry.getValue();
                    if (entry.getKey().handles(tp, Permissions.READ_NODE)) {
                        canRead = entry.getValue().canRead();
                        if (!canRead) {
                            break;
                        }
                    }
                }
            }
            return canRead;
        }

        @Override
        public boolean canRead(@Nonnull PropertyState property) {
            boolean canReadProperty = false;
            for (Map.Entry<AggregatedPermissionProvider, TreePermission> entry : map.entrySet()) {
                TreePermission tp = entry.getValue();
                if (entry.getKey().handles(tp, Permissions.READ_PROPERTY)) {
                    canReadProperty = entry.getValue().canRead(property);
                    if (!canReadProperty) {
                        return false;
                    }
                }
            }
            return canReadProperty;
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
            if (Permissions.isAggregate(permissions)) {
                for (long permission : Permissions.aggregates(permissions)) {
                    if (!grantsPermission(permission, null)) {
                        return false;
                    }
                }
                return true;
            } else {
                return grantsPermission(permissions, null);
            }
        }

        @Override
        public boolean isGranted(long permissions, @Nonnull PropertyState property) {
            if (Permissions.isAggregate(permissions)) {
                for (long permission : Permissions.aggregates(permissions)) {
                    if (!grantsPermission(permission, property)) {
                        return false;
                    }
                }
                return true;
            } else {
                return grantsPermission(permissions, property);
            }
        }

        private boolean grantsPermission(long permission, @Nullable PropertyState property) {
            boolean isGranted = false;
            for (Map.Entry<AggregatedPermissionProvider, TreePermission> entry : map.entrySet()) {
                if (entry.getKey().handles(this, permission)) {
                    TreePermission tp = entry.getValue();
                    isGranted = (property == null) ? tp.isGranted(permission) : tp.isGranted(permission, property);
                    if (!isGranted) {
                        return false;
                    }
                }
            }
            return isGranted;
        }

        @Nonnull
        private TreePermission getParentPermission(AggregatedPermissionProvider provider) {
            TreePermission parent = null;
            if (parentPermission != null) {
                parent = parentPermission.map.get(provider);
            }
            return (parent == null) ? TreePermission.EMPTY : parent;
        }
    }

    private final class CompositeRepositoryPermission implements RepositoryPermission {

        @Override
        public boolean isGranted(long repositoryPermissions) {
            if (Permissions.isAggregate(repositoryPermissions)) {
                for (long permission : Permissions.aggregates(repositoryPermissions)) {
                    if (!grantsRepoPermission(permission, filter(null))) {
                        return false;
                    }
                }
                return true;
            } else {
                return grantsRepoPermission(repositoryPermissions, filter(null));
            }
        }
    }
}
