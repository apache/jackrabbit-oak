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
    private PrivilegeBitsProvider pbp;

    CompositePermissionProvider(@Nonnull Root root, @Nonnull List<AggregatedPermissionProvider> pps) {
        this.root = root;
        this.pps = pps;

        repositoryPermission = new CompositeRepositoryPermission();
        immutableRoot = RootFactory.createReadOnlyRoot(root);
        pbp = new PrivilegeBitsProvider(immutableRoot);
    }

    //-------------------------------------------------< PermissionProvider >---
    @Override
    public void refresh() {
        immutableRoot = RootFactory.createReadOnlyRoot(root);
        pbp = new PrivilegeBitsProvider(immutableRoot);

        for (PermissionProvider pp : pps) {
            pp.refresh();
        }
    }

    @Nonnull
    @Override
    public Set<String> getPrivileges(@Nullable final Tree tree) {
        PrivilegeBits result = null;
        Iterable<AggregatedPermissionProvider> providers = Iterables.filter(pps, new Predicate<AggregatedPermissionProvider>() {
            @Override
            public boolean apply(@Nullable AggregatedPermissionProvider pp) {
                return pp != null && ((tree != null) || pp.handlesRepositoryPermissions());
            }
        });
        for (AggregatedPermissionProvider pp : providers) {
            PrivilegeBits privs = pbp.getBits(pp.getPrivileges(tree));
            if (result == null) {
                result = PrivilegeBits.getInstance();
                result.add(privs);
            } else {
                // FIXME: only retain privs that are handled by prev. pp (and thus are denied)
                result.retain(privs);
            }
        }
        return pbp.getPrivilegeNames(result);
    }

    @Override
    public boolean hasPrivileges(@Nullable final Tree tree, @Nonnull String... privilegeNames) {
        for (final String privName : pbp.getAggregatedPrivilegeNames(privilegeNames)) {
            Iterable<AggregatedPermissionProvider> providers = Iterables.filter(pps, new Predicate<AggregatedPermissionProvider>() {
                @Override
                public boolean apply(@Nullable AggregatedPermissionProvider pp) {
                    return pp != null && ((tree == null) ? pp.handlesRepositoryPermissions() : pp.handles(tree, pbp.getBits(privName)));
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
    public boolean isGranted(@Nonnull final Tree parent, @Nullable PropertyState property, final long permissions) {
        if (Permissions.isAggregate(permissions)) {
            for (final long permission : Permissions.aggregates(permissions)) {
                Iterable<AggregatedPermissionProvider> providers = Iterables.filter(pps, new Predicate<AggregatedPermissionProvider>() {
                    @Override
                    public boolean apply(@Nullable AggregatedPermissionProvider pp) {
                        return pp != null && pp.handles(parent, permission);
                    }
                });
                if (!grantsPermission(parent, property, permission, providers)) {
                    return false;
                }
            }
            return true;
        } else {
            Iterable<AggregatedPermissionProvider> providers = Iterables.filter(pps, new Predicate<AggregatedPermissionProvider>() {
                @Override
                public boolean apply(@Nullable AggregatedPermissionProvider pp) {
                    return pp != null && pp.handles(parent, permissions);
                }
            });
            return grantsPermission(parent, property, permissions, providers);
        }
    }

    @Override
    public boolean isGranted(@Nonnull final String oakPath, @Nonnull String jcrActions) {
        final String[] actions = Text.explode(jcrActions, ',', false);
        switch (actions.length) {
            case 0: return true;
            case 1:
                Iterable<AggregatedPermissionProvider> providers = Iterables.filter(pps, new Predicate<AggregatedPermissionProvider>() {
                    @Override
                    public boolean apply(@Nullable AggregatedPermissionProvider pp) {
                        return pp != null && pp.handles(oakPath, actions[0]);
                    }
                });
                return grantsAction(oakPath, actions[0], providers);
            default:
                for (final String action : actions) {
                    providers = Iterables.filter(pps, new Predicate<AggregatedPermissionProvider>() {
                        @Override
                        public boolean apply(@Nullable AggregatedPermissionProvider pp) {
                            return pp != null && pp.handles(oakPath, action);
                        }
                    });
                    if (!grantsAction(oakPath, action, providers)) {
                        return false;
                    }
                }
                return true;
        }
    }

    //--------------------------------------------------------------------------

    private static boolean grantsPermission(@Nonnull final Tree parent,
                                            @Nullable PropertyState property,
                                            final long permission,
                                            @Nonnull Iterable<AggregatedPermissionProvider> providers) {
        Iterator<AggregatedPermissionProvider> it = providers.iterator();
        while (it.hasNext()) {
            AggregatedPermissionProvider pp = it.next();
            boolean isGranted = pp.isGranted(parent, property, permission);
            if (!it.hasNext() || !isGranted) {
                return isGranted;
            }
        }
        return false;
    }

    private static boolean grantsAction(@Nonnull final String oakPath,
                                        @Nonnull final String action,
                                        @Nonnull Iterable<AggregatedPermissionProvider> providers) {
        Iterator<AggregatedPermissionProvider> it = providers.iterator();
        while (it.hasNext()) {
            AggregatedPermissionProvider pp = it.next();
            boolean isGranted = pp.isGranted(oakPath, action);
            if (!it.hasNext() || !isGranted) {
                return isGranted;
            }
        }
        return false;
    }

    private static boolean grantsRepoPermission(long permission, @Nonnull Iterable<AggregatedPermissionProvider> providers) {
        Iterator<AggregatedPermissionProvider> it = providers.iterator();
        while (it.hasNext()) {
            AggregatedPermissionProvider pp = it.next();
            boolean isGranted = pp.getRepositoryPermission().isGranted(permission);
            if (!it.hasNext() || !isGranted) {
                return isGranted;
            }

        }
        return false;
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
                Iterator<Map.Entry<AggregatedPermissionProvider, TreePermission>> it = map.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<AggregatedPermissionProvider, TreePermission> entry = it.next();
                    TreePermission tp = entry.getValue();
                    if (entry.getKey().handles(tp, Permissions.READ_NODE)) {
                        boolean isGranted = entry.getValue().canRead();
                        if (!it.hasNext() || !isGranted) {
                            this.canRead = isGranted;
                            break;
                        }
                    }
                }
            }
            return canRead;
        }

        @Override
        public boolean canRead(@Nonnull PropertyState property) {
            Iterator<Map.Entry<AggregatedPermissionProvider, TreePermission>> it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<AggregatedPermissionProvider, TreePermission> entry = it.next();
                TreePermission tp = entry.getValue();
                if (entry.getKey().handles(tp, Permissions.READ_PROPERTY)) {
                    boolean isGranted = entry.getValue().canRead(property);
                    if (!it.hasNext() || !isGranted) {
                        return isGranted;
                    }
                }
            }
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
            Iterator<Map.Entry<AggregatedPermissionProvider, TreePermission>> it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<AggregatedPermissionProvider, TreePermission> entry = it.next();
                if (entry.getKey().handles(this, permission)) {
                    TreePermission tp = entry.getValue();
                    boolean isGranted = (property == null) ? tp.isGranted(permission) : tp.isGranted(permission, property);
                    if (!it.hasNext() || !isGranted) {
                        return isGranted;
                    }
                }
            }
            return false;
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

    private class CompositeRepositoryPermission implements RepositoryPermission {

        @Override
        public boolean isGranted(long repositoryPermissions) {
            Iterable<AggregatedPermissionProvider> providers = Iterables.filter(pps, new Predicate<AggregatedPermissionProvider>() {
                @Override
                public boolean apply(@Nullable AggregatedPermissionProvider provider) {
                    return provider != null && provider.handlesRepositoryPermissions();
                }
            });
            if (Permissions.isAggregate(repositoryPermissions)) {
                for (long permission : Permissions.aggregates(repositoryPermissions)) {
                    if (!grantsRepoPermission(permission, providers)) {
                        return false;
                    }
                }
                return true;
            } else {
                return grantsRepoPermission(repositoryPermissions, providers);
            }
        }
    }
}
