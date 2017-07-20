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

import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.AND;
import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.OR;

import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code TreePermission} implementation that combines multiple {@code TreePermission}
 * implementations.
 */
final class CompositeTreePermission implements TreePermission {

    private final ImmutableTree tree;
    private final TreeType type;
    private final CompositionType compositionType;

    private final TreeTypeProvider typeProvider;
    private final AggregatedPermissionProvider[] providers;
    private final TreePermission[] treePermissions;
    private final int childSize;

    private Boolean canRead;
    private Boolean canReadProperties;

    private CompositeTreePermission(@Nonnull ImmutableTree tree, @Nonnull TreeType type,
            @Nonnull TreeTypeProvider typeProvider, @Nonnull AggregatedPermissionProvider[] providers,
            @Nonnull TreePermission[] treePermissions, int cnt, @Nonnull CompositionType compositionType) {
        this.tree = tree;
        this.type = type;

        this.typeProvider = typeProvider;
        this.providers = providers;
        this.treePermissions = treePermissions;
        this.childSize = providers.length - cnt;
        this.compositionType = compositionType;
    }

    static TreePermission create(@Nonnull ImmutableTree rootTree, @Nonnull TreeTypeProvider typeProvider,
            @Nonnull AggregatedPermissionProvider[] providers, @Nonnull CompositionType compositionType) {
        switch (providers.length) {
            case 0 : return TreePermission.EMPTY;
            case 1 : return providers[0].getTreePermission(rootTree, TreeType.DEFAULT, TreePermission.EMPTY);
            default :
                int cnt = 0;
                TreePermission[] treePermissions = new TreePermission[providers.length];
                for (int i = 0; i < providers.length; i++) {
                    TreePermission tp = providers[i].getTreePermission(rootTree, TreeType.DEFAULT, TreePermission.EMPTY);
                    if (!isValid(tp)) {
                        cnt++;
                    }
                    treePermissions[i] = tp;
                }
            return new CompositeTreePermission(rootTree, TreeType.DEFAULT, typeProvider, providers, treePermissions,
                    cnt, compositionType);
        }
    }

    static TreePermission create(@Nonnull final ImmutableTree tree, @Nonnull CompositeTreePermission parentPermission) {
        return create(() -> tree, tree.getName(), tree.getNodeState(), parentPermission, null);
    }

    static TreePermission create(@Nonnull final ImmutableTree tree, @Nonnull CompositeTreePermission parentPermission,
            @Nullable TreeType treeType) {
        return create(() -> tree, tree.getName(), tree.getNodeState(), parentPermission, treeType);
    }

    private static TreePermission create(@Nonnull Supplier<ImmutableTree> lazyTree, @Nonnull String childName, @Nonnull NodeState childState, @Nonnull CompositeTreePermission parentPermission,
            @Nullable TreeType treeType) {
        switch (parentPermission.childSize) {
            case 0: return TreePermission.EMPTY;
            case 1:
                TreePermission parent = null;
                for (TreePermission tp : parentPermission.treePermissions) {
                        if (isValid(tp)) {
                            parent = tp;
                            break;
                        }
                    }
                return (parent == null) ? TreePermission.EMPTY : parent.getChildPermission(childName, childState);
            default:
                ImmutableTree tree = lazyTree.get();
                TreeType type;
                if (treeType != null) {
                    type = treeType;
                } else {
                    type = getType(tree, parentPermission);
                }

                AggregatedPermissionProvider[] pvds = new AggregatedPermissionProvider[parentPermission.childSize];
                TreePermission[] tps = new TreePermission[parentPermission.childSize];
                int cnt = 0;
                for (int i = 0, j = 0; i < parentPermission.providers.length; i++) {
                    parent = parentPermission.treePermissions[i];
                    if (isValid(parent)) {
                        AggregatedPermissionProvider provider = parentPermission.providers[i];
                        TreePermission tp = provider.getTreePermission(tree, type, parent);
                        if (!isValid(tp)) {
                            cnt++;
                        }
                        tps[j] = tp;
                        pvds[j] = provider;
                        j++;
                    }
                }
            return new CompositeTreePermission(tree, type, parentPermission.typeProvider, pvds, tps, cnt,
                    parentPermission.compositionType);
        }
    }

    //-----------------------------------------------------< TreePermission >---
    @Nonnull
    @Override
    public TreePermission getChildPermission(@Nonnull final String childName, @Nonnull final NodeState childState) {
        return create(() -> new ImmutableTree(tree, childName, childState), childName, childState, this, null);
    }

    @Override
    public boolean canRead() {
        if (canRead == null) {
            canRead = grantsRead(null);
        }
        return canRead;
    }

    @Override
    public boolean canRead(@Nonnull PropertyState property) {
        return grantsRead(property);
    }

    @Override
    public boolean canReadAll() {
        return false;
    }

    @Override
    public boolean canReadProperties() {
        if (canReadProperties == null) {
            boolean readable = false;
            for (int i = 0; i < providers.length; i++) {
                TreePermission tp = treePermissions[i];
                long supported = providers[i].supportedPermissions(tp, null, Permissions.READ_PROPERTY);
                if (doEvaluate(supported)) {
                    readable = tp.canReadProperties();
                    if (!readable && compositionType == AND) {
                        break;
                    }
                    if (readable && compositionType == OR) {
                        break;
                    }
                }
            }
            canReadProperties = readable;
        }
        return canReadProperties;
    }

    @Override
    public boolean isGranted(long permissions) {
        return grantsPermission(permissions, null);
    }

    @Override
    public boolean isGranted(long permissions, @Nonnull PropertyState property) {
        return grantsPermission(permissions, property);
    }

    //------------------------------------------------------------< private >---

    private boolean grantsPermission(long permissions, @Nullable PropertyState property) {
        boolean isGranted = false;
        long coveredPermissions = Permissions.NO_PERMISSION;

        for (int i = 0; i < providers.length; i++) {
            TreePermission tp = treePermissions[i];
            long supported = providers[i].supportedPermissions(tp, property, permissions);
            if (doEvaluate(supported)) {
                if (compositionType == AND) {
                    isGranted = (property == null) ? tp.isGranted(supported) : tp.isGranted(supported, property);
                    if (!isGranted) {
                        return false;
                    }
                    coveredPermissions |= supported;
                } else {
                    for (long p : Permissions.aggregates(permissions)) {
                        boolean aGrant = (property == null) ? tp.isGranted(p) : tp.isGranted(p, property);
                        if (aGrant) {
                            coveredPermissions |= p;
                            isGranted = true;
                        }
                    }
                }
            }
        }
        return isGranted && coveredPermissions == permissions;
    }

    private boolean grantsRead(@Nullable PropertyState property) {
        if (property != null && canReadProperties()) {
            return true;
        }
        boolean readable = false;
        for (int i = 0; i < providers.length; i++) {
            TreePermission tp = treePermissions[i];
            long supported = providers[i].supportedPermissions(tp, property, (property == null) ? Permissions.READ_NODE : Permissions.READ_PROPERTY);
            if (doEvaluate(supported)) {
                readable = (property == null) ? tp.canRead() : tp.canRead(property);
                if (!readable && compositionType == AND) {
                    return false;
                }
                if (readable && compositionType == OR) {
                    return true;
                }
            }
        }
        return readable;
    }

    private static boolean doEvaluate(long supportedPermissions) {
        return supportedPermissions != Permissions.NO_PERMISSION;
    }

    private static boolean isValid(@Nonnull TreePermission tp) {
        return NO_RECOURSE != tp;
    }

    private static TreeType getType(@Nonnull Tree tree, @Nonnull CompositeTreePermission parent) {
        return parent.typeProvider.getType(tree, parent.type);
    }
}