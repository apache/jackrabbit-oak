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

import java.util.function.Supplier;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeProvider;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.AND;
import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.OR;
import static org.apache.jackrabbit.oak.security.authorization.composite.Util.doEvaluate;

/**
 * {@code TreePermission} implementation that combines multiple {@code TreePermission}
 * implementations.
 */
abstract class CompositeTreePermission implements TreePermission {

    private final Tree tree;
    private final TreeType type;

    private final TreeProvider treeProvider;
    private final TreeTypeProvider typeProvider;
    private final AggregatedPermissionProvider[] providers;
    private final TreePermission[] treePermissions;
    private final int childSize;

    private Boolean canRead;
    private Boolean canReadProperties;

    private CompositeTreePermission(@NotNull Tree tree, @NotNull TreeType type,
                                    @NotNull TreeProvider treeProvider,
                                    @NotNull TreeTypeProvider typeProvider,
                                    @NotNull AggregatedPermissionProvider[] providers,
                                    @NotNull TreePermission[] treePermissions, int cnt) {
        this.tree = tree;
        this.type = type;

        this.treeProvider = treeProvider;
        this.typeProvider = typeProvider;
        this.providers = providers;
        this.treePermissions = treePermissions;
        this.childSize = providers.length - cnt;
    }

    static TreePermission create(@NotNull Tree rootTree,
                                 @NotNull TreeProvider treeProvider,
                                 @NotNull TreeTypeProvider typeProvider,
                                 @NotNull AggregatedPermissionProvider[] providers,
                                 @NotNull CompositionType compositionType) {
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
                if (compositionType == AND) {
                    return new CompositeTreePermissionAnd(rootTree, TreeType.DEFAULT, treeProvider, typeProvider, providers, treePermissions, cnt);
                } else {
                    return new CompositeTreePermissionOr(rootTree, TreeType.DEFAULT, treeProvider, typeProvider, providers, treePermissions, cnt);
                }
        }
    }

    static TreePermission create(@NotNull final Tree tree, @NotNull TreeProvider treeProvider, @NotNull CompositeTreePermission parentPermission) {
        return create(() -> tree, tree.getName(), treeProvider.asNodeState(tree), parentPermission, null);
    }

    static TreePermission create(@NotNull final Tree tree, @NotNull TreeProvider treeProvider, @NotNull CompositeTreePermission parentPermission,
                                 @Nullable TreeType treeType) {
        return create(() -> tree, tree.getName(), treeProvider.asNodeState(tree), parentPermission, treeType);
    }

    private static TreePermission create(@NotNull Supplier<Tree> lazyTree,
                                         @NotNull String childName, @NotNull NodeState childState,
                                         @NotNull CompositeTreePermission parentPermission,
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
                Tree tree = lazyTree.get();
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
                if (parentPermission.getCompositionType() == AND) {
                    return new CompositeTreePermissionAnd(tree, type, parentPermission.treeProvider, parentPermission.typeProvider, pvds, tps, cnt);
                } else {
                    return new CompositeTreePermissionOr(tree, type, parentPermission.treeProvider, parentPermission.typeProvider, pvds, tps, cnt);
                }
        }
    }

    //-----------------------------------------------------< TreePermission >---
    @NotNull
    @Override
    public TreePermission getChildPermission(@NotNull final String childName, @NotNull final NodeState childState) {
        return create(() -> treeProvider.createReadOnlyTree(tree, childName, childState), childName, childState, this, null);
    }

    @Override
    public boolean canRead() {
        if (canRead == null) {
            canRead = grantsRead(null);
        }
        return canRead;
    }

    @Override
    public boolean canRead(@NotNull PropertyState property) {
        return grantsRead(property);
    }

    @Override
    public boolean canReadAll() {
        return false;
    }

    @Override
    public boolean canReadProperties() {
        if (canReadProperties == null) {
            canReadProperties = grantsReadProperties();
        }
        return canReadProperties;
    }

    @Override
    public boolean isGranted(long permissions) {
        return grantsPermission(permissions, null);
    }

    @Override
    public boolean isGranted(long permissions, @NotNull PropertyState property) {
        return grantsPermission(permissions, property);
    }

    //---------------------------------------------------------------------------
    @NotNull
    abstract CompositionType getCompositionType();

    abstract boolean grantsPermission(long permissions, @Nullable PropertyState property);

    abstract boolean grantsRead(@Nullable PropertyState property);

    abstract boolean grantsReadProperties();

    //---------------------------------------------------------------------------

    int length() {
        return providers.length;
    }

    @NotNull
    AggregatedPermissionProvider provider(int index) {
        return providers[index];
    }

    @NotNull
    TreePermission treePermission(int index) {
        return treePermissions[index];
    }

    //------------------------------------------------------------< private >---

    private static boolean isValid(@NotNull TreePermission tp) {
        return NO_RECOURSE != tp;
    }

    private static TreeType getType(@NotNull Tree tree, @NotNull CompositeTreePermission parent) {
        return parent.typeProvider.getType(tree, parent.type);
    }

    //---< OR >-----------------------------------------------------------------

    private static final class CompositeTreePermissionOr extends CompositeTreePermission {

        private CompositeTreePermissionOr(@NotNull Tree tree, @NotNull TreeType type, @NotNull TreeProvider treeProvider, @NotNull TreeTypeProvider typeProvider, @NotNull AggregatedPermissionProvider[] providers, @NotNull TreePermission[] treePermissions, int cnt) {
            super(tree, type, treeProvider, typeProvider, providers, treePermissions, cnt);
        }

        @NotNull
        @Override
        CompositionType getCompositionType() {
            return OR;
        }

        @Override
        boolean grantsPermission(long permissions, @Nullable PropertyState property) {
            boolean isGranted = false;
            long coveredPermissions = Permissions.NO_PERMISSION;

            for (int i = 0; i < length(); i++) {
                TreePermission tp = treePermission(i);
                long supported = provider(i).supportedPermissions(tp, property, permissions);
                if (doEvaluate(supported)) {
                    for (long p : Permissions.aggregates(supported)) {
                        boolean aGrant = (property == null) ? tp.isGranted(p) : tp.isGranted(p, property);
                        if (aGrant) {
                            coveredPermissions |= p;
                            isGranted = true;
                        }
                    }
                }
            }
            return isGranted && coveredPermissions == permissions;
        }

        @Override
        boolean grantsRead(@Nullable PropertyState property) {
            if (property != null && canReadProperties()) {
                return true;
            }
            boolean readable = false;
            for (int i = 0; i < length(); i++) {
                TreePermission tp = treePermission(i);
                long supported = provider(i).supportedPermissions(tp, property, (property == null) ? Permissions.READ_NODE : Permissions.READ_PROPERTY);
                if (doEvaluate(supported)) {
                    readable = (property == null) ? tp.canRead() : tp.canRead(property);
                    if (readable) {
                        return true;
                    }
                }
            }
            return readable;
        }

        @Override
        boolean grantsReadProperties() {
            boolean readable = false;
            for (int i = 0; i < length(); i++) {
                TreePermission tp = treePermission(i);
                long supported = provider(i).supportedPermissions(tp, null, Permissions.READ_PROPERTY);
                if (doEvaluate(supported)) {
                    readable = tp.canReadProperties();
                    if (readable) {
                        break;
                    }
                }
            }
            return readable;
        }
    }

    //---< AND >----------------------------------------------------------------

    private static final class CompositeTreePermissionAnd extends CompositeTreePermission {

        private CompositeTreePermissionAnd(@NotNull Tree tree, @NotNull TreeType type,
                                           @NotNull TreeProvider treeProvider,
                                           @NotNull TreeTypeProvider typeProvider, @NotNull AggregatedPermissionProvider[] providers,
                                           @NotNull TreePermission[] treePermissions, int cnt) {
            super(tree, type, treeProvider, typeProvider, providers, treePermissions, cnt);
        }

        @NotNull
        CompositeAuthorizationConfiguration.CompositionType getCompositionType() {
            return AND;
        }

        boolean grantsPermission(long permissions, @Nullable PropertyState property) {
            boolean isGranted = false;
            long coveredPermissions = Permissions.NO_PERMISSION;

            for (int i = 0; i < length(); i++) {
                TreePermission tp = treePermission(i);
                long supported = provider(i).supportedPermissions(tp, property, permissions);
                if (doEvaluate(supported)) {
                    isGranted = (property == null) ? tp.isGranted(supported) : tp.isGranted(supported, property);
                    if (!isGranted) {
                        return false;
                    }
                    coveredPermissions |= supported;
                }
            }
            return isGranted && coveredPermissions == permissions;
        }

        boolean grantsRead(@Nullable PropertyState property) {
            if (property != null && canReadProperties()) {
                return true;
            }
            boolean readable = false;
            for (int i = 0; i < length(); i++) {
                TreePermission tp = treePermission(i);
                long supported = provider(i).supportedPermissions(tp, property, (property == null) ? Permissions.READ_NODE : Permissions.READ_PROPERTY);
                if (doEvaluate(supported)) {
                    readable = (property == null) ? tp.canRead() : tp.canRead(property);
                    if (!readable) {
                        return false;
                    }
                }
            }
            return readable;
        }

        boolean grantsReadProperties() {
            boolean readable = false;
            for (int i = 0; i < length(); i++) {
                TreePermission tp = treePermission(i);
                long supported = provider(i).supportedPermissions(tp, null, Permissions.READ_PROPERTY);
                if (doEvaluate(supported)) {
                    readable = tp.canReadProperties();
                    if (!readable) {
                        break;
                    }
                }
            }
            return readable;
        }
    }
}
