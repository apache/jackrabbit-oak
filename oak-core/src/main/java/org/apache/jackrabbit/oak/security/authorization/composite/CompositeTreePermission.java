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

import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
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
    private final Map<AggregatedPermissionProvider, TreePermission> map;

    private Boolean canRead;

    CompositeTreePermission(@Nonnull Iterable<AggregatedPermissionProvider> providers) {
        tree = null;
        ImmutableMap.Builder<AggregatedPermissionProvider, TreePermission> builder = ImmutableMap.builder();
        for (AggregatedPermissionProvider app : providers) {
            builder.put(app, TreePermission.EMPTY);
        }
        map = builder.build();
    }

    CompositeTreePermission(@Nonnull final ImmutableTree tree, @Nonnull CompositeTreePermission parentPermission) {
        this.tree = tree;

        TreePermission parent;
        AggregatedPermissionProvider provider;

        int size = parentPermission.map.size();
        switch (size) {
            case 0:
                map = ImmutableMap.of();
                break;
            case 1:
                provider = parentPermission.map.keySet().iterator().next();
                parent = getParentPermission(parentPermission, provider);
                if (NO_RECOURSE != parent) {
                    map = ImmutableMap.of(provider, provider.getTreePermission(tree, parent));
                } else {
                    map = ImmutableMap.of();
                }
                break;
            default:
                map = new LinkedHashMap<AggregatedPermissionProvider, TreePermission>(size);
                for (AggregatedPermissionProvider app : parentPermission.map.keySet()) {
                    parent = getParentPermission(parentPermission, app);
                    if (NO_RECOURSE != parent) {
                        TreePermission tp = app.getTreePermission(tree, parent);
                        map.put(app, tp);
                    }
                }
        }
    }

    //-----------------------------------------------------< TreePermission >---
    @Nonnull
    @Override
    public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
        if (tree == null) {
            throw new IllegalStateException();
        }
        ImmutableTree childTree = new ImmutableTree(tree, childName, childState);
        return new CompositeTreePermission(childTree, this);
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
        return false;
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

        for (Map.Entry<AggregatedPermissionProvider, TreePermission> entry : map.entrySet()) {
            TreePermission tp = entry.getValue();
            long supported = entry.getKey().supportedPermissions(tp, property, permissions);
            if (doEvaluate(supported)) {
                isGranted = (property == null) ? tp.isGranted(supported) : tp.isGranted(supported, property);
                coveredPermissions |= supported;

                if (!isGranted) {
                    return false;
                }
            }
        }
        return isGranted && coveredPermissions == permissions;
    }

    private boolean grantsRead(@Nullable PropertyState property) {
        boolean readable = false;
        for (Map.Entry<AggregatedPermissionProvider, TreePermission> entry : map.entrySet()) {
            TreePermission tp = entry.getValue();
            long supported = entry.getKey().supportedPermissions(tp, property, (property == null) ? Permissions.READ_NODE : Permissions.READ_PROPERTY);
            if (doEvaluate(supported)) {
                readable = (property == null) ? tp.canRead() : tp.canRead(property);
                if (!readable) {
                    break;
                }
            }
        }
        return readable;
    }

    private static boolean doEvaluate(long supportedPermissions) {
        return CompositePermissionProvider.doEvaluate(supportedPermissions);
    }

    @Nonnull
    private static TreePermission getParentPermission(@Nonnull CompositeTreePermission compositeParent,
                                                      @Nonnull AggregatedPermissionProvider provider) {
        TreePermission parent = compositeParent.map.get(provider);
        return (parent == null) ? TreePermission.EMPTY : parent;
    }
}