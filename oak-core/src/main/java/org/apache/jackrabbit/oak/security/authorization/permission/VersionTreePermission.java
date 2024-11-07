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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.Objects;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.jetbrains.annotations.NotNull;

/**
 * {@link TreePermission} implementations for those items in the version storage
 * that are linked to a versionable node (i.e. the subtree spanned by every version
 * history node. For those items, the effective permissions are defined by
 * the corresponding versionable node (and it's ancestors).
 */
class VersionTreePermission implements TreePermission, VersionConstants {

    private static final Set<String> NT_NAMES = Set.of(NT_VERSION, NT_VERSIONLABELS);

    private final Tree versionTree;
    private final TreePermission versionablePermission;
    private final TreeProvider treeProvider;

    VersionTreePermission(@NotNull Tree versionTree, @NotNull TreePermission versionablePermission, @NotNull TreeProvider treeProvider) {
        this.versionTree = versionTree;
        this.versionablePermission = versionablePermission;
        this.treeProvider = treeProvider;
    }

    @NotNull
    VersionTreePermission createChildPermission(@NotNull Tree versionTree) {
        TreePermission delegatee;
        if (JCR_FROZENNODE.equals(versionTree.getName()) ||
                (Objects.nonNull(TreeUtil.getPrimaryTypeName(versionTree)) && NT_NAMES.contains(TreeUtil.getPrimaryTypeName(versionTree)))) {
            delegatee = versionablePermission;
        } else {
            delegatee = versionablePermission.getChildPermission(versionTree.getName(), treeProvider.asNodeState(versionTree));
        }
        return new VersionTreePermission(versionTree, delegatee, treeProvider);
    }

    //-----------------------------------------------------< TreePermission >---

    @NotNull
    @Override
    public TreePermission getChildPermission(@NotNull String childName, @NotNull NodeState childState) {
        return createChildPermission(treeProvider.createReadOnlyTree(versionTree, childName, childState));
    }

    @Override
    public boolean canRead() {
        return versionablePermission.canRead();
    }

    @Override
    public boolean canRead(@NotNull PropertyState property) {
        return versionablePermission.canRead(property);
    }

    @Override
    public boolean canReadAll() {
        return versionablePermission.canReadAll();
    }

    @Override
    public boolean canReadProperties() {
        return versionablePermission.canReadProperties();
    }

    @Override
    public boolean isGranted(long permissions) {
        return versionablePermission.isGranted(permissions);
    }

    @Override
    public boolean isGranted(long permissions, @NotNull PropertyState property) {
        return versionablePermission.isGranted(permissions, property);
    }
}
