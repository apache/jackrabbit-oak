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

import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;

/**
 * {@link TreePermission} implementations for those items in the version storage
 * that are linked to a versionable node (i.e. the subtree spanned by every version
 * history node. For those items, the effective permissions are defined by
 * the corresponding versionable node (and it's ancestors).
 */
class VersionTreePermission implements TreePermission, VersionConstants {

    private static final Set<String> NT_NAMES = ImmutableSet.of(NT_VERSION, NT_VERSIONLABELS);

    private final Tree versionTree;
    private final TreePermission versionablePermission;

    VersionTreePermission(@Nonnull Tree versionTree, @Nonnull TreePermission versionablePermission) {
        this.versionTree = versionTree;
        this.versionablePermission = versionablePermission;
    }

    VersionTreePermission createChildPermission(@Nonnull Tree versionTree) {
        TreePermission delegatee;
        if (JCR_FROZENNODE.equals(versionTree.getName()) || NT_NAMES.contains(TreeUtil.getPrimaryTypeName(versionTree))) {
            delegatee = versionablePermission;
        } else {
            delegatee = versionablePermission.getChildPermission(versionTree.getName(), ((ImmutableTree) versionTree).getNodeState());
        }
        return new VersionTreePermission(versionTree, delegatee);
    }

    //-----------------------------------------------------< TreePermission >---

    @Nonnull
    @Override
    public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
        return createChildPermission(new ImmutableTree((ImmutableTree) versionTree, childName, childState));
    }

    @Override
    public boolean canRead() {
        return versionablePermission.canRead();
    }

    @Override
    public boolean canRead(@Nonnull PropertyState property) {
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
    public boolean isGranted(long permissions, @Nonnull PropertyState property) {
        return versionablePermission.isGranted(permissions, property);
    }
}