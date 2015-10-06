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

import java.util.Arrays;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Session;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;

/**
 * Test implementation of the {@code AggregatedPermissionProvider} with following
 * characteristics:
 *
 * If {@code supportsAll} is {@code true} this provider supports all permissions
 * but only grants {@link Permissions#NAMESPACE_MANAGEMENT} on repository level
 * and {@link Permissions#READ_NODE} on regular items.
 * In this case the provider will always be respected for evaluation and will
 * therefore cause the final result to be always restricted to the permissions
 * granted by this provider.
 *
 * If {@code supportsAll} is {@code false} this provider supports
 * - {@link Permissions#NAMESPACE_MANAGEMENT} on repository level
 * - {@link Permissions#READ_NODE} at the tree defined by {@link AbstractCompositeProviderTest#TEST_A_PATH}
 * - {@link Permissions#NO_PERMISSION} everywhere else.
 * The permissions granted are the same as above. Due to the limited scope
 * however, the provider will in this case only respected for evaluation at
 * the supported paths (and at the repo level). The final result will restricted
 * to the permissions granted by this provider at the supported paths. For all
 * other paths the access limitations of this provider have no effect.
 */
class TestPermissionProvider implements AggregatedPermissionProvider {

    private final Root root;
    private final boolean supportsAll;

    TestPermissionProvider(@Nonnull Root root, boolean supportsAll) {
        this.root = root;
        this.supportsAll = supportsAll;
    }

    //-------------------------------------------------< PermissionProvider >---
    @Override
    public void refresh() {
        //nop
    }

    @Nonnull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        if (tree == null) {
            return ImmutableSet.of(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT);
        } else if (isSupported(tree)) {
            return ImmutableSet.of(PrivilegeConstants.REP_READ_NODES);
        } else {
            return ImmutableSet.of();
        }

    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
        if (tree == null) {
            return Arrays.equals(new String[]{PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT}, privilegeNames);
        } else if (isSupported(tree)) {
            return Arrays.equals(new String[]{PrivilegeConstants.REP_READ_NODES}, privilegeNames);
        } else {
            return false;
        }
    }

    @Nonnull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return new RepositoryPermission() {
            @Override
            public boolean isGranted(long repositoryPermissions) {
                return Permissions.NAMESPACE_MANAGEMENT == repositoryPermissions;
            }
        };
    }

    @Nonnull
    @Override
    public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
        return (isSupported(tree)) ? new TestTreePermission(tree.getPath()) : TreePermission.EMPTY;
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
        return isSupported(tree) && property == null && permissions == Permissions.READ_NODE;
    }

    @Override
    public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
        Tree tree = root.getTree(oakPath);
        return tree.exists() && isSupported(tree) && Session.ACTION_READ.equals(jcrActions);
    }

    //---------------------------------------< AggregatedPermissionProvider >---
    @Nonnull
    @Override
    public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
        if (supportsAll) {
            return (privilegeBits == null) ? new PrivilegeBitsProvider(root).getBits(PrivilegeConstants.JCR_ALL) : privilegeBits;
        } else {
            PrivilegeBits supported;
            if (tree == null) {
                supported = PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT);
            } else if (isSupportedPath(tree.getPath())) {
                supported = PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_NODES);
            } else {
                supported = PrivilegeBits.EMPTY;
            }

            if (privilegeBits != null && !supported.isEmpty()) {
                return PrivilegeBits.getInstance(privilegeBits).retain(supported);
            } else {
                return supported;
            }
        }
    }

    @Override
    public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
        if (supportsAll) {
            return permissions;
        } else {
            if (tree == null) {
                return permissions & Permissions.NAMESPACE_MANAGEMENT;
            } else if (isSupportedPath(tree.getPath())) {
                return permissions & Permissions.READ_NODE;
            } else {
                return Permissions.NO_PERMISSION;
            }
        }
    }

    @Override
    public long supportedPermissions(@Nonnull TreeLocation location, long permissions) {
        if (supportsAll) {
            return permissions;
        } else if (isSupportedPath(location.getPath())) {
            return permissions & Permissions.READ_NODE;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public long supportedPermissions(@Nonnull TreePermission treePermission, long permissions) {
        if (supportsAll) {
            return permissions;
        } else if (isSupportedPath(((TestTreePermission) treePermission).path)) {
            return permissions & Permissions.READ_NODE;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public boolean isGranted(@Nonnull TreeLocation location, long permissions) {
        if (supportsAll) {
            return permissions == Permissions.READ_NODE;
        } else if (isSupportedPath(location.getPath())) {
            return permissions == Permissions.READ_NODE;
        } else {
            return false;
        }
    }

    //--------------------------------------------------------------------------
    private boolean isSupported(@Nonnull Tree tree) {
        return supportsAll || isSupportedPath(tree.getPath());
    }

    private boolean isSupportedPath(@Nonnull String path) {
        return Text.isDescendantOrEqual(AbstractCompositeProviderTest.TEST_A_PATH, path);
    }

    private final class TestTreePermission implements TreePermission {

        private final String path;

        private TestTreePermission(@Nonnull String path) {
            this.path = path;
        }

        @Nonnull
        @Override
        public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
            return new TestTreePermission(PathUtils.concat(path, childName));
        }

        @Override
        public boolean canRead() {
            return true;
        }

        @Override
        public boolean canRead(@Nonnull PropertyState property) {
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
            return Permissions.READ_NODE == permissions;
        }

        @Override
        public boolean isGranted(long permissions, @Nonnull PropertyState property) {
            return false;
        }
    }
}