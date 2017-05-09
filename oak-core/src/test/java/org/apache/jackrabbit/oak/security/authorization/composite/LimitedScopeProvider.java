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

import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.Text;

/**
 * Test implementation of the {@code AggregatedPermissionProvider} with following
 * characteristics. It has a limited scope and supports
 *
 * - {@link Permissions#NAMESPACE_MANAGEMENT} and {@link Permissions#NODE_TYPE_DEFINITION_MANAGEMENT} on repository level
 * - {@link Permissions#WRITE} at the tree defined by {@link AbstractCompositeProviderTest#TEST_A_PATH}
 * - {@link Permissions#NO_PERMISSION} everywhere else.
 *
 * The permission setup defined by this provider is as follows:
 *
 * At the repository level
 * - {@link Permissions#NAMESPACE_MANAGEMENT} is denied
 * - {@link Permissions#NODE_TYPE_DEFINITION_MANAGEMENT} is allowed
 *
 * At {@link AbstractCompositeProviderTest#TEST_A_PATH}
 * - {@link Permissions#ADD_NODE} and {@link Permissions#ADD_PROPERTY} is denied
 * - all other aggregates of {@link Permissions#WRITE} are allowed.
 * - any other permissions are ignored
 *
 * Consequently any path outside of the scope of this provider is not affected
 * by the permission setup.
 */
class LimitedScopeProvider extends AbstractAggrProvider implements PrivilegeConstants {

    private static final Set<String> GRANTED_PRIVS = ImmutableSet.of(JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES);
    private static final Set<String> DENIED_PRIVS = ImmutableSet.of(JCR_ADD_CHILD_NODES, REP_ADD_PROPERTIES);

    private static final long GRANTED_PERMS = Permissions.REMOVE_NODE | Permissions.REMOVE_PROPERTY | Permissions.MODIFY_PROPERTY;
    private static final long DENIED_PERMS = Permissions.ADD_NODE | Permissions.ADD_PROPERTY;

    LimitedScopeProvider(@Nonnull Root root) {
        super(root);
    }

    //-------------------------------------------------< PermissionProvider >---
    @Nonnull
    @Override
    public Set<String> getPrivileges(@Nullable Tree tree) {
        if (tree == null) {
            return ImmutableSet.of(JCR_NODE_TYPE_DEFINITION_MANAGEMENT);
        } else if (isSupported(tree)) {
            return ImmutableSet.of(JCR_REMOVE_CHILD_NODES, JCR_REMOVE_NODE, REP_ALTER_PROPERTIES, REP_REMOVE_PROPERTIES);
        } else {
            return ImmutableSet.of();
        }
    }

    @Override
    public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
        Set<String> pSet = Sets.newHashSet(privilegeNames);
        if (tree == null) {
            if (pSet.contains(JCR_NAMESPACE_MANAGEMENT)) {
                return false;
            } else {
                return pSet.size() == 1 && pSet.contains(JCR_NODE_TYPE_DEFINITION_MANAGEMENT);
            }
        } else if (isSupported(tree)) {
            if (pSet.removeAll(DENIED_PRIVS)) {
                return false;
            } else if (pSet.removeAll(GRANTED_PRIVS)) {
                return pSet.isEmpty();
            }
        }

        return false;
    }

    @Nonnull
    @Override
    public RepositoryPermission getRepositoryPermission() {
        return new RepositoryPermission() {
            @Override
            public boolean isGranted(long repositoryPermissions) {
                return Permissions.NODE_TYPE_DEFINITION_MANAGEMENT == repositoryPermissions;
            }
        };
    }

    @Nonnull
    @Override
    public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
        return createTreePermission(tree.getPath());
    }

    @Override
    public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
        if (isSupported(tree)) {
            if (Permissions.includes(permissions, DENIED_PERMS)) {
                return false;
            } else {
                return Permissions.diff(permissions, GRANTED_PERMS) == Permissions.NO_PERMISSION;
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
        if (isSupported(oakPath)) {
            Tree tree = root.getTree(oakPath);
            long perms = Permissions.getPermissions(jcrActions, TreeLocation.create(tree), false);
            if (Permissions.includes(perms, DENIED_PERMS)) {
                return false;
            } else {
                return Permissions.diff(perms, GRANTED_PERMS) == Permissions.NO_PERMISSION;
            }
        } else {
            return false;
        }
    }

    //---------------------------------------< AggregatedPermissionProvider >---
    @Nonnull
    @Override
    public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
        PrivilegeBits supported;
        if (tree == null) {
            supported = PrivilegeBits.getInstance(
                    PrivilegeBits.BUILT_IN.get(JCR_NAMESPACE_MANAGEMENT),
                    PrivilegeBits.BUILT_IN.get(JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        } else if (isSupported(tree)) {
            supported = PrivilegeBits.BUILT_IN.get(JCR_WRITE);
        } else {
            supported = PrivilegeBits.EMPTY;
        }

        if (privilegeBits != null && !supported.isEmpty()) {
            return PrivilegeBits.getInstance(privilegeBits).retain(supported);
        } else {
            return supported;
        }
    }

    @Override
    public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
        if (tree == null) {
            return permissions & (Permissions.NAMESPACE_MANAGEMENT|Permissions.NODE_TYPE_DEFINITION_MANAGEMENT);
        } else if (isSupported(tree)) {
            return permissions & Permissions.WRITE;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public long supportedPermissions(@Nonnull TreeLocation location, long permissions) {
        if (isSupported(location.getPath())) {
            return permissions & Permissions.WRITE;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public long supportedPermissions(@Nonnull TreePermission treePermission, @Nullable PropertyState property, long permissions) {
        if (treePermission instanceof TestTreePermission && isSupported(((TestTreePermission) treePermission).path)) {
            return permissions & Permissions.WRITE;
        } else {
            return Permissions.NO_PERMISSION;
        }
    }

    @Override
    public boolean isGranted(@Nonnull TreeLocation location, long permissions) {
        if (isSupported(location.getPath())) {
            if (Permissions.includes(permissions, DENIED_PERMS)) {
                return false;
            } else {
                return Permissions.diff(permissions, GRANTED_PERMS) == Permissions.NO_PERMISSION;
            }
        } else {
            return false;
        }
    }

    //--------------------------------------------------------------------------
    boolean isSupported(@Nonnull Tree tree) {
        return isSupported(tree.getPath());
    }

    static boolean isSupported(@Nonnull String path) {
        return Text.isDescendantOrEqual(AbstractCompositeProviderTest.TEST_A_PATH, path);
    }

    private static TreePermission createTreePermission(@Nonnull String path) {
        if (isSupported(path)) {
            return new TestTreePermission(path);
        } else if (Text.isDescendant(path, AbstractCompositeProviderTest.TEST_A_PATH)) {
            return new EmptyTestPermission(path);
        } else {
            return TreePermission.NO_RECOURSE;
        }
    }

    private static final class EmptyTestPermission implements TreePermission {

        private final String path;

        private EmptyTestPermission(@Nonnull String path) {
            this.path = path;
        }

        @Nonnull
        @Override
        public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
            return createTreePermission(PathUtils.concat(path, childName));
        }

        @Override
        public boolean canRead() {
            return false;
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
            return false;
        }

        @Override
        public boolean isGranted(long permissions, @Nonnull PropertyState property) {
            return false;
        }
    }

    private static final class TestTreePermission implements TreePermission {

        private final String path;

        private TestTreePermission(@Nonnull String path) {
            this.path = path;
        }

        @Nonnull
        @Override
        public TreePermission getChildPermission(@Nonnull String childName, @Nonnull NodeState childState) {
            return createTreePermission(PathUtils.concat(path, childName));
        }

        @Override
        public boolean canRead() {
            return false;
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
            if (Permissions.includes(permissions, DENIED_PERMS)) {
                return false;
            } else {
                return Permissions.diff(permissions, GRANTED_PERMS) == Permissions.NO_PERMISSION;
            }
        }

        @Override
        public boolean isGranted(long permissions, @Nonnull PropertyState property) {
            if (Permissions.includes(permissions, DENIED_PERMS)) {
                return false;
            } else {
                return Permissions.diff(permissions, GRANTED_PERMS) == Permissions.NO_PERMISSION;
            }
        }
    }
}