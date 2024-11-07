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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.apache.jackrabbit.oak.security.authorization.composite.AbstractCompositeProviderTest.TEST_A_B_C_PATH;
import static org.apache.jackrabbit.oak.security.authorization.composite.AbstractCompositeProviderTest.TEST_A_PATH;
import static org.apache.jackrabbit.oak.security.authorization.composite.AbstractCompositeProviderTest.TEST_CHILD_PATH;
import static org.apache.jackrabbit.oak.security.authorization.composite.AbstractCompositeProviderTest.TEST_PATH;
import static org.apache.jackrabbit.oak.security.authorization.composite.AbstractCompositeProviderTest.TEST_PATH_2;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.ADD_NODE;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.ALL;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.MODIFY_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.NODE_TYPE_DEFINITION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.NO_PERMISSION;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.READ;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.READ_NODE;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.SET_PROPERTY;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.WORKSPACE_MANAGEMENT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for OAK-9798
 * 
 * @see CompositeTreePermissionOrTest#testIsGrantedUncoveredPermissions() for a similar test covering the behavior of {@code CompositeTreePermissionOr}.
 */
@RunWith(Parameterized.class)
public class CompositePermissionProviderOrTest extends AbstractSecurityTest {
    
    private final boolean extraVerification;
    
    private CompositePermissionProvider pp;
    private Root readOnlyRoot;

    @Parameterized.Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] { true, "Extra verification of supported permissions in 'isGranted'" },
                new Object[] { false, "No verification of supported permissions in 'isGranted'" });
    }    
    public CompositePermissionProviderOrTest(boolean extraVerification, @NotNull String name) {
        this.extraVerification = extraVerification;
    }
    
    @Before()
    public void before() throws Exception {
        super.before();
        AbstractCompositeProviderTest.setupTestContent(root);
        this.pp = createPermissionProviderOR(Collections.singleton(EveryonePrincipal.getInstance()));

        readOnlyRoot = getRootProvider().createReadOnlyRoot(root);
    }

    private CompositePermissionProvider createPermissionProviderOR(Set<Principal> principals) {
        String workspaceName = root.getContentSession().getWorkspaceName();
        AuthorizationConfiguration config = getConfig(AuthorizationConfiguration.class);

        ImmutableList<AggregatedPermissionProvider> l = ImmutableList.of(
                (AggregatedPermissionProvider) config.getPermissionProvider(root, workspaceName, principals),
                new ReadNodeGrantedInSupportedTree());
        
        return new CompositePermissionProviderOr(root, l, config.getContext(), getRootProvider(), getTreeProvider());
    }

    private Tree getTree(@NotNull String path) {
        return readOnlyRoot.getTree(path);
    }
    
    private TreeLocation getTreeLocation(@NotNull String path) {
        return TreeLocation.create(readOnlyRoot, path);
    }
    
    @Test
    public void testIsGrantedTreeUnsupportedPath() {
        for (long permissions : new long[]{READ_NODE, READ, SET_PROPERTY, ADD_NODE, MODIFY_ACCESS_CONTROL, ALL}) {
            assertFalse(pp.isGranted(getTree(TEST_PATH), null, permissions));
            assertFalse(pp.isGranted(getTree(ROOT_PATH), null, permissions));
            assertFalse(pp.isGranted(getTree(TEST_CHILD_PATH), null, permissions));
            assertFalse(pp.isGranted(getTree(TEST_PATH_2), null, permissions));
        }
    }

    @Test
    public void testIsGrantedTreeSupportedPath() {
        assertTrue(pp.isGranted(getTree(TEST_A_PATH), null, READ_NODE));
        assertTrue(pp.isGranted(getTree(TEST_A_B_C_PATH), null, READ_NODE));

        for (long permissions : new long[]{READ, SET_PROPERTY, ADD_NODE, MODIFY_ACCESS_CONTROL, ALL}) {
            assertFalse(pp.isGranted(getTree(TEST_A_PATH), null, permissions));
            assertFalse(pp.isGranted(getTree(TEST_A_B_C_PATH), null, permissions));
        }
    }

    @Test
    public void testIsGrantedTreeLocationUnsupportedPath() {
        for (long permissions : new long[]{READ_NODE, READ, SET_PROPERTY, ADD_NODE, MODIFY_ACCESS_CONTROL, ALL}) {
            assertFalse(pp.isGranted(getTreeLocation(TEST_PATH), permissions));
            assertFalse(pp.isGranted(getTreeLocation(ROOT_PATH), permissions));
            assertFalse(pp.isGranted(getTreeLocation(TEST_CHILD_PATH), permissions));
            assertFalse(pp.isGranted(getTreeLocation(TEST_PATH_2), permissions));
        }
    }

    @Test
    public void testIsGrantedTreeLocationSupportedPath() {
        assertTrue(pp.isGranted(getTreeLocation(TEST_A_PATH), READ_NODE));
        assertTrue(pp.isGranted(getTreeLocation(TEST_A_B_C_PATH), READ_NODE));

        for (long permissions : new long[]{READ, SET_PROPERTY, ADD_NODE, MODIFY_ACCESS_CONTROL, ALL}) {
            assertFalse(pp.isGranted(getTreeLocation(TEST_A_PATH), permissions));
            assertFalse(pp.isGranted(getTreeLocation(TEST_A_B_C_PATH), permissions));
        }
    }
    
    @Test
    public void testRepositoryPermissions() {
        RepositoryPermission rp = pp.getRepositoryPermission();
        
        assertTrue(rp.isGranted(WORKSPACE_MANAGEMENT));
        
        assertFalse(rp.isGranted(ALL));
        assertFalse(rp.isGranted(WORKSPACE_MANAGEMENT|NODE_TYPE_DEFINITION_MANAGEMENT));
    }
    
    private final class ReadNodeGrantedInSupportedTree implements AggregatedPermissionProvider {
        
        private boolean isSupportedPath(@NotNull String path) {
            return Text.isDescendantOrEqual(TEST_A_PATH, path);
        }
        
        private boolean includesSupportedPermission(long permissions) {
            return Permissions.includes(permissions, READ_NODE);
        }
        
        private boolean isGranted(@NotNull String path, long permissions) {
            if (extraVerification) {
                return isSupportedPath(path) && permissions == READ_NODE;
            } else {
                return isSupportedPath(path);
            }
        }
        
        @Override
        public @NotNull PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
            if (tree == null) {
                return (Permissions.includes(permissions, WORKSPACE_MANAGEMENT)) ?  WORKSPACE_MANAGEMENT : NO_PERMISSION;
            }
            if (isSupportedPath(tree.getPath()) && includesSupportedPermission(permissions)) {
                return READ_NODE;
            }
            return NO_PERMISSION;
        }

        @Override
        public long supportedPermissions(@NotNull TreeLocation location, long permissions) {
            if (isSupportedPath(location.getPath()) && includesSupportedPermission(permissions)) {
                return READ_NODE;
            }
            return NO_PERMISSION;
        }

        @Override
        public long supportedPermissions(@NotNull TreePermission treePermission, @Nullable PropertyState property, long permissions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isGranted(@NotNull TreeLocation location, long permissions) {
            return isGranted(location.getPath(), permissions);
        }

        @Override
        public @NotNull TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreeType type, @NotNull TreePermission parentPermission) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void refresh() {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull Set<String> getPrivileges(@Nullable Tree tree) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasPrivileges(@Nullable Tree tree, @NotNull String... privilegeNames) {
            throw new UnsupportedOperationException();
        }

        @Override
        public @NotNull RepositoryPermission getRepositoryPermission() {
            return repositoryPermissions -> {
                if (extraVerification) {
                    return repositoryPermissions == WORKSPACE_MANAGEMENT;
                } else {
                    return true;
                }
            };
        }

        @Override
        public @NotNull TreePermission getTreePermission(@NotNull Tree tree, @NotNull TreePermission parentPermission) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isGranted(@NotNull Tree tree, @Nullable PropertyState property, long permissions) {
            return isGranted(tree.getPath(), permissions);
        }

        @Override
        public boolean isGranted(@NotNull String oakPath, @NotNull String jcrActions) {
            return isGranted(oakPath, Permissions.getPermissions(jcrActions, TreeLocation.create(readOnlyRoot, oakPath), false));
        }
    }
}