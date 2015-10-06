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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.OpenPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the effect of the combination of
 *
 * - default permission provider (which a limited permission setup)
 * - custom provider that always grants full access and supports all permissions.
 *
 * for the {@link #getTestUser()}.
 *
 * The expected result is only the subset of permissions granted by the default
 * provider. The test user must never have full access anywhere.
 */
public class CompositeProviderAllTest extends AbstractCompositeProviderTest {

    private CompositePermissionProvider cpp;


    @Override
    public void before() throws Exception {
        super.before();

        cpp = createPermissionProvider(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
    }

    @Override
    protected AggregatedPermissionProvider getTestPermissionProvider() {
        return new OpenAggregateProvider(root);
    }

    @Test
    public void testGetPrivileges() throws Exception {
        for (String p : defPrivileges.keySet()) {
            Set<String> expected = defPrivileges.get(p);
            Tree tree = root.getTree(p);

            assertEquals(p, expected, cpp.getPrivileges(tree));
        }
    }

    @Test
    public void testGetPrivilegesOnRepo() throws Exception {
        Set<String> privilegeNames = cpp.getPrivileges(null);
        assertEquals(ImmutableSet.of(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT, PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT), privilegeNames);
    }


    @Test
    public void testHasPrivileges() throws Exception {
        for (String p : defPrivileges.keySet()) {
            Set<String> expected = defPrivileges.get(p);
            Tree tree = root.getTree(p);

            assertTrue(p, cpp.hasPrivileges(tree, expected.toArray(new String[expected.size()])));
        }
    }


    @Test
    public void testHasPrivilegesOnRepo() throws Exception {
        assertTrue(cpp.hasPrivileges(null, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT, PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
    }


    @Test
    public void testIsGranted() throws Exception {
        for (String p : defPermissions.keySet()) {
            long expected = defPermissions.get(p);
            Tree tree = root.getTree(p);

            assertTrue(p, cpp.isGranted(tree, null, expected));
        }
    }

    @Test
    public void testIsGrantedProperty() throws Exception {
        // TODO
    }

    @Test
    public void testIsGrantedAction() throws Exception {
        // TODO
    }

    @Test
    public void testRepositoryPermissionsIsGranted() throws Exception {
        RepositoryPermission rp = cpp.getRepositoryPermission();
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertTrue(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
    }

    @Test
    public void testGetTreePermission() throws Exception {
        // TODO
    }

    /**
     * Custom permission provider that supports all permissions and grants
     * full access for everyone.
     */
    private static final class OpenAggregateProvider implements AggregatedPermissionProvider {

        private static final PermissionProvider BASE = OpenPermissionProvider.getInstance();
        private final Root root;

        private OpenAggregateProvider(@Nonnull Root root) {
            this.root = root;
        }

        @Nonnull
        @Override
        public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
            return new PrivilegeBitsProvider(root).getBits(PrivilegeConstants.JCR_ALL);
        }

        //-----------------------------------< AggregatedPermissionProvider >---
        @Override
        public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
            return permissions;
        }

        @Override
        public long supportedPermissions(@Nonnull TreeLocation location, long permissions) {
            return permissions;
        }

        @Override
        public long supportedPermissions(@Nonnull TreePermission treePermission, long permissions) {
            return permissions;
        }

        @Override
        public boolean isGranted(@Nonnull TreeLocation location, long permissions) {
            return true;
        }

        //---------------------------------------------< PermissionProvider >---
        @Override
        public void refresh() {
            root.refresh();
        }

        @Nonnull
        @Override
        public Set<String> getPrivileges(@Nullable Tree tree) {
            return BASE.getPrivileges(tree);
        }

        @Override
        public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
            return BASE.hasPrivileges(tree, privilegeNames);
        }

        @Nonnull
        @Override
        public RepositoryPermission getRepositoryPermission() {
            return BASE.getRepositoryPermission();
        }

        @Nonnull
        @Override
        public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
            return BASE.getTreePermission(tree, parentPermission);
        }

        @Override
        public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
            return BASE.isGranted(tree, property, permissions);
        }

        @Override
        public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
            return BASE.isGranted(oakPath, jcrActions);
        }
    };
}