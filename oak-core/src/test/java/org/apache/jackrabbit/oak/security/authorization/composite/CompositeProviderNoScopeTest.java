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

import java.security.Principal;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test the effect of the combination of
 *
 * - default permission provider
 * - custom provider that doesn't support any permissions nowhere
 *
 * The tests are executed both for the set of principals associated with the test
 * user and with the admin session.
 * The expected outcome is that the composite provider behaves exactly like the
 * default provider (i.e. is never respected during evaluation).
 *
 * While there is no real use in such a {@link AggregatedPermissionProvider}, that
 * is never called, is is used here to verify that the composite provider doesn't
 * introduce any regressions compared to the default provider implementation.
 */
public class CompositeProviderNoScopeTest extends AbstractCompositeProviderTest {

    private CompositePermissionProvider cppTestUser;
    private PermissionProvider defTestUser;

    private CompositePermissionProvider cppAdminUser;
    private PermissionProvider defAdminUser;


    @Override
    public void before() throws Exception {
        super.before();

        ContentSession cs = root.getContentSession();

        Set<Principal> testPrincipals = ImmutableSet.of(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        cppTestUser = createPermissionProvider(testPrincipals);
        defTestUser = getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, cs.getWorkspaceName(), testPrincipals);

        Set<Principal> adminPrincipals = cs.getAuthInfo().getPrincipals();
        cppAdminUser = createPermissionProvider(adminPrincipals);
        defAdminUser = getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, cs.getWorkspaceName(), adminPrincipals);
    }

    @Override
    protected AggregatedPermissionProvider getTestPermissionProvider() {
        return new NotSupportingProvider();
    }

    @Test
    public void testGetPrivileges() throws Exception {
        for (String p : defPrivileges.keySet()) {
            Set<String> expected = defPrivileges.get(p);
            Tree tree = root.getTree(p);

            assertEquals(p, expected, cppTestUser.getPrivileges(tree));
            assertEquals(p, defTestUser.getPrivileges(tree), cppTestUser.getPrivileges(tree));
        }
    }

    @Test
    public void testGetPrivilegesAdmin() throws Exception {
        Set<String> expected = ImmutableSet.of(PrivilegeConstants.JCR_ALL);
        for (String p : NODE_PATHS) {
            Tree tree = root.getTree(p);

            assertEquals(p, expected, cppAdminUser.getPrivileges(tree));
            assertEquals(p, defAdminUser.getPrivileges(tree), cppAdminUser.getPrivileges(tree));
        }
    }

    @Test
    public void testGetPrivilegesOnRepo() throws Exception {
        Set<String> expected = ImmutableSet.of(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT, PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT);

        assertEquals(expected, cppTestUser.getPrivileges(null));
        assertEquals(defTestUser.getPrivileges(null), cppTestUser.getPrivileges(null));
    }

    @Test
    public void testGetPrivilegesOnRepoAdmin() throws Exception {
        Set<String> expected = ImmutableSet.of(PrivilegeConstants.JCR_ALL);

        assertEquals(expected, cppAdminUser.getPrivileges(null));
        assertEquals(defAdminUser.getPrivileges(null), cppAdminUser.getPrivileges(null));
    }


    @Test
    public void testHasPrivileges() throws Exception {
        for (String p : defPrivileges.keySet()) {
            Set<String> expected = defPrivileges.get(p);
            Tree tree = root.getTree(p);

            String[] privNames = expected.toArray(new String[expected.size()]);
            assertTrue(p, cppTestUser.hasPrivileges(tree, privNames));
            assertEquals(p, defTestUser.hasPrivileges(tree, privNames), cppTestUser.hasPrivileges(tree, privNames));
        }
    }

    @Test
    public void testHasPrivilegesAdmin() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = root.getTree(p);

            assertTrue(p, cppAdminUser.hasPrivileges(tree, PrivilegeConstants.JCR_ALL));
            assertEquals(p, defAdminUser.hasPrivileges(tree, PrivilegeConstants.JCR_ALL), cppAdminUser.hasPrivileges(tree, PrivilegeConstants.JCR_ALL));
        }
    }

    @Test
    public void testHasPrivilegesOnRepo() throws Exception {
        assertTrue(cppTestUser.hasPrivileges(null, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT, PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        assertEquals(
                defTestUser.hasPrivileges(null, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT, PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT),
                cppTestUser.hasPrivileges(null, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT, PrivilegeConstants.JCR_NODE_TYPE_DEFINITION_MANAGEMENT));

        assertTrue(cppTestUser.hasPrivileges(null));
        assertEquals(
                defTestUser.hasPrivileges(null),
                cppTestUser.hasPrivileges(null));
    }

    @Test
    public void testHasPrivilegeOnRepoAdminUser() throws Exception {
        assertTrue(cppAdminUser.hasPrivileges(null, PrivilegeConstants.JCR_ALL));
        assertEquals(
                defAdminUser.hasPrivileges(null, PrivilegeConstants.JCR_ALL),
                cppAdminUser.hasPrivileges(null, PrivilegeConstants.JCR_ALL));

        assertTrue(cppAdminUser.hasPrivileges(null));
        assertEquals(
                defAdminUser.hasPrivileges(null),
                cppAdminUser.hasPrivileges(null));
    }


    @Test
    public void testIsGranted() throws Exception {
        for (String p : defPermissions.keySet()) {
            long expected = defPermissions.get(p);
            Tree tree = root.getTree(p);

            assertTrue(p, cppTestUser.isGranted(tree, null, expected));
            assertEquals(p, defTestUser.isGranted(tree, null, expected), cppTestUser.isGranted(tree, null, expected));
        }
    }

    @Test
    public void testIsGrantedAdmin() throws Exception {
        for (String p : defPermissions.keySet()) {
            Tree tree = root.getTree(p);

            assertTrue(p, cppAdminUser.isGranted(tree, null, Permissions.ALL));
            assertEquals(p, defAdminUser.isGranted(tree, null, Permissions.ALL), cppAdminUser.isGranted(tree, null, Permissions.ALL));
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
    public void testRepositoryPermissionIsGranted() throws Exception {
        RepositoryPermission rp = cppTestUser.getRepositoryPermission();

        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertTrue(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
    }

    @Test
    public void testRepositoryPermissionIsGrantedAdminUser() throws Exception {
        RepositoryPermission rp = cppAdminUser.getRepositoryPermission();

        assertTrue(rp.isGranted(Permissions.ALL));
    }

    @Test
    public void testGetTreePermission() throws Exception {
        // TODO
    }

    private static final class NotSupportingProvider implements AggregatedPermissionProvider {

        @Nonnull
        @Override
        public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
            return PrivilegeBits.EMPTY;
        }

        @Override
        public long supportedPermissions(@Nullable Tree tree, @Nullable PropertyState property, long permissions) {
            return Permissions.NO_PERMISSION;
        }

        @Override
        public long supportedPermissions(@Nonnull TreeLocation location, long permissions) {
            return Permissions.NO_PERMISSION;
        }

        @Override
        public long supportedPermissions(@Nonnull TreePermission treePermission, long permissions) {
            return Permissions.NO_PERMISSION;
        }

        @Override
        public boolean isGranted(@Nonnull TreeLocation location, long permissions) {
            throw new UnsupportedOperationException("should never get here");
        }

        @Override
        public void refresh() {
            // nop
        }

        @Nonnull
        @Override
        public Set<String> getPrivileges(@Nullable Tree tree) {
            throw new UnsupportedOperationException("should never get here");
        }

        @Override
        public boolean hasPrivileges(@Nullable Tree tree, @Nonnull String... privilegeNames) {
            throw new UnsupportedOperationException("should never get here");
        }

        @Nonnull
        @Override
        public RepositoryPermission getRepositoryPermission() {
            throw new UnsupportedOperationException("should never get here");
        }

        @Nonnull
        @Override
        public TreePermission getTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
            // TODO: fix such that this is no required
            return TreePermission.EMPTY;
        }

        @Override
        public boolean isGranted(@Nonnull Tree tree, @Nullable PropertyState property, long permissions) {
            throw new UnsupportedOperationException("should never get here");
        }

        @Override
        public boolean isGranted(@Nonnull String oakPath, @Nonnull String jcrActions) {
            throw new UnsupportedOperationException("should never get here");
        }
    }
}