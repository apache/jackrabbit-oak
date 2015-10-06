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
import javax.jcr.Session;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.util.Text;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test the effect of the combination of
 *
 * - default permission provider, which always grants full access to an administrative session.
 * - custom provider that prevents all access but supports all permissions and
 *   thus is always called during composite evaluation.
 *
 * The tests are executed with the set of principals associated with the admin session,
 * which in the default permission provider is granted full access.
 *
 * The expected outcome is that despite the default provider granting full access,
 * the combination effectively prevents any access.
 */
public class CompositeProviderEmptyTest extends AbstractCompositeProviderTest {

    private CompositePermissionProvider cpp;

    @Override
    public void before() throws Exception {
        super.before();
        cpp = createPermissionProvider(root.getContentSession().getAuthInfo().getPrincipals());
    }

    @Override
    protected AggregatedPermissionProvider getTestPermissionProvider() {
        return new EmptyAggregatedProvider(root);
    }

    @Test
    public void testGetPrivileges() throws Exception {
        for (String p : NODE_PATHS) {
            assertTrue(cpp.getPrivileges(root.getTree(p)).isEmpty());
        }
    }

    @Test
    public void testGetPrivilegesOnRepo() throws Exception {
        assertTrue(cpp.getPrivileges(null).isEmpty());
    }

    @Test
    public void testHasPrivileges() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = root.getTree(p);

            assertFalse(cpp.hasPrivileges(tree, PrivilegeConstants.JCR_READ));
            assertFalse(cpp.hasPrivileges(tree, PrivilegeConstants.JCR_WRITE));
            assertFalse(cpp.hasPrivileges(tree, PrivilegeConstants.REP_READ_NODES));
        }
    }

    @Test
    public void testHasPrivilegesOnRepo() throws Exception {
        assertFalse(cpp.hasPrivileges(null, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT));
    }


    @Test
    public void testIsGranted() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = root.getTree(p);
            PropertyState ps = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);

            assertFalse(cpp.isGranted(tree, null, Permissions.READ_NODE));
            assertFalse(cpp.isGranted(tree, ps, Permissions.READ_PROPERTY));

            assertFalse(cpp.isGranted(tree, null, Permissions.READ_NODE | Permissions.MODIFY_CHILD_NODE_COLLECTION));
            assertFalse(cpp.isGranted(tree, ps, Permissions.MODIFY_PROPERTY));

            assertFalse(cpp.isGranted(tree, null, Permissions.READ_ACCESS_CONTROL | Permissions.MODIFY_ACCESS_CONTROL));
            assertFalse(cpp.isGranted(tree, ps, Permissions.READ_ACCESS_CONTROL | Permissions.MODIFY_ACCESS_CONTROL));
        }
    }

    @Test
    public void testIsGrantedProperty() throws Exception {
        // TODO
    }

    @Test
    public void testIsGrantedAction() throws Exception {
        for (String nodePath : NODE_PATHS) {
            String propPath = PathUtils.concat(nodePath, JcrConstants.JCR_PRIMARYTYPE);
            String nonExisting = PathUtils.concat(nodePath, "nonExisting");

            assertFalse(cpp.isGranted(nodePath, Session.ACTION_REMOVE));
            assertFalse(cpp.isGranted(propPath, JackrabbitSession.ACTION_MODIFY_PROPERTY));

            assertFalse(cpp.isGranted(nodePath, Text.implode(new String[] {JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL, JackrabbitSession.ACTION_READ_ACCESS_CONTROL}, ",")));
            assertFalse(cpp.isGranted(nonExisting, JackrabbitSession.ACTION_ADD_PROPERTY));
            assertFalse(cpp.isGranted(nonExisting, Session.ACTION_ADD_NODE));
        }
    }

    @Test
    public void testRepositoryPermissionsIsGranted() throws Exception {
        RepositoryPermission rp = cpp.getRepositoryPermission();
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
    }

    @Test
    public void testGetTreePermission() throws Exception {
        TreePermission rootPermission = assertTreePermission(root.getTree("/"), TreePermission.EMPTY);
        TreePermission testPermission = assertTreePermission(root.getTree(TEST_PATH), rootPermission);
        assertTreePermission(root.getTree(TEST_CHILD_PATH), testPermission);
    }

    private TreePermission assertTreePermission(@Nonnull Tree tree, @Nonnull TreePermission parentPermission) {
        PropertyState ps = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);
        assertNotNull(ps);

        TreePermission treePermission = cpp.getTreePermission(tree, parentPermission);

        assertFalse(treePermission.isGranted(Permissions.ALL));
        assertFalse(treePermission.isGranted(Permissions.READ_NODE));
        assertFalse(treePermission.isGranted(Permissions.REMOVE_NODE));
        assertFalse(treePermission.isGranted(Permissions.REMOVE_NODE));

        assertFalse(treePermission.isGranted(Permissions.READ_PROPERTY, ps));
        assertFalse(treePermission.isGranted(Permissions.REMOVE_PROPERTY, ps));

        assertFalse(treePermission.canRead());
        assertFalse(treePermission.canRead(ps));
        assertFalse(treePermission.canReadAll());
        assertFalse(treePermission.canReadProperties());

        return treePermission;
    }

    /**
     * {@code AggregatedPermissionProvider} that doesn't grant any access.
     */
    private static final class EmptyAggregatedProvider implements AggregatedPermissionProvider {

        private static final PermissionProvider BASE = EmptyPermissionProvider.getInstance();
        private final Root root;

        private EmptyAggregatedProvider(@Nonnull Root root) {
            this.root = root;
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

        //-----------------------------------< AggregatedPermissionProvider >---
        @Nonnull
        @Override
        public PrivilegeBits supportedPrivileges(@Nullable Tree tree, @Nullable PrivilegeBits privilegeBits) {
            return (privilegeBits == null) ? new PrivilegeBitsProvider(root).getBits(PrivilegeConstants.JCR_ALL) : privilegeBits;
        }

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
            return false;
        }
    };
}