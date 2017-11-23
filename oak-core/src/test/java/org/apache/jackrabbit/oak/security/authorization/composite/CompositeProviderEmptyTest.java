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
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test the effect of the 'AND' combination of
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
 * <p>
 * Test the effect of the 'OR' combination of
 *
 * - default permission provider, which always grants full access to an administrative session.
 * - custom provider that prevents all access but supports all permissions and
 *   thus is always called during composite evaluation.
 *
 * The tests are executed with the set of principals associated with the admin session,
 * which in the default permission provider is granted full access.
 *
 * The expected outcome is that the combination effectively allows any access.
 */
public class CompositeProviderEmptyTest extends AbstractCompositeProviderTest {

    private CompositePermissionProvider cpp;
    private CompositePermissionProvider cppO;

    @Override
    public void before() throws Exception {
        super.before();
        cpp = createPermissionProvider(root.getContentSession().getAuthInfo().getPrincipals());
        cppO = createPermissionProviderOR(root.getContentSession().getAuthInfo().getPrincipals());
    }

    @Override
    protected AggregatedPermissionProvider getTestPermissionProvider() {
        return new EmptyAggregatedProvider(root);
    }

    @Test
    public void testGetPrivileges() throws Exception {
        for (String p : NODE_PATHS) {
            assertTrue(cpp.getPrivileges(readOnlyRoot.getTree(p)).isEmpty());
            assertEquals(ImmutableSet.of(JCR_ALL), cppO.getPrivileges(readOnlyRoot.getTree(p)));
        }
    }

    @Test
    public void testGetPrivilegesOnRepo() throws Exception {
        assertTrue(cpp.getPrivileges(null).isEmpty());
        assertEquals(ImmutableSet.of(JCR_ALL), cppO.getPrivileges(null));
    }

    @Test
    public void testHasPrivileges() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);

            assertFalse(cpp.hasPrivileges(tree, JCR_READ));
            assertFalse(cpp.hasPrivileges(tree, JCR_WRITE));
            assertFalse(cpp.hasPrivileges(tree, REP_READ_NODES));
        }
    }

    @Test
    public void testHasPrivilegesOnRepo() throws Exception {
        assertFalse(cpp.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        assertTrue(cppO.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
    }

    @Test
    public void testIsGranted() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);

            assertFalse(cpp.isGranted(tree, null, Permissions.READ_NODE));
            assertFalse(cpp.isGranted(tree, null, Permissions.READ_NODE | Permissions.MODIFY_CHILD_NODE_COLLECTION));
            assertFalse(cpp.isGranted(tree, null, Permissions.READ_ACCESS_CONTROL | Permissions.MODIFY_ACCESS_CONTROL));
            assertTrue(cppO.isGranted(tree, null, Permissions.READ_NODE));
            assertTrue(cppO.isGranted(tree, null, Permissions.READ_NODE | Permissions.MODIFY_CHILD_NODE_COLLECTION));
            assertTrue(cppO.isGranted(tree, null, Permissions.READ_ACCESS_CONTROL | Permissions.MODIFY_ACCESS_CONTROL));
        }
    }

    @Test
    public void testIsGrantedProperty() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);

            assertFalse(cpp.isGranted(tree, PROPERTY_STATE, Permissions.READ_PROPERTY));
            assertFalse(cpp.isGranted(tree, PROPERTY_STATE, Permissions.MODIFY_PROPERTY));
            assertFalse(cpp.isGranted(tree, PROPERTY_STATE, Permissions.ADD_PROPERTY));
            assertFalse(cpp.isGranted(tree, PROPERTY_STATE, Permissions.REMOVE_PROPERTY));
            assertFalse(cpp.isGranted(tree, PROPERTY_STATE, Permissions.READ_ACCESS_CONTROL | Permissions.MODIFY_ACCESS_CONTROL));
            assertTrue(cppO.isGranted(tree, PROPERTY_STATE, Permissions.READ_PROPERTY));
            assertTrue(cppO.isGranted(tree, PROPERTY_STATE, Permissions.MODIFY_PROPERTY));
            assertTrue(cppO.isGranted(tree, PROPERTY_STATE, Permissions.ADD_PROPERTY));
            assertTrue(cppO.isGranted(tree, PROPERTY_STATE, Permissions.REMOVE_PROPERTY));
            assertTrue(cppO.isGranted(tree, PROPERTY_STATE, Permissions.READ_ACCESS_CONTROL | Permissions.MODIFY_ACCESS_CONTROL));
        }
    }

    @Test
    public void testIsGrantedAction() throws Exception {
        for (String nodePath : NODE_PATHS) {
            String propPath = PathUtils.concat(nodePath, JcrConstants.JCR_PRIMARYTYPE);
            String nonExisting = PathUtils.concat(nodePath, "nonExisting");

            assertFalse(cpp.isGranted(nodePath, Session.ACTION_REMOVE));
            assertFalse(cpp.isGranted(propPath, JackrabbitSession.ACTION_MODIFY_PROPERTY));
            assertFalse(cpp.isGranted(nodePath, getActionString(JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL, JackrabbitSession.ACTION_READ_ACCESS_CONTROL)));
            assertFalse(cpp.isGranted(nonExisting, JackrabbitSession.ACTION_ADD_PROPERTY));
            assertFalse(cpp.isGranted(nonExisting, Session.ACTION_ADD_NODE));
            assertTrue(cppO.isGranted(nodePath, Session.ACTION_REMOVE));
            assertTrue(cppO.isGranted(propPath, JackrabbitSession.ACTION_MODIFY_PROPERTY));
            assertTrue(cppO.isGranted(nodePath, getActionString(JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL, JackrabbitSession.ACTION_READ_ACCESS_CONTROL)));
            assertTrue(cppO.isGranted(nonExisting, JackrabbitSession.ACTION_ADD_PROPERTY));
            assertTrue(cppO.isGranted(nonExisting, Session.ACTION_ADD_NODE));
        }
    }

    @Test
    public void testRepositoryPermissionsIsGranted() throws Exception {
        RepositoryPermission rp = cpp.getRepositoryPermission();
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        RepositoryPermission rpO = cppO.getRepositoryPermission();
        assertTrue(rpO.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertTrue(rpO.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
    }

    @Test
    public void testTreePermissionIsGranted() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;
        for (String path : TP_PATHS) {
            TreePermission tp = cpp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            assertFalse(tp.isGranted(Permissions.READ_NODE));
            assertFalse(tp.isGranted(Permissions.REMOVE_NODE));
            assertFalse(tp.isGranted(Permissions.ALL));
            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsGrantedOR() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;
        for (String path : TP_PATHS) {
            TreePermission tp = cppO.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            assertTrue(tp.isGranted(Permissions.READ_NODE));
            assertTrue(tp.isGranted(Permissions.REMOVE_NODE));
            assertTrue(tp.isGranted(Permissions.ALL));
            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsGrantedProperty() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;
        for (String path : TP_PATHS) {
            TreePermission tp = cpp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            assertFalse(tp.isGranted(Permissions.READ_PROPERTY, PROPERTY_STATE));
            assertFalse(tp.isGranted(Permissions.REMOVE_PROPERTY, PROPERTY_STATE));
            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsGrantedPropertyOR() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;
        for (String path : TP_PATHS) {
            TreePermission tp = cppO.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            assertTrue(tp.isGranted(Permissions.READ_PROPERTY, PROPERTY_STATE));
            assertTrue(tp.isGranted(Permissions.REMOVE_PROPERTY, PROPERTY_STATE));
            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanRead() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;
        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = cpp.getTreePermission(t, parentPermission);
            assertFalse(tp.canRead());
            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadOR() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;
        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = cppO.getTreePermission(t, parentPermission);
            assertTrue(tp.canRead());
            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadProperty() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;
        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = cpp.getTreePermission(t, parentPermission);
            assertFalse(tp.canRead(PROPERTY_STATE));
            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadPropertyOR() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;
        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = cppO.getTreePermission(t, parentPermission);
            assertTrue(tp.canRead(PROPERTY_STATE));
            parentPermission = tp;
        }
    }

    /**
     * {@code AggregatedPermissionProvider} that doesn't grant any access.
     */
    static class EmptyAggregatedProvider extends AbstractAggrProvider {

        private static final PermissionProvider BASE = EmptyPermissionProvider.getInstance();

        public EmptyAggregatedProvider(@Nonnull Root root) {
            super(root);
        }

        //---------------------------------------------< PermissionProvider >---
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
        @Override
        public boolean isGranted(@Nonnull TreeLocation location, long permissions) {
            return false;
        }
    }
}