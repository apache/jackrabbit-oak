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

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test the effect of the combination of
 *
 * - default permission provider
 * - custom provider that
 *   > supports namespace-management and nodetype-def-mgt permission on repository level
 *   > write permission below {@link #TEST_A_PATH}.
 *
 * The tests are executed both for the set of principals associated with the test
 * user and with the admin session.
 *
 * At the repository level, the effective result is as follows:
 * - admin has full access at repo-level without namespace management (which is denied)
 * - test user has only nodetype-definition-mgt left as namespace management is
 *   denied by the custom provider.
 *
 * Below {@link #TEST_A_PATH} the effective result is as follows:
 * - any permissions not covered by the custom provider are defined by the default
 * - {@link Permissions#ADD_PROPERTY} and {@link Permissions#ADD_NODE} are denied
 *   for both admin and test user (due to deny in the custom provider)
 * - the other aggregates of the write permission are allowed if the default
 *   provider allows them (different for admin and test user).
 *
 * All paths outside of the scope of the custom provider have the following
 * characteristics:
 * - admin has full access (all permissions granted)
 * - effective permissions for the test user are define by the default setup.
 */
public class CompositeProviderScopeTest extends AbstractCompositeProviderTest {

    private static List<String> PATH_OUTSIDE_SCOPE = ImmutableList.of(ROOT_PATH, TEST_PATH, TEST_CHILD_PATH);

    private CompositePermissionProvider cppTestUser;
    private CompositePermissionProvider cppAdminUser;

    private LimitedScopeProvider testProvider;

    private PrivilegeBitsProvider pbp;
    private PrivilegeBits denied;


    @Override
    public void before() throws Exception {
        super.before();

        cppTestUser = createPermissionProvider(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        cppAdminUser = createPermissionProvider(root.getContentSession().getAuthInfo().getPrincipals());

        pbp = new PrivilegeBitsProvider(readOnlyRoot);
        denied = pbp.getBits(JCR_ADD_CHILD_NODES, REP_ADD_PROPERTIES);
    }

    @Override
    protected AggregatedPermissionProvider getTestPermissionProvider() {
        if (testProvider == null) {
            testProvider = new LimitedScopeProvider(readOnlyRoot);
        }
        return testProvider;
    }

    @Test
    public void testGetPrivileges() throws Exception {
        PrivilegeBitsProvider pbp = new PrivilegeBitsProvider(readOnlyRoot);

        for (String path : defPrivileges.keySet()) {
            Tree tree = readOnlyRoot.getTree(path);

            Set<String> defaultPrivs = defPrivileges.get(path);
            Set<String> privNames = cppTestUser.getPrivileges(tree);

            if (testProvider.isSupported(path)) {
                PrivilegeBits expected = pbp.getBits(defaultPrivs).modifiable().diff(denied).unmodifiable();
                assertEquals(expected, pbp.getBits(privNames));
            } else {
                assertEquals(path, defaultPrivs, privNames);
            }
        }
    }

    @Test
    public void testGetPrivilegesAdmin() throws Exception {
        for (String path : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(path);
            Set<String> privNames = cppAdminUser.getPrivileges(tree);

            if (testProvider.isSupported(path)) {
                PrivilegeBits expected = pbp.getBits(JCR_ALL).modifiable().diff(denied).unmodifiable();
                assertEquals(expected, pbp.getBits(privNames));
            } else {
                assertEquals(path, ImmutableSet.of(JCR_ALL), privNames);
            }
        }
    }

    @Test
    public void testGetPrivilegesOnRepo() throws Exception {
        Set<String> expected = ImmutableSet.of(JCR_NODE_TYPE_DEFINITION_MANAGEMENT);
        assertEquals(expected, cppTestUser.getPrivileges(null));
    }

    @Test
    public void testGetPrivilegesOnRepoAdmin() throws Exception {
        PrivilegeBits expected = pbp.getBits(JCR_ALL).modifiable().diff(pbp.getBits(JCR_NAMESPACE_MANAGEMENT)).unmodifiable();
        assertEquals(expected, pbp.getBits(cppAdminUser.getPrivileges(null)));
    }


    @Test
    public void testHasPrivileges() throws Exception {
        for (String path : defPrivileges.keySet()) {
            Set<String> defaultPrivs = defPrivileges.get(path);
            Tree tree = readOnlyRoot.getTree(path);

            if (testProvider.isSupported(path)) {
                Set<String> expected = pbp.getPrivilegeNames(pbp.getBits(defaultPrivs).modifiable().diff(denied));
                assertTrue(path, cppTestUser.hasPrivileges(tree, expected.toArray(new String[expected.size()])));

                assertFalse(path, cppTestUser.hasPrivileges(tree, JCR_ADD_CHILD_NODES));
                assertFalse(path, cppTestUser.hasPrivileges(tree, REP_ADD_PROPERTIES));
                assertFalse(path, cppTestUser.hasPrivileges(tree, JCR_MODIFY_PROPERTIES));
            } else {
                assertTrue(path, cppTestUser.hasPrivileges(tree, defaultPrivs.toArray(new String[defaultPrivs.size()])));
            }
        }
    }

    @Test
    public void testHasPrivilegesAdmin() throws Exception {
        Set<String> expectedAllowed = pbp.getPrivilegeNames(pbp.getBits(JCR_ALL).modifiable().diff(pbp.getBits(JCR_ADD_CHILD_NODES, REP_ADD_PROPERTIES)));
        for (String path : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(path);

            if (testProvider.isSupported(path)) {
                assertTrue(cppAdminUser.hasPrivileges(tree, expectedAllowed.toArray(new String[expectedAllowed.size()])));
                assertFalse(cppAdminUser.hasPrivileges(tree, JCR_ADD_CHILD_NODES));
                assertFalse(cppAdminUser.hasPrivileges(tree, REP_ADD_PROPERTIES));
                assertFalse(cppAdminUser.hasPrivileges(tree, JCR_WRITE));
            } else {
                assertTrue(cppAdminUser.hasPrivileges(tree, JCR_ALL));
            }
        }
    }

    @Test
    public void testHasPrivilegesOnRepo() throws Exception {
        assertFalse(cppTestUser.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT));
        assertFalse(cppTestUser.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(cppTestUser.hasPrivileges(null, JCR_ALL));

        assertTrue(cppTestUser.hasPrivileges(null, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        assertTrue(cppTestUser.hasPrivileges(null));
    }

    @Test
    public void testHasPrivilegeOnRepoAdmin() throws Exception {
        assertFalse(cppAdminUser.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT));
        assertFalse(cppAdminUser.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(cppAdminUser.hasPrivileges(null, JCR_ALL));

        assertTrue(cppAdminUser.hasPrivileges(null, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        Set<String> expected = pbp.getPrivilegeNames(pbp.getBits(JCR_ALL).modifiable().diff(pbp.getBits(JCR_NAMESPACE_MANAGEMENT)));
        assertTrue(cppAdminUser.hasPrivileges(null, expected.toArray(new String[expected.size()])));
        assertTrue(cppAdminUser.hasPrivileges(null));
    }

    @Test
    public void testIsGranted() throws Exception {
        for (String p : defPermissions.keySet()) {
            long defaultPerms = defPermissions.get(p);
            Tree tree = readOnlyRoot.getTree(p);

            if (testProvider.isSupported(p)) {
                long expected = Permissions.diff(defaultPerms, Permissions.ADD_NODE|Permissions.ADD_PROPERTY);
                assertTrue(cppTestUser.isGranted(tree, null, expected));

                assertFalse(cppTestUser.isGranted(tree, null, Permissions.ADD_NODE));
                assertFalse(cppTestUser.isGranted(tree, null, Permissions.ADD_PROPERTY));
                assertFalse(cppTestUser.isGranted(tree, null, Permissions.SET_PROPERTY));
                assertFalse(cppTestUser.isGranted(tree, null, Permissions.WRITE));
            } else {
                assertTrue(cppTestUser.isGranted(tree, null, defaultPerms));
            }
        }
    }

    @Test
    public void testIsGrantedAdmin() throws Exception {
        for (String path : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(path);

            if (testProvider.isSupported(path)) {
                assertTrue(cppAdminUser.isGranted(tree, null, Permissions.diff(Permissions.ALL, Permissions.ADD_NODE|Permissions.ADD_PROPERTY)));
                assertFalse(cppAdminUser.isGranted(tree, null, Permissions.ADD_NODE));
                assertFalse(cppAdminUser.isGranted(tree, null, Permissions.ADD_PROPERTY));
                assertFalse(cppAdminUser.isGranted(tree, null, Permissions.ADD_NODE | Permissions.ADD_PROPERTY));
                assertFalse(cppAdminUser.isGranted(tree, null, Permissions.WRITE));
            } else {
                assertTrue(cppAdminUser.isGranted(tree, null, Permissions.ALL));
            }
        }
    }

    @Test
    public void testIsGrantedProperty() throws Exception {
        for (String p : defPermissions.keySet()) {
            long defaultPerms = defPermissions.get(p);
            Tree tree = readOnlyRoot.getTree(p);

            if (testProvider.isSupported(p)) {
                long expected = Permissions.diff(defaultPerms, Permissions.ADD_NODE|Permissions.ADD_PROPERTY);
                assertTrue(cppTestUser.isGranted(tree, PROPERTY_STATE, expected));

                assertFalse(cppTestUser.isGranted(tree, PROPERTY_STATE, Permissions.ADD_PROPERTY));
                assertFalse(cppTestUser.isGranted(tree, PROPERTY_STATE, Permissions.SET_PROPERTY));
                assertFalse(cppTestUser.isGranted(tree, PROPERTY_STATE, Permissions.WRITE));
            } else {
                assertTrue(cppTestUser.isGranted(tree, PROPERTY_STATE, defaultPerms));
            }
        }
    }

    @Test
    public void testIsGrantedPropertyAdmin() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);

            if (testProvider.isSupported(p)) {
                assertTrue(cppAdminUser.isGranted(tree, PROPERTY_STATE, Permissions.diff(Permissions.ALL, Permissions.ADD_NODE|Permissions.ADD_PROPERTY)));
                assertFalse(cppAdminUser.isGranted(tree, PROPERTY_STATE, Permissions.ADD_NODE));
                assertFalse(cppAdminUser.isGranted(tree, PROPERTY_STATE, Permissions.ADD_PROPERTY));
                assertFalse(cppAdminUser.isGranted(tree, PROPERTY_STATE, Permissions.ADD_NODE | Permissions.ADD_PROPERTY));
                assertFalse(cppAdminUser.isGranted(tree, PROPERTY_STATE, Permissions.WRITE));
            } else {
                assertTrue(cppAdminUser.isGranted(tree, PROPERTY_STATE, Permissions.ALL));
            }
        }
    }

    @Test
    public void testIsGrantedAction() throws Exception {
        Set<String> denied = ImmutableSet.of(Session.ACTION_ADD_NODE, JackrabbitSession.ACTION_ADD_PROPERTY);

        for (String p : defActionsGranted.keySet()) {
            String[] actions = defActionsGranted.get(p);

            if (testProvider.isSupported(p)) {
                Set<String> expected = Sets.newHashSet(actions);
                expected.removeAll(denied);

                boolean canSetProperty = TreeLocation.create(readOnlyRoot, p).getProperty() != null;
                if (!canSetProperty) {
                    expected.remove(Session.ACTION_SET_PROPERTY);
                }

                assertTrue(p, cppTestUser.isGranted(p, getActionString(expected.toArray(new String[expected.size()]))));

                assertEquals(p, canSetProperty, cppTestUser.isGranted(p, Session.ACTION_SET_PROPERTY));
                assertFalse(p, cppTestUser.isGranted(p, Session.ACTION_ADD_NODE));
                assertFalse(p, cppTestUser.isGranted(p, JackrabbitSession.ACTION_ADD_PROPERTY));
            } else {
                assertTrue(p, cppTestUser.isGranted(p, getActionString(actions)));
            }
        }
    }

    @Test
    public void testIsGrantedActionAdmin() throws Exception {
        String[] grantedActions = new String[]{
                Session.ACTION_READ,
                JackrabbitSession.ACTION_REMOVE_NODE,
                JackrabbitSession.ACTION_MODIFY_PROPERTY,
                JackrabbitSession.ACTION_REMOVE_PROPERTY,
                Session.ACTION_REMOVE,
                JackrabbitSession.ACTION_READ_ACCESS_CONTROL,
                JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL,
                JackrabbitSession.ACTION_LOCKING,
                JackrabbitSession.ACTION_NODE_TYPE_MANAGEMENT,
                JackrabbitSession.ACTION_VERSIONING,
                JackrabbitSession.ACTION_USER_MANAGEMENT
        };

        for (String path : NODE_PATHS) {
            if (testProvider.isSupported(path)) {
                assertTrue(cppAdminUser.isGranted(path, getActionString(grantedActions)));
                assertFalse(cppAdminUser.isGranted(path, Session.ACTION_ADD_NODE));
                assertFalse(cppAdminUser.isGranted(path, Session.ACTION_SET_PROPERTY));
                assertFalse(cppAdminUser.isGranted(path, JackrabbitSession.ACTION_ADD_PROPERTY));
            } else {
                assertTrue(cppAdminUser.isGranted(path, Permissions.getString(Permissions.ALL)));
                assertTrue(cppAdminUser.isGranted(path, getActionString(ALL_ACTIONS)));
            }
        }
    }

    @Test
    public void testRepositoryPermissionIsGranted() throws Exception {
        RepositoryPermission rp = cppTestUser.getRepositoryPermission();

        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.PRIVILEGE_MANAGEMENT));

        assertTrue(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));

        assertFalse(rp.isGranted(Permissions.PRIVILEGE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT | Permissions.PRIVILEGE_MANAGEMENT | Permissions.WORKSPACE_MANAGEMENT));

        assertFalse(rp.isGranted(Permissions.ALL));

    }

    @Test
    public void testRepositoryPermissionIsGrantedAdmin() throws Exception {
        RepositoryPermission rp = cppAdminUser.getRepositoryPermission();

        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.PRIVILEGE_MANAGEMENT));

        assertTrue(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));

        assertTrue(rp.isGranted(Permissions.PRIVILEGE_MANAGEMENT));
        assertTrue(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT|Permissions.PRIVILEGE_MANAGEMENT|Permissions.WORKSPACE_MANAGEMENT));

        assertFalse(rp.isGranted(Permissions.ALL));
    }

    @Test
    public void testTreePermissionIsGranted() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = cppTestUser.getTreePermission(readOnlyRoot.getTree(path), parentPermission);

            Long toTest = (defPermissions.containsKey(path)) ? defPermissions.get(path) : defPermissions.get(PathUtils.getAncestorPath(path, 1));
            if (toTest != null) {
                if (testProvider.isSupported(path)) {
                    assertTrue(tp.isGranted(Permissions.diff(toTest, Permissions.ADD_NODE|Permissions.ADD_PROPERTY)));
                    assertFalse(tp.isGranted(Permissions.ADD_PROPERTY | Permissions.ADD_NODE));
                } else {
                    assertTrue(tp.isGranted(toTest));
                }
            }
            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsGrantedAdmin( ) {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = cppAdminUser.getTreePermission(readOnlyRoot.getTree(path), parentPermission);

            if (testProvider.isSupported(path)) {
                assertTrue(path, tp.isGranted(Permissions.diff(Permissions.ALL, Permissions.ADD_NODE|Permissions.ADD_PROPERTY)));
                assertFalse(path, tp.isGranted(Permissions.ADD_PROPERTY | Permissions.ADD_NODE));
                assertFalse(path, tp.isGranted(Permissions.ALL));
            } else {
                assertTrue(path, tp.isGranted(Permissions.ALL));
            }
            parentPermission = tp;
        }

        parentPermission = TreePermission.EMPTY;
        for (String nodePath : PATH_OUTSIDE_SCOPE) {
            Tree tree = readOnlyRoot.getTree(nodePath);

            TreePermission tp = cppAdminUser.getTreePermission(tree, parentPermission);
            assertTrue(nodePath, tp.isGranted(Permissions.ALL));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsGrantedProperty() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = cppTestUser.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            Long toTest = (defPermissions.containsKey(path)) ? defPermissions.get(path) : defPermissions.get(PathUtils.getAncestorPath(path, 1));
            if (toTest != null) {
                if (testProvider.isSupported(path)) {
                    assertTrue(tp.isGranted(Permissions.diff(toTest, Permissions.ADD_NODE|Permissions.ADD_PROPERTY), PROPERTY_STATE));
                    assertFalse(tp.isGranted(Permissions.ADD_PROPERTY, PROPERTY_STATE));
                } else {
                    assertTrue(tp.isGranted(toTest, PROPERTY_STATE));
                }
            }
            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanRead() throws Exception {
        Map<String, Boolean> readMap = ImmutableMap.<String, Boolean>builder().
                put(ROOT_PATH, false).
                put(TEST_PATH, true).
                put(TEST_A_PATH, true).
                put(TEST_A_B_PATH, true).
                put(TEST_A_B_C_PATH, false).
                put(TEST_A_B_C_PATH + "/nonexisting", false).
                build();

        TreePermission parentPermission = TreePermission.EMPTY;
        for (String nodePath : readMap.keySet()) {
            Tree tree = readOnlyRoot.getTree(nodePath);
            TreePermission tp = cppTestUser.getTreePermission(tree, parentPermission);

            boolean expectedResult = readMap.get(nodePath);
            assertEquals(nodePath, expectedResult, tp.canRead());

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadProperty() throws Exception {
        Map<String, Boolean> readMap = ImmutableMap.<String, Boolean>builder().
                put(ROOT_PATH, false).
                put(TEST_PATH, true).
                put(TEST_A_PATH, true).
                put(TEST_A_B_PATH, true).
                put(TEST_A_B_C_PATH, true).
                put(TEST_A_B_C_PATH + "/nonexisting", true).
                build();

        TreePermission parentPermission = TreePermission.EMPTY;
        for (String nodePath : readMap.keySet()) {
            Tree tree = readOnlyRoot.getTree(nodePath);

            TreePermission tp = cppTestUser.getTreePermission(tree, parentPermission);
            assertEquals(nodePath, readMap.get(nodePath), tp.canRead(PROPERTY_STATE));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadAdmin() {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String nodePath : TP_PATHS) {
            Tree tree = readOnlyRoot.getTree(nodePath);

            TreePermission tp = cppAdminUser.getTreePermission(tree, parentPermission);

            assertTrue(nodePath, tp.canRead());
            assertTrue(nodePath, tp.canRead(PROPERTY_STATE));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadAllAdmin() {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String nodePath : TP_PATHS) {
            Tree tree = readOnlyRoot.getTree(nodePath);

            TreePermission tp = cppAdminUser.getTreePermission(tree, parentPermission);

            assertFalse(nodePath, tp.canReadAll());
            assertTrue(nodePath, tp.canReadProperties());

            parentPermission = tp;
        }

        parentPermission = TreePermission.EMPTY;
        for (String nodePath : PATH_OUTSIDE_SCOPE) {
            Tree tree = readOnlyRoot.getTree(nodePath);

            TreePermission tp = cppAdminUser.getTreePermission(tree, parentPermission);
            assertFalse(nodePath, tp.canReadAll());
            assertTrue(nodePath, tp.canReadProperties());

            parentPermission = tp;
        }
    }
}