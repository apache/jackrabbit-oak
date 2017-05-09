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

import java.util.Map;
import java.util.Set;

import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
 * - custom provider that grants JCR_NAMESPACE_MANAGEMENT on repository level
 *   and REP_READ_NODES only
 *
 * both for the set of principals associated with the test user and with the admin session.
 * The expected outcome is that
 * - test user can only read nodes where this is also granted by the default provider
 *   but has no other access granted
 * - admin user can only read nodes and register namespaces
 */
public class CompositeProviderFullScopeTest extends AbstractCompositeProviderTest {

    private CompositePermissionProvider cppTestUser;
    private CompositePermissionProvider cppAdminUser;

    @Override
    public void before() throws Exception {
        super.before();

        cppTestUser = createPermissionProvider(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        cppAdminUser = createPermissionProvider(root.getContentSession().getAuthInfo().getPrincipals());
    }

    @Override
    protected AggregatedPermissionProvider getTestPermissionProvider() {
        return new FullScopeProvider(readOnlyRoot);
    }

    @Test
    public void testGetPrivileges() throws Exception {
        PrivilegeBitsProvider pbp = new PrivilegeBitsProvider(readOnlyRoot);
        PrivilegeBits readNodes = pbp.getBits(REP_READ_NODES);
        Set<String> expected = ImmutableSet.of(REP_READ_NODES);

        for (String path : defPrivileges.keySet()) {
            Set<String> defaultPrivs = defPrivileges.get(path);
            Tree tree = readOnlyRoot.getTree(path);

            Set<String> privNames = cppTestUser.getPrivileges(tree);
            if (pbp.getBits(defaultPrivs).includes(readNodes)) {
                assertEquals(expected, privNames);
            } else {
                assertTrue(privNames.isEmpty());
            }
        }
    }

    @Test
    public void testGetPrivilegesAdmin() throws Exception {
        Set<String> expected = ImmutableSet.of(REP_READ_NODES);

        for (String path : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(path);
            assertEquals(expected, cppAdminUser.getPrivileges(tree));
        }
    }

    @Test
    public void testGetPrivilegesOnRepo() throws Exception {
        Set<String> expected = ImmutableSet.of(JCR_NAMESPACE_MANAGEMENT);
        assertEquals(expected, cppTestUser.getPrivileges(null));
    }

    @Test
    public void testGetPrivilegesOnRepoAdmin() throws Exception {
        Set<String> expected = ImmutableSet.of(JCR_NAMESPACE_MANAGEMENT);
        assertEquals(expected, cppAdminUser.getPrivileges(null));
    }

    @Test
    public void testHasPrivileges() throws Exception {
        PrivilegeBitsProvider pbp = new PrivilegeBitsProvider(readOnlyRoot);
        PrivilegeBits readNodes = pbp.getBits(REP_READ_NODES);

        for (String path : defPrivileges.keySet()) {
            Set<String> defaultPrivs = defPrivileges.get(path);
            PrivilegeBits defaultBits = pbp.getBits(defaultPrivs);
            Tree tree = readOnlyRoot.getTree(path);

            if (defaultPrivs.isEmpty()) {
                assertFalse(path, cppTestUser.hasPrivileges(tree, REP_READ_NODES));
            } else if (defaultBits.includes(readNodes)) {
                assertTrue(path, cppTestUser.hasPrivileges(tree, REP_READ_NODES));
                if (!readNodes.equals(defaultBits)) {
                    assertFalse(path, cppTestUser.hasPrivileges(tree, defaultPrivs.toArray(new String[defaultPrivs.size()])));
                }
            } else {
                assertFalse(path, cppTestUser.hasPrivileges(tree, REP_READ_NODES));
                assertFalse(path, cppTestUser.hasPrivileges(tree, defaultPrivs.toArray(new String[defaultPrivs.size()])));
            }
        }
    }

    @Test
    public void testHasPrivilegesAdmin() throws Exception {
        for (String path : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(path);

            assertTrue(cppAdminUser.hasPrivileges(tree, REP_READ_NODES));

            assertFalse(cppAdminUser.hasPrivileges(tree, JCR_READ));
            assertFalse(cppAdminUser.hasPrivileges(tree, JCR_ALL));
            assertFalse(cppAdminUser.hasPrivileges(tree, JCR_WRITE));
            assertFalse(cppAdminUser.hasPrivileges(tree, REP_READ_NODES, REP_READ_PROPERTIES));
            assertFalse(cppAdminUser.hasPrivileges(tree, JCR_MODIFY_PROPERTIES));
            assertFalse(cppAdminUser.hasPrivileges(tree, JCR_LOCK_MANAGEMENT));
        }
    }

    @Test
    public void testHasPrivilegesOnRepo() throws Exception {
        assertTrue(cppTestUser.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT));

        assertFalse(cppTestUser.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(cppTestUser.hasPrivileges(null, JCR_ALL));

        assertTrue(cppTestUser.hasPrivileges(null));
    }

    @Test
    public void testHasPrivilegeOnRepoAdmin() throws Exception {
        assertTrue(cppAdminUser.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT));

        assertFalse(cppAdminUser.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(cppAdminUser.hasPrivileges(null, JCR_ALL));

        assertTrue(cppAdminUser.hasPrivileges(null));
    }

    @Test
    public void testIsGranted() throws Exception {
        for (String p : defPermissions.keySet()) {
            long defaultPerms = defPermissions.get(p);
            Tree tree = readOnlyRoot.getTree(p);

            if (Permissions.READ_NODE != defaultPerms) {
                assertFalse(p, cppTestUser.isGranted(tree, null, defaultPerms));
            }

            boolean expectedReadNode = Permissions.includes(defaultPerms, Permissions.READ_NODE);
            assertEquals(p, expectedReadNode, cppTestUser.isGranted(tree, null, Permissions.READ_NODE));
        }
    }

    @Test
    public void testIsGrantedAdmin() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(p, cppAdminUser.isGranted(tree, null, Permissions.READ_NODE));

            assertFalse(p, cppAdminUser.isGranted(tree, null, Permissions.READ));
            assertFalse(p, cppAdminUser.isGranted(tree, null, Permissions.WRITE));
            assertFalse(p, cppAdminUser.isGranted(tree, null, Permissions.ALL));
        }
    }

    @Test
    public void testIsGrantedProperty() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);

            assertFalse(p, cppTestUser.isGranted(tree, PROPERTY_STATE, Permissions.READ_PROPERTY));
            assertFalse(p, cppTestUser.isGranted(tree, PROPERTY_STATE, Permissions.SET_PROPERTY));
        }
    }

    @Test
    public void testIsGrantedPropertyAdmin() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);

            assertFalse(p, cppAdminUser.isGranted(tree, PROPERTY_STATE, Permissions.READ_PROPERTY));
            assertFalse(p, cppAdminUser.isGranted(tree, PROPERTY_STATE, Permissions.SET_PROPERTY));
            assertFalse(p, cppAdminUser.isGranted(tree, PROPERTY_STATE, Permissions.ALL));
        }
    }

    @Test
    public void testIsGrantedAction() throws Exception {
        for (String p : defActionsGranted.keySet()) {
            String[] actions = defActionsGranted.get(p);

            if (ImmutableList.copyOf(actions).contains(Session.ACTION_READ)) {
                TreeLocation tl = TreeLocation.create(readOnlyRoot, p);
                assertEquals(p, tl.getTree() != null, cppTestUser.isGranted(p, Session.ACTION_READ));
            } else {
                assertFalse(p, cppTestUser.isGranted(p, Session.ACTION_READ));
            }

            if (actions.length > 1) {
                assertFalse(p, cppTestUser.isGranted(p, getActionString(actions)));
            }
        }
    }

    @Test
    public void testIsGrantedAction2() throws Exception {
        Map<String, String[]> noAccess = ImmutableMap.<String, String[]>builder().
                put(ROOT_PATH, new String[]{Session.ACTION_READ}).
                put(ROOT_PATH + "jcr:primaryType", new String[]{Session.ACTION_READ, Session.ACTION_SET_PROPERTY}).
                put("/nonexisting", new String[]{Session.ACTION_READ, Session.ACTION_ADD_NODE}).
                put(TEST_PATH_2, new String[]{Session.ACTION_READ, Session.ACTION_REMOVE}).
                put(TEST_PATH_2 + "/jcr:primaryType", new String[]{Session.ACTION_READ, Session.ACTION_SET_PROPERTY}).
                put(TEST_A_B_C_PATH, new String[]{Session.ACTION_READ, Session.ACTION_REMOVE}).
                put(TEST_A_B_C_PATH + "/noneExisting", new String[]{Session.ACTION_READ, JackrabbitSession.ACTION_REMOVE_NODE}).
                put(TEST_A_B_C_PATH + "/jcr:primaryType", new String[]{JackrabbitSession.ACTION_REMOVE_PROPERTY}).build();

        for (String p : noAccess.keySet()) {
            assertFalse(p, cppTestUser.isGranted(p, getActionString(noAccess.get(p))));
        }
    }

    @Test
    public void testIsGrantedActionAdmin() throws Exception {
        for (String p : defActionsGranted.keySet()) {
            boolean expectedRead = readOnlyRoot.getTree(p).exists();
            assertEquals(p, expectedRead, cppAdminUser.isGranted(p, Session.ACTION_READ));
            assertFalse(p, cppAdminUser.isGranted(p, getActionString(ALL_ACTIONS)));
        }
    }


    @Test
    public void testRepositoryPermissionIsGranted() throws Exception {
        RepositoryPermission rp = cppTestUser.getRepositoryPermission();
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));

        assertFalse(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
    }

    @Test
    public void testRepositoryPermissionIsGrantedAdminUser() throws Exception {
        RepositoryPermission rp = cppAdminUser.getRepositoryPermission();
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));

        assertFalse(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.PRIVILEGE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT|Permissions.PRIVILEGE_MANAGEMENT));
        assertFalse(rp.isGranted(Permissions.ALL));
    }

    @Test
    public void testTreePermissionIsGranted() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = cppTestUser.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            Long toTest = (defPermissions.containsKey(path)) ? defPermissions.get(path) : defPermissions.get(PathUtils.getAncestorPath(path, 1));
            if (toTest != null) {
                if (Permissions.READ_NODE == toTest) {
                    assertTrue(path, tp.isGranted(toTest));
                } else {
                    boolean canRead = Permissions.includes(toTest, Permissions.READ_NODE);
                    assertEquals(path, canRead, tp.isGranted(Permissions.READ_NODE));
                    assertFalse(path, tp.isGranted(toTest));
                }
            }
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
                boolean granted = (toTest == Permissions.READ_NODE);
                assertEquals(path, granted, tp.isGranted(toTest, PROPERTY_STATE));
            }
            assertFalse(tp.isGranted(Permissions.READ_PROPERTY, PROPERTY_STATE));
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
        TreePermission parentPermission = TreePermission.EMPTY;
        for (String nodePath : TP_PATHS) {
            Tree tree = readOnlyRoot.getTree(nodePath);

            TreePermission tp = cppTestUser.getTreePermission(tree, parentPermission);
            assertFalse(nodePath, tp.canRead(PROPERTY_STATE));

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
            assertFalse(nodePath, tp.canRead(PROPERTY_STATE));

            parentPermission = tp;
        }
    }
}