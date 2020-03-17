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

import java.lang.reflect.Field;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
 * default provider (i.e. the {@code NotSupportingProvider} is never respected
 * during evaluation).
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
        return new NoScopeProvider(root);
    }

    @Override
    @Test
    public void testGetTreePermissionInstance() throws Exception {
        PermissionProvider pp = createPermissionProviderOR();
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = pp.getTreePermission(t, parentPermission);
            assertCompositeTreePermission(t.isRoot(), tp);
            parentPermission = tp;
        }
    }

    @Override
    @Test
    public void testGetTreePermissionInstanceOR() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = pp.getTreePermission(t, parentPermission);
            assertCompositeTreePermission(t.isRoot(), tp);
            parentPermission = tp;
        }
    }

    @Override
    @Test
    public void testTreePermissionGetChild() throws Exception {
        List<String> childNames = ImmutableList.of("test", "a", "b", "c", "nonexisting");

        Tree rootTree = readOnlyRoot.getTree(ROOT_PATH);
        NodeState ns = ((ImmutableTree) rootTree).getNodeState();
        TreePermission tp = createPermissionProvider().getTreePermission(rootTree, TreePermission.EMPTY);
        assertCompositeTreePermission(tp);

        for (String cName : childNames) {
            ns = ns.getChildNode(cName);
            tp = tp.getChildPermission(cName, ns);
            assertCompositeTreePermission(false, tp);
        }
    }

    @Override
    @Test
    public void testTreePermissionGetChildOR() throws Exception {
        List<String> childNames = ImmutableList.of("test", "a", "b", "c", "nonexisting");

        Tree rootTree = readOnlyRoot.getTree(ROOT_PATH);
        NodeState ns = ((ImmutableTree) rootTree).getNodeState();
        TreePermission tp = createPermissionProviderOR().getTreePermission(rootTree, TreePermission.EMPTY);
        assertCompositeTreePermission(tp);

        for (String cName : childNames) {
            ns = ns.getChildNode(cName);
            tp = tp.getChildPermission(cName, ns);
            assertCompositeTreePermission(false, tp);
        }
    }

    @Test
    public void testGetPrivileges() throws Exception {
        for (String p : defPrivileges.keySet()) {
            Set<String> expected = defPrivileges.get(p);
            Tree tree = readOnlyRoot.getTree(p);

            assertEquals(p, expected, cppTestUser.getPrivileges(tree));
            assertEquals(p, defTestUser.getPrivileges(tree), cppTestUser.getPrivileges(tree));
        }
    }

    @Test
    public void testGetPrivilegesAdmin() throws Exception {
        Set<String> expected = ImmutableSet.of(JCR_ALL);
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);

            assertEquals(p, expected, cppAdminUser.getPrivileges(tree));
            assertEquals(p, defAdminUser.getPrivileges(tree), cppAdminUser.getPrivileges(tree));
        }
    }

    @Test
    public void testGetPrivilegesOnRepo() throws Exception {
        Set<String> expected = ImmutableSet.of(JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT);

        assertEquals(expected, cppTestUser.getPrivileges(null));
        assertEquals(defTestUser.getPrivileges(null), cppTestUser.getPrivileges(null));
    }

    @Test
    public void testGetPrivilegesOnRepoAdmin() throws Exception {
        Set<String> expected = ImmutableSet.of(JCR_ALL);

        assertEquals(expected, cppAdminUser.getPrivileges(null));
        assertEquals(defAdminUser.getPrivileges(null), cppAdminUser.getPrivileges(null));
    }


    @Test
    public void testHasPrivileges() throws Exception {
        for (String p : defPrivileges.keySet()) {
            Set<String> expected = defPrivileges.get(p);
            Tree tree = readOnlyRoot.getTree(p);

            String[] privNames = expected.toArray(new String[expected.size()]);
            assertTrue(p, cppTestUser.hasPrivileges(tree, privNames));
            assertEquals(p, defTestUser.hasPrivileges(tree, privNames), cppTestUser.hasPrivileges(tree, privNames));
        }
    }

    @Test
    public void testHasPrivilegesAdmin() throws Exception {
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(p, cppAdminUser.hasPrivileges(tree, JCR_ALL));
            assertTrue(p, defAdminUser.hasPrivileges(tree, JCR_ALL));
        }
    }

    @Test
    public void testHasPrivilegesOnRepo() throws Exception {
        assertTrue(cppTestUser.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        assertTrue(defTestUser.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));

        assertTrue(cppTestUser.hasPrivileges(null));
        assertTrue(defTestUser.hasPrivileges(null));
    }

    @Test
    public void testHasPrivilegeOnRepoAdminUser() throws Exception {
        assertTrue(cppAdminUser.hasPrivileges(null, JCR_ALL));
        assertTrue(defAdminUser.hasPrivileges(null, JCR_ALL));

        assertTrue(cppAdminUser.hasPrivileges(null));
        assertTrue(defAdminUser.hasPrivileges(null));
    }

    @Test
    public void testIsGranted() throws Exception {
        for (String p : defPermissions.keySet()) {
            long expected = defPermissions.get(p);
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(p, cppTestUser.isGranted(tree, null, expected));
            assertTrue(p, defTestUser.isGranted(tree, null, expected));
        }
    }

    @Test
    public void testIsGrantedAdmin() throws Exception {
        for (String p : defPermissions.keySet()) {
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(p, cppAdminUser.isGranted(tree, null, Permissions.ALL));
            assertTrue(p, defAdminUser.isGranted(tree, null, Permissions.ALL));
        }
    }

    @Test
    public void testIsGrantedProperty() throws Exception {
        for (String p : defPermissions.keySet()) {
            long expected = defPermissions.get(p);
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(p, cppTestUser.isGranted(tree, PROPERTY_STATE, expected));
            assertTrue(p, defTestUser.isGranted(tree, PROPERTY_STATE, expected));
        }
    }

    @Test
    public void testIsGrantedPropertyAdmin() throws Exception {
        for (String p : defPermissions.keySet()) {
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(p, cppAdminUser.isGranted(tree, PROPERTY_STATE, Permissions.ALL));
            assertTrue(p, defAdminUser.isGranted(tree, PROPERTY_STATE, Permissions.ALL));
        }
    }

    @Test
    public void testIsGrantedAction() throws Exception {
        for (String p : defActionsGranted.keySet()) {
            String actions = getActionString(defActionsGranted.get(p));
            assertTrue(p + " : " + actions, cppTestUser.isGranted(p, actions));
            assertTrue(p + " : " + actions, defTestUser.isGranted(p, actions));
        }
    }

    @Test
    public void testIsGrantedAction2() throws Exception {
        Map<String, String[]> noAccess = ImmutableMap.<String, String[]>builder().
                put(ROOT_PATH, new String[] {Session.ACTION_READ}).
                put(ROOT_PATH + "jcr:primaryType", new String[] {Session.ACTION_READ, Session.ACTION_SET_PROPERTY}).
                put("/nonexisting", new String[] {Session.ACTION_READ, Session.ACTION_ADD_NODE}).
                put(TEST_PATH_2, new String[] {Session.ACTION_READ, Session.ACTION_REMOVE}).
                put(TEST_PATH_2 + "/jcr:primaryType", new String[] {Session.ACTION_READ, Session.ACTION_SET_PROPERTY}).
                put(TEST_A_B_C_PATH, new String[] {Session.ACTION_READ, Session.ACTION_REMOVE}).
                put(TEST_A_B_C_PATH + "/noneExisting", new String[] {Session.ACTION_READ, JackrabbitSession.ACTION_REMOVE_NODE}).
                put(TEST_A_B_C_PATH + "/jcr:primaryType", new String[] {JackrabbitSession.ACTION_REMOVE_PROPERTY}).build();

        for (String p : noAccess.keySet()) {
            String actions = getActionString(noAccess.get(p));
            assertFalse(p, cppTestUser.isGranted(p, actions));
            assertFalse(p, defTestUser.isGranted(p, actions));
        }
    }

    @Test
    public void testIsGrantedActionAdmin() throws Exception {
        String allActions = getActionString(ALL_ACTIONS);
        for (String p : defActionsGranted.keySet()) {
            assertTrue(p + " : " + allActions, cppAdminUser.isGranted(p, allActions));
            assertTrue(p + " : " + allActions, defAdminUser.isGranted(p, allActions));
        }
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
    public void testTreePermissionIsGranted() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = cppTestUser.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            Long toTest = (defPermissions.containsKey(path)) ? defPermissions.get(path) : defPermissions.get(PathUtils.getAncestorPath(path, 1));
            if (toTest != null) {
                assertTrue(tp.isGranted(toTest));
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
                assertTrue(tp.isGranted(toTest, PROPERTY_STATE));
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
        TreePermission parentPermission2 = TreePermission.EMPTY;
        for (String nodePath : readMap.keySet()) {
            Tree tree = readOnlyRoot.getTree(nodePath);
            TreePermission tp = cppTestUser.getTreePermission(tree, parentPermission);
            TreePermission tp2 = defTestUser.getTreePermission(tree, parentPermission2);

            boolean expectedResult = readMap.get(nodePath);
            assertEquals(nodePath, expectedResult, tp.canRead());
            assertEquals(nodePath + "(default)", expectedResult, tp2.canRead());

            parentPermission = tp;
            parentPermission2 = tp2;
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
        TreePermission parentPermission2 = TreePermission.EMPTY;
        for (String nodePath : readMap.keySet()) {
            Tree tree = readOnlyRoot.getTree(nodePath);

            TreePermission tp = cppTestUser.getTreePermission(tree, parentPermission);
            TreePermission tp2 = defTestUser.getTreePermission(tree, parentPermission2);

            boolean expectedResult = readMap.get(nodePath);
            assertEquals(nodePath, expectedResult, tp.canRead(PROPERTY_STATE));
            assertEquals(nodePath + "(default)", expectedResult, tp2.canRead(PROPERTY_STATE));

            parentPermission = tp;
            parentPermission2 = tp2;
        }
    }

    @Test
    public void testTreePermissionCanReadAdmin() {
        TreePermission parentPermission = TreePermission.EMPTY;
        TreePermission parentPermission2 = TreePermission.EMPTY;

        for (String nodePath : TP_PATHS) {
            Tree tree = readOnlyRoot.getTree(nodePath);

            TreePermission tp = cppAdminUser.getTreePermission(tree, parentPermission);
            TreePermission tp2 = defAdminUser.getTreePermission(tree, parentPermission2);

            assertTrue(nodePath, tp.canRead());
            assertTrue(nodePath, tp.canRead(PROPERTY_STATE));

            assertTrue(nodePath + "(default)", tp2.canRead());
            assertTrue(nodePath + "(default)", tp2.canRead(PROPERTY_STATE));

            parentPermission = tp;
            parentPermission2 = tp2;
        }
    }

    @Test
    public void testTreePermissionSize() throws Exception {
        Field tpField = CompositeTreePermission.class.getDeclaredField("treePermissions");
        tpField.setAccessible(true);


        Tree rootTree = readOnlyRoot.getTree(ROOT_PATH);
        NodeState ns = ((ImmutableTree) rootTree).getNodeState();
        TreePermission tp = cppTestUser.getTreePermission(rootTree, TreePermission.EMPTY);
        assertEquals(2, ((TreePermission[]) tpField.get(tp)).length);

        List<String> childNames = ImmutableList.of("test", "a", "b", "c", "nonexisting");
        for (String cName : childNames) {
            ns = ns.getChildNode(cName);
            tp = tp.getChildPermission(cName, ns);

            assertCompositeTreePermission(false, tp);
        }
    }
}