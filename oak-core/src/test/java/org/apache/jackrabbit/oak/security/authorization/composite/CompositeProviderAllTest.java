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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Session;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.OpenPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test the effect of the 'AND' combination of
 *
 * - default permission provider (which a limited permission setup)
 * - custom provider that always grants full access and supports all permissions.
 *
 * for the {@link #getTestUser()}.
 *
 * The expected result is only the subset of permissions granted by the default
 * provider. The test user must never have full access anywhere.
 * <p>
 * Test the effect of the 'OR'ed combination of
 *
 * - default permission provider (which a limited permission setup)
 * - custom provider that always grants full access and supports all permissions.
 *
 * for the {@link #getTestUser()}.
 *
 * The expected result is the test user will have full access anywhere.

 */
public class CompositeProviderAllTest extends AbstractCompositeProviderTest {

    private CompositePermissionProvider cpp;
    private CompositePermissionProvider cppO;

    @Override
    public void before() throws Exception {
        super.before();

        cpp = createPermissionProvider(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        cppO = createPermissionProviderOR(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
    }

    @Override
    protected AggregatedPermissionProvider getTestPermissionProvider() {
        return new OpenAggregateProvider(root);
    }

    @Override
    @Test
    public void testHasPrivilegesJcrAllOR() throws Exception {
        PermissionProvider pp = createPermissionProviderOR();
        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);
            assertTrue(p, pp.hasPrivileges(tree, JCR_ALL));
        }
    }

    @Override
    @Test
    public void testIsGrantedAll() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        PermissionProvider ppo = createPermissionProviderOR();

        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);
            PropertyState ps = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);

            assertFalse(p, pp.isGranted(tree, null, Permissions.ALL));
            assertFalse(PathUtils.concat(p, JcrConstants.JCR_PRIMARYTYPE), pp.isGranted(tree, ps, Permissions.ALL));
            assertTrue(p, ppo.isGranted(tree, null, Permissions.ALL));
            assertTrue(PathUtils.concat(p, JcrConstants.JCR_PRIMARYTYPE), ppo.isGranted(tree, ps, Permissions.ALL));
        }
    }

    @Override
    @Test
    public void testHasPrivilegesOnRepoJcrAll() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        assertFalse(pp.hasPrivileges(null, JCR_ALL));
        PermissionProvider ppo = createPermissionProviderOR();
        assertTrue(ppo.hasPrivileges(null, JCR_ALL));
    }

    @Override
    @Test
    public void testIsNotGranted() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        PermissionProvider ppo = createPermissionProviderOR();

        for (String p : NODE_PATHS) {
            Tree tree = readOnlyRoot.getTree(p);
            PropertyState ps = tree.getProperty(JcrConstants.JCR_PRIMARYTYPE);

            assertFalse(p, pp.isGranted(tree, null, Permissions.MODIFY_ACCESS_CONTROL));
            assertFalse(PathUtils.concat(p, JcrConstants.JCR_PRIMARYTYPE), pp.isGranted(tree, ps, Permissions.MODIFY_ACCESS_CONTROL));
            assertTrue(p, ppo.isGranted(tree, null, Permissions.MODIFY_ACCESS_CONTROL));
            assertTrue(PathUtils.concat(p, JcrConstants.JCR_PRIMARYTYPE), ppo.isGranted(tree, ps, Permissions.MODIFY_ACCESS_CONTROL));
        }
    }

    @Override
    @Test
    public void testIsNotGrantedAction() throws Exception {
        PermissionProvider pp = createPermissionProvider();
        PermissionProvider ppo = createPermissionProviderOR();
        String[] actions = new String[]{JackrabbitSession.ACTION_LOCKING, JackrabbitSession.ACTION_MODIFY_ACCESS_CONTROL};

        for (String nodePath : NODE_PATHS) {
            String actionStr = getActionString(actions);
            assertFalse(nodePath, pp.isGranted(nodePath, actionStr));
            assertTrue(nodePath, ppo.isGranted(nodePath, actionStr));

            String propPath = PathUtils.concat(nodePath, JcrConstants.JCR_PRIMARYTYPE);
            assertFalse(propPath, pp.isGranted(propPath, actionStr));
            assertTrue(propPath, ppo.isGranted(propPath, actionStr));

            String nonExPath = PathUtils.concat(nodePath, "nonExisting");
            assertFalse(nonExPath, pp.isGranted(nonExPath, actionStr));
            assertTrue(nonExPath, ppo.isGranted(nonExPath, actionStr));
        }
    }

    @Override
    @Test
    public void testTreePermissionIsGrantedAllOR() throws Exception {
        PermissionProvider pp = createPermissionProviderOR();
        TreePermission parentPermission = TreePermission.EMPTY;

        PropertyState ps = PropertyStates.createProperty("propName", "val");

        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = pp.getTreePermission(t, parentPermission);

            assertTrue(tp.isGranted(Permissions.ALL));
            assertTrue(tp.isGranted(Permissions.ALL, ps));

            parentPermission = tp;
        }
    }

    @Override
    @Test
    public void testTreePermissionIsNotGrantedOR() throws Exception {
        PermissionProvider pp = createPermissionProviderOR();
        TreePermission parentPermission = TreePermission.EMPTY;

        PropertyState ps = PropertyStates.createProperty("propName", "val");

        for (String path : TP_PATHS) {
            Tree t = readOnlyRoot.getTree(path);
            TreePermission tp = pp.getTreePermission(t, parentPermission);

            assertFalse(tp.isGranted(Permissions.NO_PERMISSION));
            assertTrue(tp.isGranted(Permissions.MODIFY_ACCESS_CONTROL));
            assertFalse(tp.isGranted(Permissions.NO_PERMISSION, ps));
            assertTrue(tp.isGranted(Permissions.MODIFY_ACCESS_CONTROL, ps));

            parentPermission = tp;
        }
    }

    @Override
    @Test
    public void testTreePermissionCanReadPropertiesOR() throws Exception {
        PermissionProvider pp = createPermissionProviderOR();
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = pp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            assertTrue(tp.canReadProperties());

            parentPermission = tp;
        }
    }

    @Override
    @Test
    public void testRepositoryPermissionIsNotGrantedOR() throws Exception {
        RepositoryPermission rp = createPermissionProviderOR().getRepositoryPermission();
        assertTrue(rp.isGranted(Permissions.PRIVILEGE_MANAGEMENT));
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.PRIVILEGE_MANAGEMENT));
        assertTrue(rp.isGranted(Permissions.WORKSPACE_MANAGEMENT));
        assertTrue(rp.isGranted(Permissions.ALL));
        assertFalse(rp.isGranted(Permissions.NO_PERMISSION));
    }

    @Test
    public void testGetPrivileges() throws Exception {
        for (String p : defPrivileges.keySet()) {
            Set<String> expected = defPrivileges.get(p);
            Tree tree = root.getTree(p);
            assertEquals(p, expected, cpp.getPrivileges(tree));
            assertEquals(p, ImmutableSet.of(JCR_ALL), cppO.getPrivileges(tree));
        }
    }

    @Test
    public void testGetPrivilegesOnRepo() throws Exception {
        Set<String> privilegeNames = cpp.getPrivileges(null);
        assertEquals(ImmutableSet.of(JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT), privilegeNames);
        Set<String> privilegeNamesO = cppO.getPrivileges(null);
        assertEquals(ImmutableSet.of(JCR_ALL), privilegeNamesO);
    }


    @Test
    public void testHasPrivileges() throws Exception {
        for (String p : defPrivileges.keySet()) {
            Set<String> expected = defPrivileges.get(p);
            Tree tree = root.getTree(p);
            assertTrue(p, cpp.hasPrivileges(tree, expected.toArray(new String[expected.size()])));
            assertTrue(p, cppO.hasPrivileges(tree, expected.toArray(new String[expected.size()])));
        }
    }

    @Test
    public void testHasPrivilegesOnRepo() throws Exception {
        assertTrue(cpp.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
        assertTrue(cppO.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
    }

    @Test
    public void testIsGranted() throws Exception {
        for (String p : defPermissions.keySet()) {
            long expected = defPermissions.get(p);
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(p, cpp.isGranted(tree, null, expected));
            assertTrue(p, cppO.isGranted(tree, null, expected));
        }
    }

    @Test
    public void testIsGrantedProperty() throws Exception {
        for (String p : defPermissions.keySet()) {
            long expected = defPermissions.get(p);
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(p, cpp.isGranted(tree, PROPERTY_STATE, expected));
            assertTrue(p, cppO.isGranted(tree, PROPERTY_STATE, expected));
        }
    }

    @Test
    public void testIsGrantedAction() throws Exception {
        for (String p : defActionsGranted.keySet()) {
            String actionStr = getActionString(defActionsGranted.get(p));
            assertTrue(p + " : " + actionStr, cpp.isGranted(p, actionStr));
            assertTrue(p + " : " + actionStr, cppO.isGranted(p, actionStr));
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
            assertFalse(p, cpp.isGranted(p, getActionString(noAccess.get(p))));
            assertTrue(p, cppO.isGranted(p, getActionString(noAccess.get(p))));
        }
    }

    @Test
    public void testRepositoryPermissionsIsGranted() throws Exception {
        RepositoryPermission rp = cpp.getRepositoryPermission();
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertTrue(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));

        RepositoryPermission rpO = cpp.getRepositoryPermission();
        assertTrue(rpO.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertTrue(rpO.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertTrue(rpO.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
    }

    @Test
    public void testTreePermissionIsGranted() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = cpp.getTreePermission(root.getTree(path), parentPermission);
            Long toTest = (defPermissions.containsKey(path)) ? defPermissions.get(path) : defPermissions.get(PathUtils.getAncestorPath(path, 1));
            if (toTest != null) {
                assertTrue(tp.isGranted(toTest));
            }
            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsGrantedOR() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = cppO.getTreePermission(root.getTree(path), parentPermission);
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
            TreePermission tp = cpp.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
            Long toTest = (defPermissions.containsKey(path)) ? defPermissions.get(path) : defPermissions.get(PathUtils.getAncestorPath(path, 1));
            if (toTest != null) {
                assertTrue(tp.isGranted(toTest, PROPERTY_STATE));
            }
            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionIsGrantedPropertyOR() throws Exception {
        TreePermission parentPermission = TreePermission.EMPTY;

        for (String path : TP_PATHS) {
            TreePermission tp = cppO.getTreePermission(readOnlyRoot.getTree(path), parentPermission);
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
        for (String nodePath : readMap.keySet()) {
            Tree tree = readOnlyRoot.getTree(nodePath);
            TreePermission tp = cpp.getTreePermission(tree, parentPermission);

            boolean expectedResult = readMap.get(nodePath);
            assertEquals(nodePath, expectedResult, tp.canRead());

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadOR() throws Exception {
        Map<String, Boolean> readMap = ImmutableMap.<String, Boolean>builder().
                put(ROOT_PATH, true).
                put(TEST_PATH, true).
                put(TEST_A_PATH, true).
                put(TEST_A_B_PATH, true).
                put(TEST_A_B_C_PATH, true).
                put(TEST_A_B_C_PATH + "/nonexisting", true).
                build();

        TreePermission parentPermission = TreePermission.EMPTY;
        for (String nodePath : readMap.keySet()) {
            Tree tree = readOnlyRoot.getTree(nodePath);
            TreePermission tp = cppO.getTreePermission(tree, parentPermission);

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

            TreePermission tp = cpp.getTreePermission(tree, parentPermission);

            boolean expectedResult = readMap.get(nodePath);
            assertEquals(nodePath, expectedResult, tp.canRead(PROPERTY_STATE));

            parentPermission = tp;
        }
    }

    @Test
    public void testTreePermissionCanReadPropertyOR() throws Exception {
        Map<String, Boolean> readMap = ImmutableMap.<String, Boolean>builder().
                put(ROOT_PATH, true).
                put(TEST_PATH, true).
                put(TEST_A_PATH, true).
                put(TEST_A_B_PATH, true).
                put(TEST_A_B_C_PATH, true).
                put(TEST_A_B_C_PATH + "/nonexisting", true).
                build();

        TreePermission parentPermission = TreePermission.EMPTY;
        for (String nodePath : readMap.keySet()) {
            Tree tree = readOnlyRoot.getTree(nodePath);

            TreePermission tp = cppO.getTreePermission(tree, parentPermission);

            boolean expectedResult = readMap.get(nodePath);
            assertEquals(nodePath, expectedResult, tp.canRead(PROPERTY_STATE));

            parentPermission = tp;
        }
    }

    /**
     * Custom permission provider that supports all permissions and grants
     * full access for everyone.
     */
    private static final class OpenAggregateProvider extends AbstractAggrProvider {

        private static final PermissionProvider BASE = OpenPermissionProvider.getInstance();

        private OpenAggregateProvider(@Nonnull Root root) {
            super(root);
        }

        //-----------------------------------< AggregatedPermissionProvider >---

        @Override
        public boolean isGranted(@Nonnull TreeLocation location, long permissions) {
            return true;
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
    }
}