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
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
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
        assertEquals(ImmutableSet.of(JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT), privilegeNames);
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
        assertTrue(cpp.hasPrivileges(null, JCR_NAMESPACE_MANAGEMENT, JCR_NODE_TYPE_DEFINITION_MANAGEMENT));
    }


    @Test
    public void testIsGranted() throws Exception {
        for (String p : defPermissions.keySet()) {
            long expected = defPermissions.get(p);
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(p, cpp.isGranted(tree, null, expected));
        }
    }

    @Test
    public void testIsGrantedProperty() throws Exception {
        for (String p : defPermissions.keySet()) {
            long expected = defPermissions.get(p);
            Tree tree = readOnlyRoot.getTree(p);

            assertTrue(p, cpp.isGranted(tree, PROPERTY_STATE, expected));
        }
    }

    @Test
    public void testIsGrantedAction() throws Exception {
        for (String p : defActionsGranted.keySet()) {
            String actionStr = getActionString(defActionsGranted.get(p));
            assertTrue(p + " : " + actionStr, cpp.isGranted(p, actionStr));
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
        }
    }

    @Test
    public void testRepositoryPermissionsIsGranted() throws Exception {
        RepositoryPermission rp = cpp.getRepositoryPermission();
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT));
        assertTrue(rp.isGranted(Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
        assertTrue(rp.isGranted(Permissions.NAMESPACE_MANAGEMENT | Permissions.NODE_TYPE_DEFINITION_MANAGEMENT));
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