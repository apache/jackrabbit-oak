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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.jcr.AccessDeniedException;
import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CugPermissionProviderTest extends AbstractCugTest implements NodeTypeConstants {

    private static final String TEST_GROUP_ID = "testGroupForCugTest";

    private Principal testGroupPrincipal;
    private CugPermissionProvider cugPermProvider;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        Group testGroup = getUserManager(root).createGroup(TEST_GROUP_ID);
        root.commit();

        // add more child nodes
        NodeUtil n = new NodeUtil(root.getTree(SUPPORTED_PATH));
        n.addChild("a", NT_OAK_UNSTRUCTURED).addChild("b", NT_OAK_UNSTRUCTURED).addChild("c", NT_OAK_UNSTRUCTURED);
        n.addChild("aa", NT_OAK_UNSTRUCTURED).addChild("bb", NT_OAK_UNSTRUCTURED).addChild("cc", NT_OAK_UNSTRUCTURED);

        testGroupPrincipal = testGroup.getPrincipal();
        createCug("/content/a", testGroupPrincipal);
        createCug("/content/a/b/c", EveryonePrincipal.getInstance());
        createCug("/content/aa/bb", testGroupPrincipal);

        root.commit();

        Set<Principal> principals = ImmutableSet.of(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        Set<String> supportedPaths = ImmutableSet.of(SUPPORTED_PATH);

        cugPermProvider = new CugPermissionProvider(root, principals, supportedPaths, CugContext.INSTANCE);
    }

    @Override
    public void after() throws Exception {
        try {
            // revert transient pending changes (that might be invalid)
            root.refresh();

            // remove the test group
            Authorizable testGroup = getUserManager(root).getAuthorizable(TEST_GROUP_ID);
            if (testGroup != null) {
                testGroup.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testHandlesPath() {
        Map<String, Boolean> pathMap = new HashMap<String, Boolean>();
        pathMap.put(SUPPORTED_PATH, false);
        pathMap.put("/content/a", true);
        pathMap.put("/content/a/rep:cugPolicy", true);
        pathMap.put("/content/a/b", true);
        pathMap.put("/content/a/b/c/jcr:primaryType", true);
        pathMap.put("/content/aa", false);
        pathMap.put("/content/aa/bb/cc", true);
        // paths that may not contain cugs anyway
        pathMap.put(NODE_TYPES_PATH, false);
        pathMap.put("/", false);
        pathMap.put(UNSUPPORTED_PATH, false);
        pathMap.put(INVALID_PATH, false);

        for (String path : pathMap.keySet()) {
            boolean expected = pathMap.get(path);

            assertEquals(path, expected, cugPermProvider.handles(path, Session.ACTION_READ));
            assertFalse(cugPermProvider.handles(path, Session.ACTION_ADD_NODE));
        }
    }

    @Test
    public void testHandlesTree() {
        Map<Tree, Boolean> pathMap = new HashMap<Tree, Boolean>();
        pathMap.put(root.getTree(SUPPORTED_PATH), false);
        pathMap.put(root.getTree("/content/a"), true);
        pathMap.put(root.getTree("/content/a/rep:cugPolicy"), true);
        pathMap.put(root.getTree("/content/a/b"), true);
        pathMap.put(root.getTree("/content/a/b/c/jcr:primaryType"), true);
        pathMap.put(root.getTree("/content/aa"), false);
        pathMap.put(root.getTree("/content/aa/bb/cc"), true);
        // paths that may not contain cugs anyway
        pathMap.put(root.getTree(NODE_TYPES_PATH), false);
        pathMap.put(root.getTree("/"), false);
        pathMap.put(root.getTree(UNSUPPORTED_PATH), false);
        pathMap.put(root.getTree(INVALID_PATH), false);

        for (Tree tree : pathMap.keySet()) {
            boolean expected = pathMap.get(tree);

            assertEquals(tree.getPath(), expected, cugPermProvider.handles(tree, PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ)));
            assertEquals(tree.getPath(), expected, cugPermProvider.handles(tree, Permissions.READ));
            assertEquals(tree.getPath(), expected, cugPermProvider.handles(tree, Permissions.READ_PROPERTY));
            assertEquals(tree.getPath(), expected, cugPermProvider.handles(tree, Permissions.READ_NODE));

            assertFalse(cugPermProvider.handles(tree, Permissions.ADD_NODE|Permissions.REMOVE));
        }
    }

    @Test
    public void testHandlesTreePermission() {
        TreePermission rootTp = cugPermProvider.getTreePermission(root.getTree("/"), TreePermission.EMPTY);

        Map<TreePermission, Boolean> tpMap = new HashMap<TreePermission, Boolean>();

        TreePermission contentTp = cugPermProvider.getTreePermission(root.getTree(SUPPORTED_PATH), rootTp);
        tpMap.put(contentTp, false);

        TreePermission aTp = cugPermProvider.getTreePermission(root.getTree("/content/a"), contentTp);
        tpMap.put(aTp, true);
        tpMap.put(cugPermProvider.getTreePermission(root.getTree("/content/a/rep:cugPolicy"), aTp), true);

        TreePermission bTp = cugPermProvider.getTreePermission(root.getTree("/content/a/b"), aTp);
        tpMap.put(bTp, true);
        tpMap.put(cugPermProvider.getTreePermission(root.getTree("/content/a/b/c"), bTp), true);

        TreePermission aaTp = cugPermProvider.getTreePermission(root.getTree("/content/aa"), contentTp);
        tpMap.put(aaTp, false);

        TreePermission bbTp = cugPermProvider.getTreePermission(root.getTree("/content/aa/bb"), aaTp);
        tpMap.put(cugPermProvider.getTreePermission(root.getTree("/content/aa/bb/cc"), bbTp), true);

        // paths that may not contain cugs anyway
        tpMap.put(cugPermProvider.getTreePermission(root.getTree("/jcr:system"), rootTp), false);
        tpMap.put(rootTp, false);
        tpMap.put(cugPermProvider.getTreePermission(root.getTree(UNSUPPORTED_PATH), rootTp), false);

        for (TreePermission tp : tpMap.keySet()) {
            boolean expected = tpMap.get(tp);

            assertEquals(expected, cugPermProvider.handles(tp, Permissions.READ));
            assertEquals(expected, cugPermProvider.handles(tp, Permissions.READ_NODE));
            assertEquals(expected, cugPermProvider.handles(tp, Permissions.READ_PROPERTY));

            assertFalse(cugPermProvider.handles(tp, Permissions.ADD_NODE|Permissions.REMOVE));
        }
    }

    @Test
    public void testHandlesRepositoryPermissions() {
        assertFalse(cugPermProvider.handlesRepositoryPermissions());
    }

    @Test
    public void getPrivileges() {
        List<String> paths = ImmutableList.of("/", UNSUPPORTED_PATH,
                "/content", "/content/a", "/content/a/b",
                "/content/aa", "/content/bb", "/content/aa/bb/rep:cugPolicy");
        for (String p : paths) {
            assertTrue(cugPermProvider.getPrivileges(root.getTree(p)).isEmpty());
        }
    }

    @Test
    public void testGetPrivilegesAtCug() {
        Set<String> expected = ImmutableSet.of(
                        PrivilegeConstants.JCR_READ,
                        PrivilegeConstants.REP_READ_NODES,
                        PrivilegeConstants.REP_READ_PROPERTIES);
        assertEquals(expected, cugPermProvider.getPrivileges(root.getTree("/content/a/b/c")));
    }

    @Test
    public void testGetPrivilegesAtCug2() {
        PermissionProvider pp = new CugPermissionProvider(root, ImmutableSet.of(testGroupPrincipal), ImmutableSet.of(SUPPORTED_PATH), CugContext.INSTANCE);

        Set<String> expected = ImmutableSet.of(
                PrivilegeConstants.JCR_READ,
                PrivilegeConstants.REP_READ_NODES,
                PrivilegeConstants.REP_READ_PROPERTIES);
        assertEquals(expected, pp.getPrivileges(root.getTree("/content/a")));
        assertEquals(expected, pp.getPrivileges(root.getTree("/content/aa/bb")));

        assertTrue(pp.getPrivileges(root.getTree("/content/a/b/c")).isEmpty());
    }

    @Test
    public void hasReadPrivileges() {
        List<String> paths = ImmutableList.of("/", UNSUPPORTED_PATH,
                "/content", "/content/a", "/content/a/b",
                "/content/aa", "/content/bb", "/content/aa/bb/rep:cugPolicy");
        for (String p : paths) {
            Tree tree = root.getTree(p);
            assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_READ));
            assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.REP_READ_NODES));
            assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.REP_READ_PROPERTIES));
            assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.REP_READ_PROPERTIES));
        }

        Tree cugTree = root.getTree("/content/a/b/c");
        assertTrue(cugPermProvider.hasPrivileges(cugTree, PrivilegeConstants.JCR_READ));
        assertTrue(cugPermProvider.hasPrivileges(cugTree, PrivilegeConstants.REP_READ_NODES));
        assertTrue(cugPermProvider.hasPrivileges(cugTree, PrivilegeConstants.REP_READ_PROPERTIES));
        assertTrue(cugPermProvider.hasPrivileges(cugTree, PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.REP_READ_PROPERTIES));
    }

    @Test
    public void hasNonReadPrivileges() {
        List<String> paths = ImmutableList.of("/", UNSUPPORTED_PATH,
                "/content", "/content/a", "/content/a/b", "/content/a/b/c",
                "/content/aa", "/content/bb", "/content/aa/bb/rep:cugPolicy");
        for (String p : paths) {
            Tree tree = root.getTree(p);
            assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_WRITE));
            assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT));
            assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT));
            assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
            assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_ALL));
        }
    }

    @Test
    public void testIsGrantedRead() {
        List<String> paths = ImmutableList.of("/", UNSUPPORTED_PATH,
                "/content", "/content/a", "/content/a/b",
                "/content/aa", "/content/bb", "/content/aa/bb/rep:cugPolicy");
        for (String p : paths) {
            Tree tree = root.getTree(p);
            assertFalse(cugPermProvider.isGranted(tree, null, Permissions.READ));
            assertFalse(cugPermProvider.isGranted(tree, tree.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
        }

        Tree cugTree = root.getTree("/content/a/b/c");
        assertTrue(cugPermProvider.isGranted(cugTree, null, Permissions.READ));
        assertTrue(cugPermProvider.isGranted(cugTree, null, Permissions.READ_NODE));
        assertTrue(cugPermProvider.isGranted(cugTree, cugTree.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
    }

    @Test
    public void testIsGrantedNonRead() {
        List<String> paths = ImmutableList.of("/", UNSUPPORTED_PATH,
                        "/content", "/content/a", "/content/a/b", "/content/a/b/c",
                        "/content/aa", "/content/bb", "/content/aa/bb/rep:cugPolicy");
        for (String p : paths) {
            Tree tree = root.getTree(p);
            assertFalse(cugPermProvider.isGranted(tree, null, Permissions.ALL));
            assertFalse(cugPermProvider.isGranted(tree, null, Permissions.READ|Permissions.READ_ACCESS_CONTROL));
            assertFalse(cugPermProvider.isGranted(tree, null, Permissions.REMOVE_NODE));
        }
    }

    @Test
    public void testIsGrantedJcrActions() {
        List<String> paths = ImmutableList.of("/", UNSUPPORTED_PATH,
                "/content", "/content/a", "/content/a/b",
                "/content/aa", "/content/bb", "/content/aa/bb/rep:cugPolicy");
        for (String p : paths) {
            assertFalse(cugPermProvider.isGranted(p, Session.ACTION_READ));
            assertFalse(cugPermProvider.isGranted(p, Session.ACTION_ADD_NODE));
            assertFalse(cugPermProvider.isGranted(p, Session.ACTION_READ + ',' + Session.ACTION_ADD_NODE));
        }

        assertTrue(cugPermProvider.isGranted("/content/a/b/c", Session.ACTION_READ));
        assertFalse(cugPermProvider.isGranted("/content/a/b/c", Session.ACTION_ADD_NODE));
        assertFalse(cugPermProvider.isGranted("/content/a/b/c", Session.ACTION_READ + ',' + Session.ACTION_ADD_NODE));
    }

    @Test
    public void testGetTreePermissions() throws AccessDeniedException {
        TreePermission rootTp = cugPermProvider.getTreePermission(root.getTree("/"), TreePermission.EMPTY);
        assertTrue(rootTp.getClass().getName().endsWith("EmptyCugPermission"));

        TreePermission contentTp = cugPermProvider.getTreePermission(root.getTree(SUPPORTED_PATH), rootTp);
        assertTrue(contentTp.getClass().getName().endsWith("EmptyCugPermission"));

        TreePermission aTp = cugPermProvider.getTreePermission(root.getTree("/content/a"), contentTp);
        assertTrue(aTp.getClass().getName().endsWith("CugTreePermission"));

        TreePermission bTp = cugPermProvider.getTreePermission(root.getTree("/content/a/b"), aTp);
        assertTrue(bTp.getClass().getName().endsWith("CugTreePermission"));

        TreePermission cTp = cugPermProvider.getTreePermission(root.getTree("/content/a/b/c"), bTp);
        assertTrue(cTp.getClass().getName().endsWith("CugTreePermission"));

        TreePermission aaTp = cugPermProvider.getTreePermission(root.getTree("/content/aa"), contentTp);
        assertTrue(aaTp.getClass().getName().endsWith("EmptyCugPermission"));

        TreePermission bbTp = cugPermProvider.getTreePermission(root.getTree("/content/aa/bb"), aaTp);
        assertTrue(bbTp.getClass().getName().endsWith("CugTreePermission"));

        TreePermission ccTp = cugPermProvider.getTreePermission(root.getTree("/content/aa/bb/cc"), bbTp);
        assertTrue(ccTp.getClass().getName().endsWith("CugTreePermission"));

        // false cug-policy node (wrong nt)
        Tree aaTree = root.getTree("/content/aa");
        new NodeUtil(aaTree).addChild(CugConstants.REP_CUG_POLICY, NT_OAK_UNSTRUCTURED);
        TreePermission aaTp2 = cugPermProvider.getTreePermission(root.getTree("/content/aa"), contentTp);
        assertTrue(aaTp2.getClass().getName().endsWith("EmptyCugPermission"));

        TreePermission falseCugTp = cugPermProvider.getTreePermission(root.getTree("/content/aa/rep:cugPolicy"), aaTp2);
        assertNotSame(TreePermission.EMPTY, falseCugTp);

        // cug content
        TreePermission cugTp = cugPermProvider.getTreePermission(root.getTree("/content/a/rep:cugPolicy"), aTp);
        assertSame(TreePermission.EMPTY, cugTp);

        // paths that may not contain cugs anyway
        assertSame(TreePermission.EMPTY, cugPermProvider.getTreePermission(root.getTree("/jcr:system"), rootTp));
        assertSame(TreePermission.EMPTY, cugPermProvider.getTreePermission(root.getTree(UNSUPPORTED_PATH), rootTp));
    }
}