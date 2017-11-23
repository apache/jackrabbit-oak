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
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.GuestCredentials;
import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.RepositoryPermission;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CugPermissionProviderTest extends AbstractCugTest implements NodeTypeConstants {

    private static final Map<String, Boolean> PATH_INCUG_MAP = new HashMap<String, Boolean>();
    static {
        PATH_INCUG_MAP.put(SUPPORTED_PATH, false);
        PATH_INCUG_MAP.put("/content/a", true);
        PATH_INCUG_MAP.put("/content/a/rep:cugPolicy", false);
        PATH_INCUG_MAP.put("/content/a/b", true);
        PATH_INCUG_MAP.put("/content/a/b/c/jcr:primaryType", true);
        PATH_INCUG_MAP.put("/content/aa", false);
        PATH_INCUG_MAP.put("/content/aa/bb/cc", true);

        // path below supported-path but which never contains a cug
        PATH_INCUG_MAP.put("/content/no", false);
        PATH_INCUG_MAP.put("/content/no/cug", false);
        PATH_INCUG_MAP.put("/content/no/cug/in", false);
        PATH_INCUG_MAP.put("/content/no/cug/in/subtree", false);

        // paths that may not contain cugs anyway
        PATH_INCUG_MAP.put(NODE_TYPES_PATH, false);
        PATH_INCUG_MAP.put("/", false);
        PATH_INCUG_MAP.put(UNSUPPORTED_PATH, false);
        PATH_INCUG_MAP.put(INVALID_PATH, false);
    }

    private static final List<String> READABLE_PATHS = ImmutableList.of(
            "/content/a/b/c", "/content/a/b/c/jcr:primaryType",
            "/content/a/b/c/nonExisting", "/content/a/b/c/nonExisting/jcr:primaryType");

    private static final List<String> NOT_READABLE_PATHS = ImmutableList.of(
            "/", "/jcr:primaryType",
            UNSUPPORTED_PATH, UNSUPPORTED_PATH + "/jcr:primaryType",
            "/content", "/content/jcr:primaryType",
            "/content/a", "/content/a/jcr:primaryType",
            "/content/a/b", "/content/a/b/jcr:primaryType",
            "/content/a/b/c/rep:cugPolicy", "/content/a/b/c/rep:cugPolicy/jcr:primaryType", "/content/a/b/c/rep:cugPolicy/rep:principalNames",
            "/content/a/b/c/rep:cugPolicy/nonExisting", "/content/a/b/c/rep:cugPolicy/nonExisting/jcr:primaryType",
            "/content/aa", "/content/aa/jcr:primaryType",
            "/content/bb", "/content/bb/jcr:primaryType",
            "/content/aa/bb/rep:cugPolicy", "/content/aa/bb/rep:cugPolicy/jcr:primaryType", "/content/aa/bb/rep:cugPolicy/rep:principalNames",
            "/content/nonExisting", "/content/nonExisting/jcr:primaryType",
            "/content/no","/content/no/cug","/content/no/cug/in","/content/no/cug/in/subtree");

    private Principal testGroupPrincipal;
    private CugPermissionProvider cugPermProvider;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        testGroupPrincipal = getTestGroupPrincipal();

        // add more child nodes
        NodeUtil n = new NodeUtil(root.getTree(SUPPORTED_PATH));
        n.addChild("a", NT_OAK_UNSTRUCTURED).addChild("b", NT_OAK_UNSTRUCTURED).addChild("c", NT_OAK_UNSTRUCTURED);
        n.addChild("aa", NT_OAK_UNSTRUCTURED).addChild("bb", NT_OAK_UNSTRUCTURED).addChild("cc", NT_OAK_UNSTRUCTURED);
        n.addChild("no", NT_OAK_UNSTRUCTURED).addChild("cug", NT_OAK_UNSTRUCTURED).addChild("in", NT_OAK_UNSTRUCTURED).addChild("subtree", NT_OAK_UNSTRUCTURED);

        createCug("/content/a", testGroupPrincipal);
        createCug("/content/a/b/c", EveryonePrincipal.getInstance());
        createCug("/content/aa/bb", testGroupPrincipal);

        root.commit();

        cugPermProvider = createCugPermissionProvider(ImmutableSet.of(SUPPORTED_PATH), getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
    }

    //---------------------------------------< AggregatedPermissionProvider >---
    /**
     * @see org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider#supportedPrivileges(org.apache.jackrabbit.oak.api.Tree, org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits)
     */
    @Test
    public void testSupportedPrivileges() {
        PrivilegeBits readBits = PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ);
        PrivilegeBits readNodeBits = PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_NODES);
        PrivilegeBits readPropBits = PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_PROPERTIES);
        PrivilegeBitsProvider provider = new PrivilegeBitsProvider(root);

        for (String path : PATH_INCUG_MAP.keySet()) {
            boolean isInCug = PATH_INCUG_MAP.get(path);
            Tree tree = root.getTree(path);
            if (isInCug) {
                assertPrivilegeBits(readBits, cugPermProvider.supportedPrivileges(tree, readBits));
                assertPrivilegeBits(readNodeBits, cugPermProvider.supportedPrivileges(tree, readNodeBits));
                assertPrivilegeBits(readPropBits, cugPermProvider.supportedPrivileges(tree, readPropBits));

                assertPrivilegeBits(readBits, cugPermProvider.supportedPrivileges(tree, provider.getBits(PrivilegeConstants.JCR_ALL)));
                assertPrivilegeBits(readNodeBits, cugPermProvider.supportedPrivileges(tree, provider.getBits(PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.JCR_READ_ACCESS_CONTROL)));

            } else {
                assertTrue(cugPermProvider.supportedPrivileges(tree, readBits).isEmpty());
                assertTrue(cugPermProvider.supportedPrivileges(tree, readNodeBits).isEmpty());
                assertTrue(cugPermProvider.supportedPrivileges(tree, readPropBits).isEmpty());

                assertTrue(cugPermProvider.supportedPrivileges(tree, provider.getBits(PrivilegeConstants.JCR_ALL)).isEmpty());
                assertTrue(cugPermProvider.supportedPrivileges(tree, provider.getBits(PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.JCR_READ_ACCESS_CONTROL)).isEmpty());
            }

            assertTrue(cugPermProvider.supportedPrivileges(tree, provider.getBits(PrivilegeConstants.REP_WRITE)).isEmpty());
            assertTrue(cugPermProvider.supportedPrivileges(tree, provider.getBits(PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_REMOVE_CHILD_NODES, PrivilegeConstants.JCR_REMOVE_NODE)).isEmpty());
            assertTrue(cugPermProvider.supportedPrivileges(tree, provider.getBits(PrivilegeConstants.JCR_READ_ACCESS_CONTROL)).isEmpty());
        }
    }

    private static void assertPrivilegeBits(@Nonnull PrivilegeBits expected, @Nonnull PrivilegeBits toTest) {
        assertEquals(expected, toTest.unmodifiable());
    }

    /**
     * @see org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider#supportedPrivileges(org.apache.jackrabbit.oak.api.Tree, org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits)
     */
    @Test
    public void testSupportedPrivilegesForNullTree() {
        PrivilegeBits readBits = PrivilegeBits.BUILT_IN.get(PrivilegeConstants.JCR_READ);
        PrivilegeBits readNodeBits = PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_NODES);
        PrivilegeBits readPropBits = PrivilegeBits.BUILT_IN.get(PrivilegeConstants.REP_READ_PROPERTIES);
        PrivilegeBitsProvider provider = new PrivilegeBitsProvider(root);


        assertTrue(cugPermProvider.supportedPrivileges(null, readBits).isEmpty());
        assertTrue(cugPermProvider.supportedPrivileges(null, readNodeBits).isEmpty());
        assertTrue(cugPermProvider.supportedPrivileges(null, readPropBits).isEmpty());

        assertTrue(cugPermProvider.supportedPrivileges(null, provider.getBits(PrivilegeConstants.JCR_ALL)).isEmpty());
        assertTrue(cugPermProvider.supportedPrivileges(null, provider.getBits(PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.JCR_READ_ACCESS_CONTROL)).isEmpty());

        assertTrue(cugPermProvider.supportedPrivileges(null, provider.getBits(PrivilegeConstants.REP_WRITE)).isEmpty());
        assertTrue(cugPermProvider.supportedPrivileges(null, provider.getBits(PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_REMOVE_CHILD_NODES, PrivilegeConstants.JCR_REMOVE_NODE)).isEmpty());
        assertTrue(cugPermProvider.supportedPrivileges(null, provider.getBits(PrivilegeConstants.JCR_READ_ACCESS_CONTROL)).isEmpty());
    }

    /**
     * @see org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider#supportedPermissions(org.apache.jackrabbit.oak.api.Tree, org.apache.jackrabbit.oak.api.PropertyState, long)
     */
    @Test
    public void testSupportedPermissionsByTree() {
        for (String path : PATH_INCUG_MAP.keySet()) {
            boolean isInCug = PATH_INCUG_MAP.get(path);
            Tree tree = root.getTree(path);

            if (isInCug) {
                assertEquals(Permissions.READ, cugPermProvider.supportedPermissions(tree, null, Permissions.READ));
                assertEquals(Permissions.READ_NODE, cugPermProvider.supportedPermissions(tree, null, Permissions.READ_NODE));
                assertEquals(Permissions.READ_PROPERTY, cugPermProvider.supportedPermissions(tree, null, Permissions.READ_PROPERTY));

                assertEquals(Permissions.READ, cugPermProvider.supportedPermissions(tree, null, Permissions.ALL));
                assertEquals(Permissions.READ_NODE, cugPermProvider.supportedPermissions(tree, null, Permissions.READ_NODE | Permissions.READ_ACCESS_CONTROL));

            } else {
                assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tree, null, Permissions.READ));
                assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tree, null, Permissions.READ_NODE));
                assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tree, null, Permissions.READ_PROPERTY));

                assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tree, null, Permissions.ALL));
                assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tree, null, Permissions.READ_NODE | Permissions.READ_ACCESS_CONTROL));
            }

            assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tree, null, Permissions.WRITE));
            assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tree, null, Permissions.ADD_NODE | Permissions.REMOVE));
            assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tree, null, Permissions.READ_ACCESS_CONTROL));
        }
    }

    /**
     * @see org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider#supportedPermissions(org.apache.jackrabbit.oak.api.Tree, org.apache.jackrabbit.oak.api.PropertyState, long)
     */
    @Test
    public void testSupportedPermissionsByNullTree() {
        assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions((Tree) null, null, Permissions.READ));
        assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions((Tree) null, null, Permissions.READ_NODE));
        assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions((Tree) null, null, Permissions.READ_PROPERTY));

        assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions((Tree) null, null, Permissions.ALL));
        assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions((Tree) null, null, Permissions.READ_NODE | Permissions.READ_ACCESS_CONTROL));

        assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions((Tree) null, null, Permissions.WRITE));
        assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions((Tree) null, null, Permissions.ADD_NODE | Permissions.REMOVE));
        assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions((Tree) null, null, Permissions.READ_ACCESS_CONTROL));
    }

    /**
     * @see org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider#supportedPermissions(org.apache.jackrabbit.oak.plugins.tree.TreeLocation, long)
     */
    @Test
    public void testSupportedPermissionsByLocation() {
        for (String path : PATH_INCUG_MAP.keySet()) {
            boolean isInCug = PATH_INCUG_MAP.get(path);
            TreeLocation location = TreeLocation.create(root, path);

            if (isInCug) {
                assertEquals(path, Permissions.READ, cugPermProvider.supportedPermissions(location, Permissions.READ));
                assertEquals(path, Permissions.READ_NODE, cugPermProvider.supportedPermissions(location, Permissions.READ_NODE));
                assertEquals(path, Permissions.READ_PROPERTY, cugPermProvider.supportedPermissions(location, Permissions.READ_PROPERTY));

                assertEquals(path, Permissions.READ, cugPermProvider.supportedPermissions(location, Permissions.ALL));
                assertEquals(path, Permissions.READ_NODE, cugPermProvider.supportedPermissions(location, Permissions.READ_NODE | Permissions.READ_ACCESS_CONTROL));
            } else {
                assertEquals(path, Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(location, Permissions.READ));
                assertEquals(path, Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(location, Permissions.READ_NODE));
                assertEquals(path, Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(location, Permissions.READ_PROPERTY));

                assertEquals(path, Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(location, Permissions.ALL));
                assertEquals(path, Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(location, Permissions.READ_NODE | Permissions.READ_ACCESS_CONTROL));
            }

            assertEquals(path, Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(location, Permissions.READ_ACCESS_CONTROL));
            assertEquals(path, Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(location, Permissions.MODIFY_ACCESS_CONTROL));
            assertEquals(path, Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(location, Permissions.ADD_NODE));
            assertEquals(path, Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(location, Permissions.WRITE));

            assertEquals(path, Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(TreeLocation.create(root, "/path/to/no-existing/tree"), Permissions.READ));
        }
    }

    /**
     * @see org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider#supportedPermissions(org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission, org.apache.jackrabbit.oak.api.PropertyState, long)
     */
    @Test
    public void testSupportedPermissionsByTreePermission() {
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
            boolean isInCug = tpMap.get(tp);

            if (isInCug) {
                assertEquals(Permissions.READ, cugPermProvider.supportedPermissions(tp, null, Permissions.READ));
                assertEquals(Permissions.READ_NODE, cugPermProvider.supportedPermissions(tp, null, Permissions.READ_NODE));
                assertEquals(Permissions.READ_PROPERTY, cugPermProvider.supportedPermissions(tp, null, Permissions.READ_PROPERTY));
                assertEquals(Permissions.READ, cugPermProvider.supportedPermissions(tp, null, Permissions.ALL));
                assertEquals(Permissions.READ_NODE, cugPermProvider.supportedPermissions(tp, null, Permissions.READ_NODE | Permissions.READ_ACCESS_CONTROL));
            } else {
                assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tp, null, Permissions.READ));
                assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tp, null, Permissions.READ_NODE));
                assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tp, null, Permissions.READ_PROPERTY));
                assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tp, null, Permissions.ALL));
                assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tp, null, Permissions.READ_NODE | Permissions.READ_ACCESS_CONTROL));
            }
            assertEquals(Permissions.NO_PERMISSION, cugPermProvider.supportedPermissions(tp, null, Permissions.ADD_NODE | Permissions.REMOVE));
        }
    }

    /**
     * @see org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider#isGranted(org.apache.jackrabbit.oak.plugins.tree.TreeLocation, long)
     */
    @Test
    public void testIsGrantedByLocation() throws Exception {
        for (String p : NOT_READABLE_PATHS) {
            TreeLocation location = TreeLocation.create(root, p);
            assertFalse(cugPermProvider.isGranted(location, Permissions.READ));
            assertFalse(cugPermProvider.isGranted(location, Permissions.READ_NODE));
            assertFalse(cugPermProvider.isGranted(location, Permissions.READ_PROPERTY));

            assertFalse(cugPermProvider.isGranted(location, Permissions.ALL));
            assertFalse(cugPermProvider.isGranted(location, Permissions.ADD_NODE));
            assertFalse(cugPermProvider.isGranted(location, Permissions.READ_ACCESS_CONTROL));
        }

        for (String p : READABLE_PATHS) {
            TreeLocation location = TreeLocation.create(root, p);
            assertTrue(cugPermProvider.isGranted(location, Permissions.READ));
            assertTrue(cugPermProvider.isGranted(location, Permissions.READ_NODE));
            assertTrue(cugPermProvider.isGranted(location, Permissions.READ_PROPERTY));

            assertFalse(cugPermProvider.isGranted(location, Permissions.ALL));
            assertFalse(cugPermProvider.isGranted(location, Permissions.ADD_NODE));
            assertFalse(cugPermProvider.isGranted(location, Permissions.READ_ACCESS_CONTROL));
        }
    }

    /**
     * @see org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider#isGranted(org.apache.jackrabbit.oak.plugins.tree.TreeLocation, long)
     */
    @Test
    public void testIsGrantedNonExistingLocation() throws Exception {
        ContentSession anonymous = login(new GuestCredentials());
        try {
            // additionally create a root that doesn't have access to the root node
            Root anonymousRoot = anonymous.getLatestRoot();

            for (Root r : new Root[] {anonymousRoot, root}) {
                TreeLocation location = TreeLocation.create(r, "/path/to/non/existing/tree");
                assertFalse(cugPermProvider.isGranted(location, Permissions.READ));
                assertFalse(cugPermProvider.isGranted(location, Permissions.READ_NODE));
                assertFalse(cugPermProvider.isGranted(location, Permissions.READ_PROPERTY));

                assertFalse(cugPermProvider.isGranted(location, Permissions.ALL));
                assertFalse(cugPermProvider.isGranted(location, Permissions.ADD_NODE));
                assertFalse(cugPermProvider.isGranted(location, Permissions.READ_ACCESS_CONTROL));
            }
        } finally {
            anonymous.close();
        }
    }

    //------------------------------------------------------< getPrivileges >---
    /**
     * @see PermissionProvider#getPrivileges(org.apache.jackrabbit.oak.api.Tree)
     */
    @Test
    public void getPrivileges() {
        for (String p : NOT_READABLE_PATHS) {
            Tree tree = root.getTree(p);
            if (tree.exists()) {
                assertTrue(cugPermProvider.getPrivileges(root.getTree(p)).isEmpty());
            }
        }
    }

    /**
     * @see PermissionProvider#getPrivileges(org.apache.jackrabbit.oak.api.Tree)
     */
    @Test
    public void testGetPrivilegesAtCug() {
        Set<String> expected = ImmutableSet.of(
                PrivilegeConstants.JCR_READ,
                PrivilegeConstants.REP_READ_NODES,
                PrivilegeConstants.REP_READ_PROPERTIES);

        for (String p : READABLE_PATHS) {
            Tree tree = root.getTree(p);
            if (tree.exists()) {
                assertEquals(expected, cugPermProvider.getPrivileges(tree));
            }
        }
    }

    /**
     * @see PermissionProvider#getPrivileges(org.apache.jackrabbit.oak.api.Tree)
     */
    @Test
    public void testGetPrivilegesAtCug2() {
        PermissionProvider pp = createCugPermissionProvider(ImmutableSet.of(SUPPORTED_PATH), testGroupPrincipal);

        Set<String> expected = ImmutableSet.of(
                PrivilegeConstants.JCR_READ,
                PrivilegeConstants.REP_READ_NODES,
                PrivilegeConstants.REP_READ_PROPERTIES);
        assertEquals(expected, pp.getPrivileges(root.getTree("/content/a")));
        assertEquals(expected, pp.getPrivileges(root.getTree("/content/aa/bb")));

        assertTrue(pp.getPrivileges(root.getTree("/content/a/b/c")).isEmpty());
    }

    /**
     * @see PermissionProvider#getPrivileges(org.apache.jackrabbit.oak.api.Tree)
     */
    @Test
    public void testGetPrivilegesNullPath() {
        assertTrue(cugPermProvider.getPrivileges(null).isEmpty());
    }

    //------------------------------------------------------< hasPrivileges >---
    /**
     * @see PermissionProvider#hasPrivileges(org.apache.jackrabbit.oak.api.Tree, String...)
     */
    @Test
    public void testHasReadPrivileges() {
        for (String p : NOT_READABLE_PATHS) {
            Tree tree = root.getTree(p);
            if (tree.exists()) {
                assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_READ));
                assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.REP_READ_NODES));
                assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.REP_READ_PROPERTIES));
                assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.REP_READ_PROPERTIES));
            }
        }
    }

    /**
     * @see PermissionProvider#hasPrivileges(org.apache.jackrabbit.oak.api.Tree, String...)
     */
    @Test
    public void testHasReadPrivilegesAtCug() {
        for (String p : READABLE_PATHS) {
            Tree tree = root.getTree(p);
            if (tree.exists()) {
                assertTrue(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_READ));
                assertTrue(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.REP_READ_NODES));
                assertTrue(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.REP_READ_PROPERTIES));
                assertTrue(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.REP_READ_PROPERTIES));
            }
        }
    }

    /**
     * @see PermissionProvider#hasPrivileges(org.apache.jackrabbit.oak.api.Tree, String...)
     */
    @Test
    public void testHasNonReadPrivileges() {
        for (String p : Iterables.concat(READABLE_PATHS, NOT_READABLE_PATHS)) {
            Tree tree = root.getTree(p);
            if (tree.exists()) {
                assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_WRITE));
                assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT));
                assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT));
                assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
                assertFalse(cugPermProvider.hasPrivileges(tree, PrivilegeConstants.JCR_ALL));
            }
        }
    }

    /**
     * @see PermissionProvider#hasPrivileges(org.apache.jackrabbit.oak.api.Tree, String...)
     */
    @Test
    public void testHasPrivilegesNullPath() {
        assertFalse(cugPermProvider.hasPrivileges(null, PrivilegeConstants.JCR_READ));
    }

    //--------------------------------------------< getRepositoryPermission >---
    /**
     * @see org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider#getRepositoryPermission()
     */
    @Test
    public void testGetRepositoryPermissions() {
        assertSame(RepositoryPermission.EMPTY, cugPermProvider.getRepositoryPermission());
    }

    //--------------------------------------------------< getTreePermission >---
    /**
     * @see PermissionProvider#getTreePermission(org.apache.jackrabbit.oak.api.Tree, org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission)
     */
    @Test
    public void testGetTreePermissions() throws AccessDeniedException {
        TreePermission rootTp = cugPermProvider.getTreePermission(root.getTree("/"), TreePermission.EMPTY);
        assertTrue(rootTp instanceof EmptyCugTreePermission);

        TreePermission contentTp = cugPermProvider.getTreePermission(root.getTree(SUPPORTED_PATH), rootTp);
        assertTrue(contentTp instanceof CugTreePermission);

        TreePermission aTp = cugPermProvider.getTreePermission(root.getTree("/content/a"), contentTp);
        assertTrue(aTp instanceof CugTreePermission);

        TreePermission bTp = cugPermProvider.getTreePermission(root.getTree("/content/a/b"), aTp);
        assertTrue(bTp instanceof CugTreePermission);

        TreePermission cTp = cugPermProvider.getTreePermission(root.getTree("/content/a/b/c"), bTp);
        assertTrue(cTp instanceof CugTreePermission);

        TreePermission aaTp = cugPermProvider.getTreePermission(root.getTree("/content/aa"), contentTp);
        assertTrue(aaTp instanceof CugTreePermission);

        TreePermission bbTp = cugPermProvider.getTreePermission(root.getTree("/content/aa/bb"), aaTp);
        assertTrue(bbTp instanceof CugTreePermission);

        TreePermission ccTp = cugPermProvider.getTreePermission(root.getTree("/content/aa/bb/cc"), bbTp);
        assertTrue(ccTp instanceof CugTreePermission);

        // false cug-policy node (wrong nt)
        Tree aaTree = root.getTree("/content/aa");
        new NodeUtil(aaTree).addChild(CugConstants.REP_CUG_POLICY, NT_OAK_UNSTRUCTURED);
        TreePermission aaTp2 = cugPermProvider.getTreePermission(root.getTree("/content/aa"), contentTp);
        assertTrue(aaTp2 instanceof CugTreePermission);

        TreePermission falseCugTp = cugPermProvider.getTreePermission(root.getTree("/content/aa/rep:cugPolicy"), aaTp2);
        assertNotSame(TreePermission.EMPTY, falseCugTp);

        // cug content
        TreePermission cugTp = cugPermProvider.getTreePermission(root.getTree("/content/a/rep:cugPolicy"), aTp);
        assertSame(TreePermission.NO_RECOURSE, cugTp);

        // jcr:system special case
        TreePermission jcrSystemTp = cugPermProvider.getTreePermission(root.getTree("/jcr:system"), rootTp);
        assertTrue(jcrSystemTp instanceof EmptyCugTreePermission);

        // paths that may not contain cugs anyway
        assertSame(TreePermission.NO_RECOURSE, cugPermProvider.getTreePermission(root.getTree(NodeTypeConstants.NODE_TYPES_PATH), jcrSystemTp));
        TreePermission unsupportedPathTp = cugPermProvider.getTreePermission(root.getTree(UNSUPPORTED_PATH), rootTp);
        assertSame(TreePermission.NO_RECOURSE, unsupportedPathTp);
        try {
            cugPermProvider.getTreePermission(root.getTree(UNSUPPORTED_PATH + "/child"), unsupportedPathTp);
            fail();
        } catch (IllegalStateException e) {
            // success
        }
    }

    //-------------------------------< isGranted(Tree, PropertyState, long) >---
    /**
     * @see PermissionProvider#isGranted(org.apache.jackrabbit.oak.api.Tree, org.apache.jackrabbit.oak.api.PropertyState, long)
     */
    @Test
    public void testIsGrantedNonRead() {
        for (String p : Iterables.concat(READABLE_PATHS, NOT_READABLE_PATHS)) {
            Tree tree = root.getTree(p);
            if (tree.exists()) {
                assertFalse(cugPermProvider.isGranted(tree, null, Permissions.ALL));
                assertFalse(cugPermProvider.isGranted(tree, null, Permissions.READ | Permissions.READ_ACCESS_CONTROL));
                assertFalse(cugPermProvider.isGranted(tree, null, Permissions.REMOVE_NODE));
            }
        }
    }

    /**
     * @see PermissionProvider#isGranted(org.apache.jackrabbit.oak.api.Tree, org.apache.jackrabbit.oak.api.PropertyState, long)
     */
    @Test
    public void testIsGrantedRead() {
        for (String p : NOT_READABLE_PATHS) {
            Tree tree = root.getTree(p);
            if (tree.exists()) {
                assertFalse(cugPermProvider.isGranted(tree, null, Permissions.READ));
                assertFalse(cugPermProvider.isGranted(tree, tree.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
            }
        }

        for (String p : READABLE_PATHS) {
            Tree tree = root.getTree(p);
            if (tree.exists()) {
                assertTrue(cugPermProvider.isGranted(tree, null, Permissions.READ));
                assertTrue(cugPermProvider.isGranted(tree, null, Permissions.READ_NODE));
                assertTrue(cugPermProvider.isGranted(tree, tree.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
            }
        }
    }

    //------------------------------------------< isGranted(String, String) >---
    /**
     * @see PermissionProvider#isGranted(String, String)
     */
    @Test
    public void testIsGrantedJcrActions() {
        for (String p : NOT_READABLE_PATHS) {
            assertFalse(cugPermProvider.isGranted(p, Session.ACTION_READ));
            assertFalse(cugPermProvider.isGranted(p, Session.ACTION_ADD_NODE));
            assertFalse(cugPermProvider.isGranted(p, Session.ACTION_READ + ',' + Session.ACTION_ADD_NODE));
        }

        for (String p : READABLE_PATHS) {
            assertTrue(cugPermProvider.isGranted(p, Session.ACTION_READ));
            assertFalse(cugPermProvider.isGranted(p, Session.ACTION_ADD_NODE));
            assertFalse(cugPermProvider.isGranted(p, Session.ACTION_READ + ',' + Session.ACTION_ADD_NODE));
        }
    }

    /**
     * @see PermissionProvider#isGranted(String, String)
     */
    @Test
    public void testIsGrantedJcrActionsNonExistingPath() {
        String p = "/path/to/non/existing/tree";
        assertFalse(cugPermProvider.isGranted(p, Session.ACTION_READ));
        assertFalse(cugPermProvider.isGranted(p, Permissions.getString(Permissions.READ_NODE)));
        assertFalse(cugPermProvider.isGranted(p, Permissions.getString(Permissions.READ_PROPERTY)));
        assertFalse(cugPermProvider.isGranted(p, Session.ACTION_ADD_NODE));
        assertFalse(cugPermProvider.isGranted(p, Session.ACTION_READ + ',' + Session.ACTION_ADD_NODE));
    }
}