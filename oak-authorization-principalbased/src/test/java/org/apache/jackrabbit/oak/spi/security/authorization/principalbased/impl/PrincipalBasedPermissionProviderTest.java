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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBits;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.Value;
import java.security.Principal;
import java.util.Map;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_NT_NAMES;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_POLICY;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_REMOVE_CHILD_NODES;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_REMOVE_NODE;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_VERSION_MANAGEMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PrincipalBasedPermissionProviderTest extends AbstractPrincipalBasedTest {
    
    private PrincipalBasedPermissionProvider permissionProvider;

    private String childPath;

    @Before
    public void before() throws Exception {
        super.before();

        childPath = PathUtils.getAncestorPath(TEST_OAK_PATH, 2);

        Principal testPrincipal = getTestSystemUser().getPrincipal();
        setupContentTrees(TEST_OAK_PATH);
        setupContentTrees(NT_FOLDER, childPath + "/folder", TEST_OAK_PATH + "/folder");

        // setup permissions on childPath + TEST_OAK_PATH
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(testPrincipal, getNamePathMapper().getJcrPath(childPath), JCR_READ, JCR_REMOVE_CHILD_NODES);
        addPrincipalBasedEntry(policy, getNamePathMapper().getJcrPath(TEST_OAK_PATH), JCR_VERSION_MANAGEMENT);

        // add an entry with nt-name restriction at childPath
        JackrabbitAccessControlManager jacm = getAccessControlManager(root);
        policy = getPrincipalPolicyImpl(testPrincipal, jacm);
        Map<String, Value[]> restrictions = Map.of(REP_NT_NAMES, new Value[] {getValueFactory(root).createValue(NT_FOLDER, PropertyType.NAME)});
        policy.addEntry(childPath, privilegesFromNames(JCR_REMOVE_NODE), Map.of(), restrictions);
        jacm.setPolicy(policy.getPath(), policy);
        root.commit();

        permissionProvider = createPermissionProvider(root, getTestSystemUser().getPrincipal());
    }

    @Override
    protected NamePathMapper getNamePathMapper() {
        return NamePathMapper.DEFAULT;
    }

    @Test
    public void testSupportedPrivileges() {
        for (PrivilegeBits bits : PrivilegeBits.BUILT_IN.values()) {
            assertEquals(bits, permissionProvider.supportedPrivileges(null, bits));
            assertEquals(bits, permissionProvider.supportedPrivileges(mock(Tree.class), bits));
        }
    }

    @Test
    public void testSupportedPrivilegesAllBits() {
        PrivilegeBits all = new PrivilegeBitsProvider(root).getBits(PrivilegeConstants.JCR_ALL);
        assertEquals(all, permissionProvider.supportedPrivileges(null, all));
        assertEquals(all, permissionProvider.supportedPrivileges(mock(Tree.class), all));
    }

    @Test
    public void testSupportedPrivilegesNullBits() {
        PrivilegeBits all = new PrivilegeBitsProvider(root).getBits(PrivilegeConstants.JCR_ALL);
        assertEquals(all, permissionProvider.supportedPrivileges(null, null));
        assertEquals(all, permissionProvider.supportedPrivileges(mock(Tree.class), null));
    }

    @Test
    public void testSupportedPermissions() {
        Tree tree = mock(Tree.class);
        PropertyState property = mock(PropertyState.class);
        for (long permission : Permissions.aggregates(Permissions.ALL)) {
            assertEquals(permission, permissionProvider.supportedPermissions(tree, property, permission));
            assertEquals(permission, permissionProvider.supportedPermissions(tree, null, permission));
        }
        assertEquals(Permissions.ALL, permissionProvider.supportedPermissions(tree, property, Permissions.ALL));
        assertEquals(Permissions.ALL, permissionProvider.supportedPermissions(tree, null, Permissions.ALL));
    }

    @Test
    public void testSupportedPermissionsTreeLocation() {
        TreeLocation location = mock(TreeLocation.class);
        for (long permission : Permissions.aggregates(Permissions.ALL)) {
            assertEquals(permission, permissionProvider.supportedPermissions(location, permission));
        }
        assertEquals(Permissions.ALL, permissionProvider.supportedPermissions(location, Permissions.ALL));
    }

    @Test
    public void testSupportedPermissionsTreePermission() {
        TreePermission tp = mock(TreePermission.class);
        PropertyState property = mock(PropertyState.class);
        for (long permission : Permissions.aggregates(Permissions.ALL)) {
            assertEquals(permission, permissionProvider.supportedPermissions(tp, property, permission));
            assertEquals(permission, permissionProvider.supportedPermissions(tp, null, permission));
        }
        assertEquals(Permissions.ALL, permissionProvider.supportedPermissions(tp, property, Permissions.ALL));
        assertEquals(Permissions.ALL, permissionProvider.supportedPermissions(tp, null, Permissions.ALL));
    }

    @Test
    public void testHasPrivileges() {
        assertTrue(permissionProvider.hasPrivileges(root.getTree(childPath), PrivilegeConstants.JCR_READ));
        assertFalse(permissionProvider.hasPrivileges(root.getTree(childPath), PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_REMOVE_NODE));
        assertTrue(permissionProvider.hasPrivileges(root.getTree(childPath + "/folder"), PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_REMOVE_NODE));
        assertFalse(permissionProvider.hasPrivileges(root.getTree(childPath), PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_VERSION_MANAGEMENT));
        assertTrue(permissionProvider.hasPrivileges(root.getTree(TEST_OAK_PATH), PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_VERSION_MANAGEMENT));
    }

    @Test
    public void testGetTreePermission() {
        Tree tree = root.getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(tree, TreePermission.EMPTY);
        for (String elem : PathUtils.elements(TEST_OAK_PATH)) {
            tree = tree.getChild(elem);
            tp = permissionProvider.getTreePermission(tree, tp);
            assertTrue(tp instanceof AbstractTreePermission);
            assertSame(TreeType.DEFAULT, ((AbstractTreePermission) tp).getType());
        }
    }

    @Test
    public void testIsGranted() {
        Tree t = root.getTree(childPath);
        assertTrue(permissionProvider.isGranted(t, null, Permissions.READ_NODE));
        assertTrue(permissionProvider.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
        assertFalse(permissionProvider.isGranted(t, null, Permissions.READ_NODE|Permissions.VERSION_MANAGEMENT));


        t = root.getTree(TEST_OAK_PATH);
        assertTrue(permissionProvider.isGranted(t, null, Permissions.READ_NODE|Permissions.VERSION_MANAGEMENT));
        assertTrue(permissionProvider.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY|Permissions.VERSION_MANAGEMENT));
    }

    @Test
    public void testIsGrantedNonExistingTree() {
        Tree nonExisting = root.getTree(TEST_OAK_PATH).getChild("nonExisting");

        assertTrue(permissionProvider.isGranted(nonExisting, null, Permissions.READ));
        assertTrue(permissionProvider.isGranted(nonExisting, PropertyStates.createProperty("propName", "value"), Permissions.READ));
    }

    @Test
    public void testIsGrantedWithRestriction() {
        assertFalse(permissionProvider.isGranted(root.getTree(TEST_OAK_PATH), null, Permissions.REMOVE_NODE));
        assertFalse(permissionProvider.isGranted(root.getTree(childPath), null, Permissions.REMOVE_NODE));

        assertTrue(permissionProvider.isGranted(root.getTree(TEST_OAK_PATH + "/folder"), null, Permissions.REMOVE_NODE));
        assertTrue(permissionProvider.isGranted(root.getTree(childPath + "/folder"), null, Permissions.REMOVE_NODE));
    }

    @Test
    public void testIsGrantedTreeLocation() {
        TreeLocation tl = TreeLocation.create(root, TEST_OAK_PATH);
        assertFalse(permissionProvider.isGranted(tl, Permissions.READ|Permissions.REMOVE_NODE));
    }

    @Test
    public void testIsGrantedNonExistingTreeLocation() {
        TreeLocation tl = TreeLocation.create(root, childPath + "/nonExisting");

        assertTrue(permissionProvider.isGranted(tl, Permissions.READ));
        assertFalse(permissionProvider.isGranted(tl, Permissions.REMOVE_NODE));
    }

    @Test
    public void testIsGrantedNonExistingParentTreeLocation() {
        TreeLocation tl = TreeLocation.create(root, childPath + "/nonExistingParent/nonExisting");

        assertTrue(permissionProvider.isGranted(tl, Permissions.READ));
        assertFalse(permissionProvider.isGranted(tl, Permissions.REMOVE_NODE));
    }

    @Test
    public void testIsGrantedAccessControlTreeLocation() throws Exception{
        TreeLocation tl = TreeLocation.create(root, PathUtils.concat(getTestSystemUser().getPath(), REP_PRINCIPAL_POLICY));
        assertFalse(permissionProvider.isGranted(tl, Permissions.READ));
    }
}