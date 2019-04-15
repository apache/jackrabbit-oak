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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import java.security.Principal;

import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.Constants.REP_PRINCIPAL_POLICY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class RegularTreePermissionTest extends AbstractPrincipalBasedTest {

    private PrincipalBasedPermissionProvider permissionProvider;

    @Before
    public void before() throws Exception {
        super.before();

        Principal principal = getTestSystemUser().getPrincipal();
        setupContentTrees(TEST_OAK_PATH);
        setupPrincipalBasedAccessControl(principal, testContentJcrPath, PrivilegeConstants.JCR_READ);
        root.commit();

        permissionProvider = createPermissionProvider(root, principal);
    }

    @Override
    protected NamePathMapper getNamePathMapper() {
        return NamePathMapper.DEFAULT;
    }

    @Test
    public void testGetTreePermissionRootTree() {
        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(rootTree, TreePermission.EMPTY);

        assertTrue(tp instanceof AbstractTreePermission);
        AbstractTreePermission atp = (AbstractTreePermission) tp;
        assertNotEquals(rootTree, atp.getTree());
        assertEquals(TreeType.DEFAULT, atp.getType());
    }

    @Test
    public void testGetTreePermissionReadOnlyRootTree() {
        Tree rootTree = getRootProvider().createReadOnlyRoot(root).getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(rootTree, TreeType.VERSION, mock(TreePermission.class));

        assertTrue(tp instanceof AbstractTreePermission);
        AbstractTreePermission atp = (AbstractTreePermission) tp;
        assertSame(rootTree, atp.getTree());
        assertEquals(TreeType.DEFAULT, atp.getType());
    }

    @Test
    public void testIsGrantedForRootTree() throws Exception {
        TreePermission tp = permissionProvider.getTreePermission(root.getTree(PathUtils.ROOT_PATH), TreeType.DEFAULT, mock(TreePermission.class));
        assertFalse(tp.isGranted(Permissions.READ));
    }

    @Test
    public void testCanReadForRootTree() throws Exception {
        TreePermission tp = permissionProvider.getTreePermission(root.getTree(PathUtils.ROOT_PATH), TreeType.DEFAULT, mock(TreePermission.class));
        assertFalse(tp.canRead());
        assertFalse(tp.canRead(MockUtility.createPrimaryTypeProperty(NodeTypeConstants.NT_REP_ROOT)));
    }

    @Test
    public void testRefreshReflectedOnTreePermission() throws Exception {
        TreePermission tp = permissionProvider.getTreePermission(root.getTree(PathUtils.ROOT_PATH), TreePermission.EMPTY);

        setupPrincipalBasedAccessControl(getTestSystemUser().getPrincipal(), PathUtils.ROOT_PATH, PrivilegeConstants.REP_READ_NODES);
        root.commit();
        permissionProvider.refresh();

        assertTrue(tp.canRead());
        assertFalse(tp.canRead(MockUtility.createPrimaryTypeProperty(NodeTypeConstants.NT_REP_ROOT)));
    }

    @Test
    public void testGetTreePermissionMockedParentPermission() throws Exception {
        Tree tree = getRootProvider().createReadOnlyRoot(root).getTree(getNamePathMapper().getOakPath(getTestSystemUser().getPath())).getChild(REP_PRINCIPAL_POLICY);
        assertTrue(tree.exists());
        TreePermission tp = permissionProvider.getTreePermission(tree, mock(TreePermission.class));

        assertTrue(tp instanceof AbstractTreePermission);
        AbstractTreePermission atp = (AbstractTreePermission) tp;
        assertSame(tree, atp.getTree());
        assertEquals(TreeType.ACCESS_CONTROL, atp.getType());
    }

    @Test
    public void testGetTreePermissionNonExistingTree() throws Exception {
        Tree tree = getRootProvider().createReadOnlyRoot(root).getTree("/nonExisting");
        assertFalse(tree.exists());

        TreePermission tp = permissionProvider.getTreePermission(tree, permissionProvider.getTreePermission(root.getTree(PathUtils.ROOT_PATH), TreePermission.EMPTY));

        assertTrue(tp instanceof AbstractTreePermission);
        AbstractTreePermission atp = (AbstractTreePermission) tp;
    }

    @Test
    public void testIsGrantedForTestTree() throws Exception {
        Tree tree = root.getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(tree, TreePermission.EMPTY);
        for (String elem : PathUtils.elements(TEST_OAK_PATH)) {
            tree = tree.getChild(elem);
            tp = permissionProvider.getTreePermission(tree, tp);
        }

        assertTrue(tp.isGranted(Permissions.READ));
        assertTrue(tp.isGranted(Permissions.READ, tree.getProperty(JcrConstants.JCR_PRIMARYTYPE)));

        assertFalse(tp.isGranted(Permissions.READ_ACCESS_CONTROL));
        assertFalse(tp.isGranted(Permissions.WRITE));
    }

    @Test
    public void testCanReadForTestTree() throws Exception {
        Tree tree = root.getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(tree, TreePermission.EMPTY);
        for (String elem : PathUtils.elements(TEST_OAK_PATH)) {
            tree = tree.getChild(elem);
            tp = permissionProvider.getTreePermission(tree, tp);
        }

        assertTrue(tp.canRead());
        assertTrue(tp.canRead(tree.getProperty(JcrConstants.JCR_PRIMARYTYPE)));
    }

    @Test
    public void testCanReadForTypeAccessControl() throws Exception {
        String prinipalPath = getNamePathMapper().getOakPath(getTestSystemUser().getPath());
        String policyPath = PathUtils.concat(prinipalPath, REP_PRINCIPAL_POLICY);

        Tree tree = root.getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(tree, TreePermission.EMPTY);
        for (String elem : PathUtils.elements(policyPath)) {
            tree = tree.getChild(elem);
            tp = permissionProvider.getTreePermission(tree, tp);
        }

        assertFalse(tp.canRead());

        setupPrincipalBasedAccessControl(getTestSystemUser().getPrincipal(), getTestSystemUser().getPath(), PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        root.commit();
        permissionProvider.refresh();

        assertTrue(tp.canRead());
    }

    @Test
    public void testGetChildTreePermission() {
        Tree readOnly = getRootProvider().createReadOnlyRoot(root).getTree(PathUtils.ROOT_PATH);
        TreePermission tp = (AbstractTreePermission) permissionProvider.getTreePermission(readOnly, TreePermission.EMPTY);
        NodeState ns = getTreeProvider().asNodeState(readOnly);
        for (String elem : PathUtils.elements(TEST_OAK_PATH)) {
            ns = ns.getChildNode(elem);
            tp = permissionProvider.getTreePermission(elem, ns, (AbstractTreePermission) tp);
            assertTrue(tp instanceof AbstractTreePermission);
            assertSame(TreeType.DEFAULT, ((AbstractTreePermission) tp).getType());
        }
    }
}