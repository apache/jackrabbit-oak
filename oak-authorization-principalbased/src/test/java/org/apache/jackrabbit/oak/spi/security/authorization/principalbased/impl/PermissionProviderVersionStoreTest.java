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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.Value;
import java.security.Principal;
import java.util.Map;
import java.util.Set;

import static javax.jcr.Session.ACTION_READ;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_NT_NAMES;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.REP_VERSIONSTORAGE;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.VERSION_STORE_PATH;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PermissionProviderVersionStoreTest extends AbstractPrincipalBasedTest {

    private Principal testPrincipal;
    private PrincipalBasedPermissionProvider permissionProvider;

    @Before
    public void before() throws Exception {
        super.before();

        testPrincipal = getTestSystemUser().getPrincipal();
        setupContentTrees(TEST_OAK_PATH);

        String contentPath = PathUtils.getAncestorPath(TEST_OAK_PATH, 3);
        String andPath = PathUtils.getAncestorPath(TEST_OAK_PATH, 3);

        Tree typeRoot = root.getTree(NodeTypeConstants.NODE_TYPES_PATH);
        for (String path : new String[] {contentPath, andPath, TEST_OAK_PATH}) {
            TreeUtil.addMixin(root.getTree(path), NodeTypeConstants.MIX_VERSIONABLE, typeRoot, "uid");
        }
        root.commit();

        permissionProvider = createPermissionProvider(root, testPrincipal);
    }

    @Override
    protected NamePathMapper getNamePathMapper() {
        return NamePathMapper.DEFAULT;
    }

    private void grantReadOnVersionStoreTrees() throws Exception {
        JackrabbitAccessControlManager jacm = getAccessControlManager(root);
        PrincipalPolicyImpl policy = getPrincipalPolicyImpl(testPrincipal, jacm);
        Map<String, Value[]> restr = Map.of(REP_NT_NAMES, new Value[] {getValueFactory(root).createValue(REP_VERSIONSTORAGE, PropertyType.NAME)});
        policy.addEntry(PathUtils.ROOT_PATH, privilegesFromNames(PrivilegeConstants.JCR_READ), Map.of(), restr);
        jacm.setPolicy(policy.getPath(), policy);
        root.commit();

        permissionProvider.refresh();
    }

    @Test
    public void testGetTreePermission() {
        Tree tree = getRootProvider().createReadOnlyRoot(root).getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(tree, TreePermission.EMPTY);
        for (String elem : PathUtils.elements(VERSION_STORE_PATH)) {
            tree = tree.getChild(elem);
            tp = permissionProvider.getTreePermission(tree, tp);
        }

        assertTrue(tp instanceof AbstractTreePermission);
        AbstractTreePermission atp = (AbstractTreePermission) tp;
        assertSame(TreeType.VERSION, atp.getType());
        // must be 'regular' tree permission without extra versionable tree
        assertSame(tree, atp.getTree());
    }

    @Test
    public void testGetTreePermissionFromNodeState() {
        Tree tree = getRootProvider().createReadOnlyRoot(root).getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(tree, TreePermission.EMPTY);

        NodeState ns = getTreeProvider().asNodeState(tree);
        for (String elem : PathUtils.elements(VERSION_STORE_PATH)) {
            ns = ns.getChildNode(elem);
            tp = permissionProvider.getTreePermission(elem, ns, (AbstractTreePermission) tp);
            assertTrue(tp instanceof AbstractTreePermission);
        }

        AbstractTreePermission atp = (AbstractTreePermission) tp;
        assertSame(TreeType.VERSION, atp.getType());
    }

    @Test
    public void testIsGranted() throws Exception {
        Tree versionStore = root.getTree(VERSION_STORE_PATH);
        assertFalse(permissionProvider.isGranted(versionStore, null, Permissions.READ_NODE));
        assertFalse(permissionProvider.isGranted(versionStore, versionStore.getProperty(JcrConstants.JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));

        grantReadOnVersionStoreTrees();

        assertTrue(permissionProvider.isGranted(versionStore, null, Permissions.READ_NODE));
        assertTrue(permissionProvider.isGranted(versionStore, versionStore.getProperty(JcrConstants.JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));
    }

    @Test
    public void testIsGrantedTreeLocation() throws Exception {
        TreeLocation tl = TreeLocation.create(root, VERSION_STORE_PATH);
        assertFalse(permissionProvider.isGranted(tl, Permissions.READ_NODE));

        grantReadOnVersionStoreTrees();

        assertTrue(permissionProvider.isGranted(tl, Permissions.READ));
    }

    @Test
    public void testIsGrantedPropertyLocation() throws Exception {
        TreeLocation tl = TreeLocation.create(root, VERSION_STORE_PATH).getChild(JcrConstants.JCR_PRIMARYTYPE);
        assertNotNull(tl.getProperty());

        assertFalse(permissionProvider.isGranted(tl, Permissions.READ_PROPERTY));

        grantReadOnVersionStoreTrees();

        assertTrue(permissionProvider.isGranted(tl, Permissions.READ_PROPERTY));
    }

    @Test
    public void testIsGrantedNonExistingLocation() throws Exception {
        TreeLocation tl = TreeLocation.create(root, VERSION_STORE_PATH + "/nonExisting");
        assertFalse(permissionProvider.isGranted(tl, Permissions.READ_NODE));

        grantReadOnVersionStoreTrees();

        assertFalse(permissionProvider.isGranted(tl, Permissions.READ_NODE));
    }

    @Test
    public void testIsGrantedByPath() throws Exception {
        assertFalse(permissionProvider.isGranted(VERSION_STORE_PATH, ACTION_READ));

        grantReadOnVersionStoreTrees();

        assertTrue(permissionProvider.isGranted(VERSION_STORE_PATH, ACTION_READ));
    }

    @Test
    public void testIsGrantedByNonExistingPath() throws Exception {
        assertFalse(permissionProvider.isGranted(VERSION_STORE_PATH + "/nonExisting", ACTION_READ));

        grantReadOnVersionStoreTrees();

        assertFalse(permissionProvider.isGranted(VERSION_STORE_PATH + "/nonExisting", ACTION_READ));
    }

    @Test
    public void testGetPrivileges() throws Exception {
        Tree versionStore = root.getTree(VERSION_STORE_PATH);
        assertTrue(permissionProvider.getPrivileges(versionStore).isEmpty());

        grantReadOnVersionStoreTrees();

        assertTrue(Iterables.elementsEqual(Set.of(PrivilegeConstants.JCR_READ), permissionProvider.getPrivileges(versionStore)));
    }

    @Test
    public void testHasPrivileges() throws Exception {
        Tree versionStore = root.getTree(VERSION_STORE_PATH);
        assertFalse(permissionProvider.hasPrivileges(versionStore, PrivilegeConstants.REP_READ_NODES));

        grantReadOnVersionStoreTrees();

        assertTrue(permissionProvider.hasPrivileges(versionStore, PrivilegeConstants.REP_READ_NODES, PrivilegeConstants.REP_READ_PROPERTIES));
    }
}