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

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.security.Principal;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.jcr.Session.ACTION_READ;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_LOCK_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_VERSION_MANAGEMENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PermissionProviderVersionTest extends AbstractPrincipalBasedTest {

    private Principal testPrincipal;
    private PrincipalBasedPermissionProvider permissionProvider;

    private String contentPath;
    private String childPath;
    private String grandchildPath;

    @Before
    public void before() throws Exception {
        super.before();

        testPrincipal = getTestSystemUser().getPrincipal();
        setupContentTrees(TEST_OAK_PATH);

        contentPath = PathUtils.getAncestorPath(TEST_OAK_PATH, 3);
        childPath = PathUtils.getAncestorPath(TEST_OAK_PATH, 2);
        grandchildPath = PathUtils.getAncestorPath(TEST_OAK_PATH, 1);

        // setup permissions on childPath + TEST_OAK_PATH
        PrincipalPolicyImpl policy = setupPrincipalBasedAccessControl(testPrincipal, getNamePathMapper().getJcrPath(childPath), JCR_READ);
        addPrincipalBasedEntry(policy, getNamePathMapper().getJcrPath(TEST_OAK_PATH), PrivilegeConstants.JCR_VERSION_MANAGEMENT);

        // versionabel nodes: contentPath + grandChildPath + TEST_OAK_PATH
        // -> 1 TEST_OAK_PATH node hold policy, grandchildPath get it inherited and contentPath has no permissions granted
        Tree typeRoot = root.getTree(NodeTypeConstants.NODE_TYPES_PATH);
        for (String path : new String[] {contentPath, grandchildPath, TEST_OAK_PATH}) {
            Tree versionable = root.getTree(path);
            TreeUtil.addMixin(root.getTree(path), NodeTypeConstants.MIX_VERSIONABLE, typeRoot, "uid");
        }
        root.commit();

        // force creation of a new version for grandchildPath, TEST_OAK_PATH but not for contentPath
        // removing tree for contentPath will then also result in removal of VH upon removal.
        for (String path : new String[] {grandchildPath, TEST_OAK_PATH}) {
            root.getTree(path).setProperty(JCR_ISCHECKEDOUT, false);
            root.commit();
            root.getTree(path).setProperty(JCR_ISCHECKEDOUT, true);
            root.commit();
        }

        permissionProvider = createPermissionProvider(root, testPrincipal);
    }

    @Override
    protected NamePathMapper getNamePathMapper() {
        return NamePathMapper.DEFAULT;
    }

    @NotNull
    private String getVersionPath(@NotNull String versionablePath, boolean history) {
        Tree versionable = root.getTree(versionablePath);
        String path;
        if (history) {
            path = new IdentifierManager(root).getPath(versionable.getProperty(JCR_VERSIONHISTORY));
        } else {
            path = new IdentifierManager(root).getPath(versionable.getProperty(JCR_BASEVERSION));
        }
        checkNotNull(path);
        return path;
    }

    @NotNull
    private Tree getVersionTree(@NotNull String versionablePath, boolean history) {
        Tree versionable = root.getTree(versionablePath);
        PropertyState reference;
        if (history) {
            reference = versionable.getProperty(JCR_VERSIONHISTORY);
        } else {
            reference = versionable.getProperty(JCR_BASEVERSION);
        }
        Tree t = new IdentifierManager(root).getTree(reference.getValue(Type.STRING));
        checkNotNull(t);
        return t;
    }

    @Test
    public void testGetTreePermission() {
        Tree tree = getRootProvider().createReadOnlyRoot(root).getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(tree, TreePermission.EMPTY);

        for (String elem : PathUtils.elements(getVersionPath(TEST_OAK_PATH, false))) {
            tree = tree.getChild(elem);
            tp = permissionProvider.getTreePermission(tree, tp);
        }

        assertTrue(tp instanceof AbstractTreePermission);
        AbstractTreePermission atp = (AbstractTreePermission) tp;
        assertSame(TreeType.VERSION, atp.getType());
        // tree must point to versionable node and NOT to the version tree
        assertNotSame(tree, atp.getTree());
        assertEquals(TEST_OAK_PATH, atp.getTree().getPath());
    }

    @Test
    public void testGetTreePermissionVersionableNodeRemoved() throws Exception {
        String versionPath = getVersionPath(TEST_OAK_PATH, false);

        root.getTree(TEST_OAK_PATH).remove();
        root.commit();
        permissionProvider.refresh();

        Tree tree = getRootProvider().createReadOnlyRoot(root).getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(tree, TreePermission.EMPTY);

        for (String elem : PathUtils.elements(versionPath)) {
            tree = tree.getChild(elem);
            tp = permissionProvider.getTreePermission(tree, tp);
        }

        assertTrue(tp instanceof AbstractTreePermission);
        AbstractTreePermission atp = (AbstractTreePermission) tp;
        assertSame(TreeType.VERSION, atp.getType());
        // tree must point to non-existing versionable node and NOT to the version tree
        assertNotSame(tree, atp.getTree());
        assertFalse(atp.getTree().exists());
        assertEquals(TEST_OAK_PATH, atp.getTree().getPath());
    }

    @Test
    public void testGetTreePermissionVersionHistoryRemoved() throws Exception {
        String vhPath = getVersionPath(contentPath, true);

        root.getTree(contentPath).remove();
        root.commit();
        permissionProvider.refresh();

        Tree tree = getRootProvider().createReadOnlyRoot(root).getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(tree, TreePermission.EMPTY);

        for (String elem : PathUtils.elements(vhPath)) {
            tree = tree.getChild(elem);
            tp = permissionProvider.getTreePermission(tree, tp);
        }

        assertSame(TreePermission.EMPTY, tp);
    }

    @Test
    public void testIsGranted() throws Exception {
        assertFalse(permissionProvider.isGranted(getVersionTree(contentPath, true), null, Permissions.READ_NODE));
        assertFalse(permissionProvider.isGranted(getVersionTree(grandchildPath, true), null, Permissions.READ_NODE|Permissions.VERSION_MANAGEMENT));
        assertFalse(permissionProvider.isGranted(getVersionTree(TEST_OAK_PATH, true), null, Permissions.READ|Permissions.WRITE));


        assertTrue(permissionProvider.isGranted(getVersionTree(grandchildPath, true), null, Permissions.READ_NODE));
        assertTrue(permissionProvider.isGranted(getVersionTree(TEST_OAK_PATH, true), null, Permissions.READ|Permissions.VERSION_MANAGEMENT));
    }

    @Test
    public void testIsGrantedWithProperty() throws Exception {
        Tree t = getVersionTree(contentPath, true);
        assertFalse(permissionProvider.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));

        t = getVersionTree(grandchildPath, false);
        assertFalse(permissionProvider.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY|Permissions.VERSION_MANAGEMENT));

        t = getVersionTree(TEST_OAK_PATH, true);
        assertFalse(permissionProvider.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY|Permissions.WRITE));


        t = getVersionTree(grandchildPath, true);
        assertTrue(permissionProvider.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));

        t = getVersionTree(TEST_OAK_PATH, false);
        assertTrue(permissionProvider.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY|Permissions.VERSION_MANAGEMENT));
    }

    @Test
    public void testIsGrantedVersionableTreeRemoved() throws Exception {
        String versionPath = getVersionPath(TEST_OAK_PATH, false);

        root.getTree(TEST_OAK_PATH).remove();
        root.commit();
        permissionProvider.refresh();

        Tree versionTree = root.getTree(versionPath);
        assertTrue(versionTree.exists());

        // permissions not affected as they are stored with the principal.
        assertTrue(permissionProvider.isGranted(versionTree, null, Permissions.READ|Permissions.VERSION_MANAGEMENT));
    }

    @Test
    public void testIsGrantedVersionHistoryRemoved() throws Exception {
        String vhPath = getVersionPath(contentPath, true);

        root.getTree(contentPath).remove();
        root.commit();
        permissionProvider.refresh();

        Tree vhTree = root.getTree(vhPath);
        assertFalse(vhTree.exists());

        // permissions affected because unable to resolve versionable tree
        assertFalse(permissionProvider.isGranted(vhTree, null, Permissions.READ|Permissions.VERSION_MANAGEMENT));
    }

    @Test
    public void testIsGrantedTreeLocation() throws Exception {
        TreeLocation tl = TreeLocation.create(root, getVersionPath(contentPath, false));
        assertFalse(permissionProvider.isGranted(tl, Permissions.READ_NODE));

        tl = TreeLocation.create(root, getVersionPath(grandchildPath, true));
        assertTrue(permissionProvider.isGranted(tl, Permissions.READ));

        tl = TreeLocation.create(root, getVersionPath(TEST_OAK_PATH, false));
        assertTrue(permissionProvider.isGranted(tl, Permissions.READ|Permissions.VERSION_MANAGEMENT));
    }

    @Test
    public void testIsGrantedByPath() throws Exception {
        assertFalse(permissionProvider.isGranted(getVersionPath(contentPath, false), ACTION_READ));
        assertFalse(permissionProvider.isGranted(getVersionPath(grandchildPath, true), Permissions.getString(Permissions.READ|Permissions.VERSION_MANAGEMENT)));
        assertFalse(permissionProvider.isGranted(getVersionPath(TEST_OAK_PATH, false), Permissions.getString(Permissions.READ|Permissions.WRITE)));

        assertTrue(permissionProvider.isGranted(getVersionPath(grandchildPath, false), ACTION_READ));
        assertTrue(permissionProvider.isGranted(getVersionPath(TEST_OAK_PATH, true), Permissions.getString(Permissions.READ|Permissions.VERSION_MANAGEMENT)));
    }

    @Test
    public void testGetPrivileges() throws Exception {
        assertTrue(permissionProvider.getPrivileges(getVersionTree(contentPath, true)).isEmpty());
        assertEquals(Sets.newHashSet(JCR_READ), permissionProvider.getPrivileges(getVersionTree(grandchildPath, false)));
        assertEquals(Sets.newHashSet(JCR_READ, JCR_VERSION_MANAGEMENT), permissionProvider.getPrivileges(getVersionTree(TEST_OAK_PATH, true)));
    }

    @Test
    public void testGetPrivilegesVersionableTreeRemoved() throws Exception {
        String versionPath = getVersionPath(TEST_OAK_PATH, false);
        String versionHPath = getVersionPath(TEST_OAK_PATH, true);
        assertTrue(root.getTree(versionPath).exists());

        root.getTree(TEST_OAK_PATH).remove();
        root.commit();
        permissionProvider.refresh();

        Tree versionTree = root.getTree(versionPath);
        assertTrue(versionTree.exists());

        // permissions not affected (as long as no restrictions involved) due to the fact that permissions are not
        // stored with the versionable node.
        assertEquals(Sets.newHashSet(JCR_READ, JCR_VERSION_MANAGEMENT), permissionProvider.getPrivileges(versionTree));
    }

    @Test
    public void testGetPrivilegesVersionHistoryRemoved() throws Exception {
        String vhPath = getVersionPath(contentPath, true);
        assertTrue(root.getTree(vhPath).exists());

        root.getTree(contentPath).remove();
        root.commit();
        permissionProvider.refresh();

        Tree vhTree = root.getTree(vhPath);
        assertFalse(vhTree.exists());
        assertTrue(permissionProvider.getPrivileges(vhTree).isEmpty());
    }

    @Test
    public void testHasPrivileges() throws Exception {
        assertFalse(permissionProvider.hasPrivileges(getVersionTree(contentPath, false), JCR_READ));
        assertFalse(permissionProvider.hasPrivileges(getVersionTree(grandchildPath, true), JCR_VERSION_MANAGEMENT));
        assertFalse(permissionProvider.hasPrivileges(getVersionTree(TEST_OAK_PATH, false), JCR_READ, JCR_LOCK_MANAGEMENT));

        assertTrue(permissionProvider.hasPrivileges(getVersionTree(grandchildPath, false), JCR_READ));
        assertTrue(permissionProvider.hasPrivileges(getVersionTree(TEST_OAK_PATH, true), JCR_READ, JCR_VERSION_MANAGEMENT));
    }
}