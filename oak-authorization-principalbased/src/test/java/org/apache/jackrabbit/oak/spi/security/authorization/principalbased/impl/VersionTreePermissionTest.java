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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.identifier.IdentifierManager;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.version.VersionConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.security.Principal;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.JCR_BASEVERSION;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENNODE;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_VERSIONHISTORY;
import static org.apache.jackrabbit.JcrConstants.NT_VERSIONEDCHILD;
import static org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl.MockUtility.mockNodeState;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class VersionTreePermissionTest extends AbstractPrincipalBasedTest {
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
        // -> TEST_OAK_PATH versionable node holds policy, grandchildPath get permissions inherited, and contentPath has no permissions granted
        Tree typeRoot = root.getTree(NodeTypeConstants.NODE_TYPES_PATH);
        for (String path : new String[] {contentPath, grandchildPath, TEST_OAK_PATH}) {
            Tree versionable = root.getTree(path);
            TreeUtil.addMixin(root.getTree(path), NodeTypeConstants.MIX_VERSIONABLE, typeRoot, "uid");
        }
        root.commit();

        // force creation of a new versions (except for TEST_OAK_PATH)
        for (String path : new String[] {contentPath, grandchildPath}) {
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
    private String getPathFromReference(@NotNull String treePath, @NotNull String refProperty) {
        Tree tree = root.getTree(treePath);
        String path = new IdentifierManager(root).getPath(tree.getProperty(refProperty));
        checkNotNull(path);
        return path;
    }

    @NotNull
    private AbstractTreePermission getTreePermission(@NotNull String path) {
        Tree tree = root.getTree(PathUtils.ROOT_PATH);
        TreePermission tp = permissionProvider.getTreePermission(tree, TreePermission.EMPTY);
        for (String elem : PathUtils.elements(path)) {
            tree = tree.getChild(elem);
            tp = permissionProvider.getTreePermission(tree, tp);
        }
        assertTrue(tp instanceof AbstractTreePermission);
        return (AbstractTreePermission) tp;
    }

    @Test
    public void testGetTreePermissionVersionHistory() {
        String vhPath = getPathFromReference(TEST_OAK_PATH, JCR_VERSIONHISTORY);
        AbstractTreePermission tp = getTreePermission(vhPath);

        assertSame(TreeType.VERSION, tp.getType());
        assertNotEquals(vhPath, tp.getTree().getPath());
        assertSame(permissionProvider, tp.getPermissionProvider());
    }

    @Test
    public void testGetTreePermissionVersion() {
        String versionPath = getPathFromReference(TEST_OAK_PATH, JCR_BASEVERSION);
        AbstractTreePermission tp = getTreePermission(versionPath);

        assertSame(TreeType.VERSION, tp.getType());
        assertNotEquals(versionPath, tp.getTree().getPath());
        assertSame(permissionProvider, tp.getPermissionProvider());
    }

    @Test
    public void testGetChildPermissionRootVersionNode() {
        String vhPath = getPathFromReference(contentPath, JCR_VERSIONHISTORY);
        AbstractTreePermission tp = getTreePermission(vhPath);

        TreePermission rootversionTp = tp.getChildPermission(VersionConstants.JCR_ROOTVERSION, mockNodeState(VersionConstants.NT_VERSION));
        assertTrue(rootversionTp instanceof AbstractTreePermission);

        assertSame(tp.getTree(), ((AbstractTreePermission) rootversionTp).getTree());
    }

    @Test
    public void testGetChildPermissionVersion1Node() {
        String vhPath = getPathFromReference(TEST_OAK_PATH, JCR_VERSIONHISTORY);
        AbstractTreePermission tp = getTreePermission(vhPath);

        TreePermission versionTp = tp.getChildPermission("1.0", mockNodeState(VersionConstants.NT_VERSION));
        assertTrue(versionTp instanceof AbstractTreePermission);

        assertSame(tp.getTree(), ((AbstractTreePermission) versionTp).getTree());
    }

    @Test
    public void testGetChildPermissionLabelsNode() {
        String vhPath = getPathFromReference(contentPath, JCR_VERSIONHISTORY);
        AbstractTreePermission tp = getTreePermission(vhPath);

        TreePermission labelsTp = tp.getChildPermission(VersionConstants.JCR_VERSIONLABELS, mockNodeState(VersionConstants.NT_VERSIONLABELS));
        assertTrue(labelsTp instanceof AbstractTreePermission);

        assertSame(tp.getTree(), ((AbstractTreePermission) labelsTp).getTree());
    }

    @Test
    public void testGetChildPermissionFrozenNode() {
        String v1Path = getPathFromReference(grandchildPath, JCR_BASEVERSION);
        Tree v1Tree = root.getTree(v1Path);
        assertTrue(v1Tree.exists());

        AbstractTreePermission tp = getTreePermission(v1Path);
        TreePermission frozenTp = tp.getChildPermission(VersionConstants.JCR_FROZENNODE, getTreeProvider().asNodeState(v1Tree.getChild(JCR_FROZENNODE)));

        assertSame(tp.getTree(), ((AbstractTreePermission) frozenTp).getTree());
    }

    @Test
    public void testVersionedChildNode() {
        String path = PathUtils.concat(getPathFromReference(contentPath, JCR_BASEVERSION), VersionConstants.JCR_FROZENNODE, PathUtils.getName(childPath));
        AbstractTreePermission tp = getTreePermission(path);

        String versionedChildName = PathUtils.getName(grandchildPath);
        String versionedChildPath = PathUtils.concat(path, versionedChildName);
        Tree versionedChildTree = root.getTree(versionedChildPath);
        assertEquals(NT_VERSIONEDCHILD, TreeUtil.getPrimaryTypeName(versionedChildTree));

        TreePermission versionedChildTp = tp.getChildPermission(versionedChildName, getTreeProvider().asNodeState(versionedChildTree));
        assertTrue(versionedChildTp instanceof AbstractTreePermission);

        Tree childVersionHistory = root.getTree(getPathFromReference(versionedChildPath, VersionConstants.JCR_CHILD_VERSION_HISTORY));
        assertTrue(childVersionHistory.exists());
        Tree versionable = ReadOnlyVersionManager.getInstance(root, getNamePathMapper()).getVersionable(childVersionHistory, root.getContentSession().getWorkspaceName());

        assertEquals(versionable.getPath(), ((AbstractTreePermission) versionedChildTp).getTree().getPath());
    }

    @Test
    public void testVersionedChildNodePointingToRemovedVH() throws CommitFailedException {
        root.getTree(TEST_OAK_PATH).remove();
        root.commit();
        permissionProvider.refresh();

        String path = PathUtils.concat(getPathFromReference(grandchildPath, JCR_BASEVERSION), JCR_FROZENNODE);
        AbstractTreePermission tp = getTreePermission(path);

        String versionedChildName = PathUtils.getName(TEST_OAK_PATH);
        String versionedChildPath = PathUtils.concat(path, versionedChildName);
        Tree versionedChildTree = root.getTree(versionedChildPath);
        assertEquals(NT_VERSIONEDCHILD, TreeUtil.getPrimaryTypeName(versionedChildTree));

        TreePermission versionedChildTp = tp.getChildPermission(versionedChildName, getTreeProvider().asNodeState(versionedChildTree));
        assertTrue(versionedChildTp instanceof AbstractTreePermission);

        assertEquals(TEST_OAK_PATH, ((AbstractTreePermission) versionedChildTp).getTree().getPath());
        assertTrue(versionedChildTp.canRead());
        // version-mgt permission still granted because it is not stored on the removed node
        assertTrue(versionedChildTp.isGranted(Permissions.VERSION_MANAGEMENT));
    }


    @Test
    public void testGetChildPermissionCopiedChild() {
        String frozenPath = PathUtils.concat(getPathFromReference(contentPath, JCR_BASEVERSION), JCR_FROZENNODE);
        AbstractTreePermission tp = getTreePermission(frozenPath);

        String copiedChildName = PathUtils.getName(childPath);
        Tree copiedChildTree = root.getTree(PathUtils.concat(frozenPath, copiedChildName));

        TreePermission copiedChildTp = tp.getChildPermission(copiedChildName, getTreeProvider().asNodeState(copiedChildTree));
        assertTrue(copiedChildTp instanceof AbstractTreePermission);

        assertEquals(childPath, ((AbstractTreePermission) copiedChildTp).getTree().getPath());
    }

    @Test
    public void testIsGrantedVersionHistoryNode() {
        String vhPath = getPathFromReference(contentPath, JCR_VERSIONHISTORY);
        AbstractTreePermission tp = getTreePermission(vhPath);
        assertFalse(tp.canRead());
        assertFalse(tp.isGranted(Permissions.VERSION_MANAGEMENT));

        vhPath = getPathFromReference(grandchildPath, JCR_VERSIONHISTORY);
        tp = getTreePermission(vhPath);
        assertTrue(tp.canRead());
        assertFalse(tp.isGranted(Permissions.VERSION_MANAGEMENT));

        vhPath = getPathFromReference(TEST_OAK_PATH, JCR_VERSIONHISTORY);
        tp = getTreePermission(vhPath);
        assertTrue(tp.canRead());
        assertTrue(tp.isGranted(Permissions.VERSION_MANAGEMENT));
    }

    @Test
    public void testIsGrantedVersionNode() {
        String vPath = getPathFromReference(contentPath, JCR_BASEVERSION);
        AbstractTreePermission tp = getTreePermission(vPath);
        assertFalse(tp.canRead());
        assertFalse(tp.isGranted(Permissions.VERSION_MANAGEMENT));

        vPath = getPathFromReference(grandchildPath, JCR_BASEVERSION);
        tp = getTreePermission(vPath);
        assertTrue(tp.canRead());
        assertFalse(tp.isGranted(Permissions.VERSION_MANAGEMENT));

        vPath = getPathFromReference(TEST_OAK_PATH, JCR_BASEVERSION);
        tp = getTreePermission(vPath);
        assertTrue(tp.canRead());
        assertTrue(tp.isGranted(Permissions.VERSION_MANAGEMENT));
    }

    @Test
    public void testIsGrantedFrozenNode() {
        String frozenPath = PathUtils.concat(getPathFromReference(contentPath, JCR_BASEVERSION), JCR_FROZENNODE);
        AbstractTreePermission tp = getTreePermission(frozenPath);
        assertFalse(tp.canRead());
        assertFalse(tp.isGranted(Permissions.VERSION_MANAGEMENT));

        frozenPath = PathUtils.concat(getPathFromReference(grandchildPath, JCR_BASEVERSION), JCR_FROZENNODE);
        tp = getTreePermission(frozenPath);
        assertTrue(tp.canRead());
        assertFalse(tp.isGranted(Permissions.VERSION_MANAGEMENT));

        frozenPath = PathUtils.concat(getPathFromReference(TEST_OAK_PATH, JCR_BASEVERSION), JCR_FROZENNODE);
        tp = getTreePermission(frozenPath);
        assertTrue(tp.canRead());
        assertTrue(tp.isGranted(Permissions.VERSION_MANAGEMENT));
    }

    @Test
    public void testIsGrantedCopiedChild() {
        String copiedChildPath = PathUtils.concat(getPathFromReference(contentPath, JCR_BASEVERSION), JCR_FROZENNODE, PathUtils.getName(childPath));
        assertTrue(root.getTree(copiedChildPath).exists());

        AbstractTreePermission tp = getTreePermission(copiedChildPath);
        assertTrue(tp.canRead());
        assertFalse(tp.isGranted(Permissions.VERSION_MANAGEMENT));
    }

    @Test
    public void testIsGrantedVersionedChild() {
        String versionedChildPath = PathUtils.concat(getPathFromReference(grandchildPath, JCR_BASEVERSION), JCR_FROZENNODE, PathUtils.getName(TEST_OAK_PATH));
        assertTrue(root.getTree(versionedChildPath).exists());

        AbstractTreePermission tp = getTreePermission(versionedChildPath);
        assertTrue(tp.canRead());
        assertTrue(tp.isGranted(Permissions.VERSION_MANAGEMENT));
    }
}