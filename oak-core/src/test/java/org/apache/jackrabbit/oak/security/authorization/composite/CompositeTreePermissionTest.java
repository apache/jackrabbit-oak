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

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.AND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompositeTreePermissionTest extends AbstractSecurityTest {

    private Tree rootTree;

    private AggregatedPermissionProvider fullScopeProvider;

    @Override
    public void before() throws Exception {
        super.before();

        Tree rootNode = root.getTree(PathUtils.ROOT_PATH);
        TreeUtil.addChild(rootNode, "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        root.commit();

        Root readOnlyRoot = getRootProvider().createReadOnlyRoot(root);
        rootTree = readOnlyRoot.getTree(PathUtils.ROOT_PATH);

        fullScopeProvider = new FullScopeProvider(readOnlyRoot);
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            root.getTree("/test").remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @NotNull
    CompositeAuthorizationConfiguration.CompositionType getCompositionType() {
        return AND;
    }

    @NotNull
    TreePermission createRootTreePermission(@NotNull AggregatedPermissionProvider... providers) {
        return CompositeTreePermission.create(rootTree, getTreeProvider(), new TreeTypeProvider(Context.DEFAULT), providers, getCompositionType());
    }

    private static void assertCompositeTreePermission(boolean expected, @NotNull TreePermission tp) {
        assertEquals(expected, tp instanceof CompositeTreePermission);
    }

    @Test
    public void testEmpty() {
        TreePermission rootTp = createRootTreePermission();
        assertSame(TreePermission.EMPTY, rootTp);
        assertFalse(rootTp.canRead());
    }

    @Test
    public void testSingle() {
        Class<? extends TreePermission> expected = fullScopeProvider.getTreePermission(rootTree, TreePermission.EMPTY).getClass();

        TreePermission rootTp = createRootTreePermission(fullScopeProvider);
        assertCompositeTreePermission(false, rootTp);
        assertEquals(expected, rootTp.getClass());

        TreePermission testTp = rootTp.getChildPermission("test", getTreeProvider().asNodeState(rootTree.getChild("test")));
        assertEquals(expected, testTp.getClass());
    }

    @Test
    public void testMultiple() {
        TreePermission rootTp = createRootTreePermission(fullScopeProvider, fullScopeProvider);
        assertCompositeTreePermission(true, rootTp);

        TreePermission testTp = rootTp.getChildPermission("test", getTreeProvider().asNodeState(rootTree.getChild("test")));
        assertCompositeTreePermission(true, testTp);
    }

    @Test
    public void testMultipleNoRecurse() {
        TreePermission rootTp = createRootTreePermission(new NoScopeProvider(root), new NoScopeProvider(root));
        assertCompositeTreePermission(true, rootTp);

        assertSame(TreePermission.EMPTY, rootTp.getChildPermission("test", getTreeProvider().asNodeState(rootTree.getChild("test"))));
    }

    @Test
    public void testMultipleToSingle() {
        TreePermission rootTp = createRootTreePermission(fullScopeProvider, new NoScopeProvider(root), new NoScopeProvider(root));
        assertCompositeTreePermission(true, rootTp);

        NodeState childState = getTreeProvider().asNodeState(rootTree.getChild("test"));
        TreePermission testTp = rootTp.getChildPermission("test", childState);
        TreePermission expected = fullScopeProvider.getTreePermission(rootTree, TreePermission.EMPTY).getChildPermission("test", childState);
        assertEquals(expected.getClass(), testTp.getClass());
    }

    @Test
    public void testCanRead() throws Exception {
        TreePermission rootTp = createRootTreePermission(fullScopeProvider, fullScopeProvider);

        Field f = CompositeTreePermission.class.getDeclaredField("canRead");
        f.setAccessible(true);

        Object canRead = f.get(rootTp);
        assertNull(canRead);

        rootTp.canRead();

        canRead = f.get(rootTp);
        assertNotNull(canRead);
    }

    @Test
    public void testCanReadTwiceAllowed() {
        TreePermission mockTp = when(mock(TreePermission.class).canRead()).thenReturn(true).getMock();
        AggregatedPermissionProvider mockPP = mock(AggregatedPermissionProvider.class);
        when(mockPP.getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(mockTp);
        when(mockPP.supportedPermissions(mockTp, null, Permissions.READ_NODE)).thenReturn(Permissions.ALL);

        TreePermission rootTp = createRootTreePermission(mockPP, fullScopeProvider);

        rootTp.canRead();
        rootTp.canRead();

        verify(mockTp, times(1)).canRead();
        verify(mockTp, never()).canReadProperties();
        verify(mockTp, never()).canRead(any(PropertyState.class));
        verify(mockTp, never()).canReadAll();

        verify(mockPP, times(1)).getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class));
        verify(mockPP, times(1)).supportedPermissions(mockTp, null, Permissions.READ_NODE);
    }

    @Test
    public void testCanReadTwiceDenied() {
        TreePermission mockTp = when(mock(TreePermission.class).canRead()).thenReturn(false).getMock();
        AggregatedPermissionProvider mockPP = mock(AggregatedPermissionProvider.class);
        when(mockPP.getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(mockTp);
        when(mockPP.supportedPermissions(mockTp, null, Permissions.READ_NODE)).thenReturn(Permissions.ALL);

        TreePermission rootTp = createRootTreePermission(mockPP, fullScopeProvider);

        rootTp.canRead();
        rootTp.canRead();

        verify(mockTp, times(1)).canRead();
        verify(mockTp, never()).canReadProperties();
        verify(mockTp, never()).canRead(any(PropertyState.class));
        verify(mockTp, never()).canReadAll();

        verify(mockPP, times(1)).getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class));
        verify(mockPP, times(1)).supportedPermissions(mockTp, null, Permissions.READ_NODE);
    }

    @Test
    public void testCanReadUnsupportedPermission() {
        TreePermission mockTp = when(mock(TreePermission.class).canRead()).thenReturn(true).getMock();
        AggregatedPermissionProvider mockPP = mock(AggregatedPermissionProvider.class);
        when(mockPP.getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(mockTp);
        when(mockPP.supportedPermissions(mockTp, null, Permissions.READ_NODE)).thenReturn(Permissions.NO_PERMISSION);

        TreePermission rootTp = createRootTreePermission(mockPP, mockPP);

        assertFalse(rootTp.canRead());

        verify(mockTp, never()).canRead();
        verify(mockPP, times(2)).getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class));
        verify(mockPP, times(2)).supportedPermissions(mockTp, null, Permissions.READ_NODE);
    }

    @Test
    public void testCanReadPropertyTwice() {
        PropertyState ps = PropertyStates.createProperty("propName", "value");

        TreePermission mockTp = when(mock(TreePermission.class).canReadProperties()).thenReturn(false).getMock();
        when(mockTp.canRead(ps)).thenReturn(true);

        AggregatedPermissionProvider mockPP = mock(AggregatedPermissionProvider.class);
        when(mockPP.getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(mockTp);
        when(mockPP.supportedPermissions(mockTp, null, Permissions.READ_PROPERTY)).thenReturn(Permissions.ALL);
        when(mockPP.supportedPermissions(mockTp, ps, Permissions.READ_PROPERTY)).thenReturn(Permissions.ALL);

        TreePermission rootTp = createRootTreePermission(mockPP, fullScopeProvider);

        rootTp.canRead(ps);
        rootTp.canRead(ps);

        verify(mockTp, never()).canRead();
        verify(mockTp, times(1)).canReadProperties();
        verify(mockTp, times(2)).canRead(ps);
        verify(mockTp, never()).canReadAll();
    }

    @Test
    public void testCanReadProperties() {
        PropertyState ps = PropertyStates.createProperty("propName", "value");

        TreePermission mockTp = when(mock(TreePermission.class).canReadProperties()).thenReturn(true).getMock();
        when(mockTp.canRead(ps)).thenReturn(false);

        AggregatedPermissionProvider mockPP = mock(AggregatedPermissionProvider.class);
        when(mockPP.getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(mockTp);
        when(mockPP.supportedPermissions(mockTp, null, Permissions.READ_PROPERTY)).thenReturn(Permissions.ALL);

        TreePermission rootTp = createRootTreePermission(mockPP, fullScopeProvider);

        rootTp.canReadProperties();
        rootTp.canReadProperties();
        rootTp.canRead(ps);

        verify(mockTp, never()).canRead();
        verify(mockTp, times(1)).canReadProperties();
        verify(mockTp, never()).canRead(ps);
        verify(mockTp, never()).canReadAll();
    }

    @Test
    public void testCanReadAll() {
        TreePermission mockTp = when(mock(TreePermission.class).canReadAll()).thenReturn(true).getMock();
        AggregatedPermissionProvider mockPP = mock(AggregatedPermissionProvider.class);
        when(mockPP.getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(mockTp);
        when(mockPP.supportedPermissions(mockTp, null, Permissions.READ_NODE)).thenReturn(Permissions.ALL);

        TreePermission rootTp = createRootTreePermission(mockPP, fullScopeProvider);

        assertFalse(rootTp.canReadAll());
        verify(mockTp, never()).canReadAll();
    }

    @Test
    public void testIsGrantedUncoveredPermissions() {
        TreePermission mockTp = when(mock(TreePermission.class).isGranted(anyLong())).thenReturn(true).getMock();

        AggregatedPermissionProvider mockPP = mock(AggregatedPermissionProvider.class);
        when(mockPP.getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(mockTp);
        when(mockPP.supportedPermissions(mockTp, null, Permissions.WRITE)).thenReturn(Permissions.SET_PROPERTY);

        TreePermission rootTp = createRootTreePermission(mockPP, mockPP);

        assertFalse(rootTp.isGranted(Permissions.WRITE));

        verify(mockTp, times(2)).isGranted(Permissions.SET_PROPERTY);
        verify(mockTp, never()).isGranted(Permissions.WRITE);
        verify(mockPP, times(2)).supportedPermissions(mockTp, null, Permissions.WRITE);
    }

    @Test
    public void testParentNoRecourse() {
        TreePermission rootTp = createRootTreePermission(new NoScopeProvider(root));
        assertSame(TreePermission.NO_RECOURSE, rootTp);
    }

    @Test
    public void testCreateWithTreeType() throws Exception {
        TreePermission mockTp = mock(TreePermission.class);
        AggregatedPermissionProvider mockPP = mock(AggregatedPermissionProvider.class);
        when(mockPP.getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(mockTp);

        TreePermission parent = createRootTreePermission(mockPP, fullScopeProvider);
        assertCompositeTreePermission(true, parent);

        Tree t = rootTree.getChild("test");
        TreePermission tp = CompositeTreePermission.create(t, getTreeProvider(), (CompositeTreePermission) parent, TreeType.INTERNAL);
        assertCompositeTreePermission(true, tp);

        Field typeF = CompositeTreePermission.class.getDeclaredField("type");
        typeF.setAccessible(true);
        assertSame(TreeType.INTERNAL, typeF.get(tp));

        verify(mockPP, times(1)).getTreePermission(rootTree, TreeType.DEFAULT, TreePermission.EMPTY);
        verify(mockPP, times(1)).getTreePermission(t, TreeType.INTERNAL, mockTp);
        verify(mockPP, never()).getTreePermission(t, TreeType.DEFAULT, parent);
    }

    @Test
    public void testCreateSingleWithInvalidParent() {
        AggregatedPermissionProvider mockPP = mock(AggregatedPermissionProvider.class);
        when(mockPP.getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(TreePermission.NO_RECOURSE);

        TreePermission parent = createRootTreePermission(mockPP, fullScopeProvider);
        Tree t = rootTree.getChild("test");
        TreePermission tp = CompositeTreePermission.create(t, getTreeProvider(), (CompositeTreePermission) parent, TreeType.DEFAULT);
        assertCompositeTreePermission(false, tp);
    }

    @Test
    public void testCreateMultipleWithInvalidParent() {
        AggregatedPermissionProvider mockPP = mock(AggregatedPermissionProvider.class);
        when(mockPP.getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(TreePermission.NO_RECOURSE);

        TreePermission parent = createRootTreePermission(mockPP, fullScopeProvider, fullScopeProvider);
        Tree t = rootTree.getChild("test");
        TreePermission tp = CompositeTreePermission.create(t, getTreeProvider(), (CompositeTreePermission) parent, TreeType.DEFAULT);
        assertCompositeTreePermission(true, tp);
    }

    @Test
    public void testCreateWithInvalidParent() {
        AggregatedPermissionProvider mockPP = mock(AggregatedPermissionProvider.class);
        when(mockPP.getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(TreePermission.NO_RECOURSE);

        TreePermission parent = createRootTreePermission(mockPP, mockPP, mockPP);
        Tree t = rootTree.getChild("test");
        TreePermission tp = CompositeTreePermission.create(t, getTreeProvider(), (CompositeTreePermission) parent, TreeType.DEFAULT);
        assertSame(TreePermission.EMPTY, tp);
    }
}
