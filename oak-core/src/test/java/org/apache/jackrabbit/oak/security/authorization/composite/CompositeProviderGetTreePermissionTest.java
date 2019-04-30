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

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeProvider;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.AND;
import static org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration.CompositionType.OR;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompositeProviderGetTreePermissionTest extends AbstractSecurityTest {

    private CompositePermissionProvider createProvider(@NotNull CompositeAuthorizationConfiguration.CompositionType compositionType,
                                                       @NotNull AggregatedPermissionProvider... providers) {
        return CompositePermissionProvider.create(root, ImmutableList.copyOf(providers), Context.DEFAULT, compositionType, getRootProvider(), getTreeProvider());
    }

    @Test
    public void testEmptyProvidersRootTree() {
        AggregatedPermissionProvider composite = createProvider(OR);

        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        assertSame(TreePermission.EMPTY, composite.getTreePermission(rootTree, TreeType.DEFAULT, TreePermission.EMPTY));
    }

    @Test
    public void testEmptyProvidersCompositeParentPermission() {
        TreePermission tp = mock(TreePermission.class);
        AggregatedPermissionProvider aggr = when(mock(AggregatedPermissionProvider.class).getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(tp).getMock();

        // parent-TreePermission is CompositeTreePermission
        Tree rootTree = getRootProvider().createReadOnlyRoot(root).getTree(PathUtils.ROOT_PATH);
        AggregatedPermissionProvider[] providers = new AggregatedPermissionProvider[]{aggr, new FullScopeProvider(root)};
        CompositeTreePermission parentTp = (CompositeTreePermission) CompositeTreePermission.create(rootTree, getTreeProvider(), new TreeTypeProvider(Context.DEFAULT), providers, OR);

        // getTreePermission from compositePP -> aggregated-providers as taken from parent-permission
        AggregatedPermissionProvider composite = createProvider(AND);
        Tree systemTree = rootTree.getChild(JcrConstants.JCR_SYSTEM);
        assertTrue(composite.getTreePermission(systemTree, TreeType.HIDDEN, parentTp) instanceof CompositeTreePermission);

        verify(aggr, times(1)).getTreePermission(rootTree, TreeType.DEFAULT, TreePermission.EMPTY);
        verify(aggr, times(1)).getTreePermission(systemTree, TreeType.HIDDEN, tp);
    }

    @Test
    public void testEmptyProvidersMockParentPermission() {
        TreePermission tp = mock(TreePermission.class);
        when(tp.getChildPermission(anyString(), any(NodeState.class))).thenReturn(tp);

        AggregatedPermissionProvider composite = createProvider(OR);

        Tree ntTree = getRootProvider().createReadOnlyRoot(root).getTree(NodeTypeConstants.NODE_TYPES_PATH);
        assertSame(tp, composite.getTreePermission(ntTree, TreeType.DEFAULT, tp));

        verify(tp, times(1)).getChildPermission(ntTree.getName(), getTreeProvider().asNodeState(ntTree));
    }

    @Test
    public void testSingleProvider() {
        TreePermission tp = mock(TreePermission.class);
        when(tp.getChildPermission(anyString(), any(NodeState.class))).thenReturn(tp);
        AggregatedPermissionProvider aggr = when(mock(AggregatedPermissionProvider.class).getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(tp).getMock();

        AggregatedPermissionProvider composite = createProvider(AND, aggr);

        Tree rootTree = getRootProvider().createReadOnlyRoot(root).getTree(PathUtils.ROOT_PATH);
        TreePermission rootTreePermission = composite.getTreePermission(rootTree, TreeType.HIDDEN, TreePermission.EMPTY);
        assertSame(tp, rootTreePermission);

        // type param is ignored for the root tree
        verify(aggr, times(1)).getTreePermission(rootTree, TreeType.DEFAULT, TreePermission.EMPTY);
        verify(aggr, never()).getTreePermission(rootTree, TreeType.HIDDEN, TreePermission.EMPTY);

        Tree systemTree = rootTree.getChild(JcrConstants.JCR_SYSTEM);
        assertSame(tp, composite.getTreePermission(systemTree, TreeType.INTERNAL, rootTreePermission));

        verify(aggr, never()).getTreePermission(systemTree, TreeType.INTERNAL, rootTreePermission);
        verify(aggr, never()).getTreePermission(systemTree, TreeType.DEFAULT, rootTreePermission);
        verify(tp, times(1)).getChildPermission(systemTree.getName(), getTreeProvider().asNodeState(systemTree));
    }

    @Test
    public void testTwoProvider() {
        TreePermission tp = mock(TreePermission.class);
        when(tp.getChildPermission(anyString(), any(NodeState.class))).thenReturn(tp);
        AggregatedPermissionProvider aggr = when(mock(AggregatedPermissionProvider.class).getTreePermission(any(Tree.class), any(TreeType.class), any(TreePermission.class))).thenReturn(tp).getMock();

        AggregatedPermissionProvider composite = createProvider(OR, aggr, new FullScopeProvider(root));

        Tree rootTree = getRootProvider().createReadOnlyRoot(root).getTree(PathUtils.ROOT_PATH);
        TreePermission rootTreePermission = composite.getTreePermission(rootTree, TreeType.VERSION, TreePermission.EMPTY);
        assertTrue(rootTreePermission instanceof CompositeTreePermission);

        // type param is ignored for the root tree
        verify(aggr, times(1)).getTreePermission(rootTree, TreeType.DEFAULT, TreePermission.EMPTY);
        verify(aggr, never()).getTreePermission(rootTree, TreeType.VERSION, TreePermission.EMPTY);

        Tree systemTree = rootTree.getChild(JcrConstants.JCR_SYSTEM);
        assertTrue(composite.getTreePermission(systemTree, TreeType.ACCESS_CONTROL, rootTreePermission) instanceof CompositeTreePermission);

        verify(aggr, times(1)).getTreePermission(systemTree, TreeType.ACCESS_CONTROL, tp);
        verify(aggr, never()).getTreePermission(systemTree, TreeType.DEFAULT, rootTreePermission);
        verify(tp, never()).getChildPermission(systemTree.getName(), getTreeProvider().asNodeState(systemTree));
    }
}