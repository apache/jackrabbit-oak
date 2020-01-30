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
package org.apache.jackrabbit.oak.security.authorization.permission;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.tree.TreeConstants.OAK_CHILD_ORDER;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.NT_REP_ACE;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.NT_REP_DENY_ACE;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.NT_REP_GRANT_ACE;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_PRINCIPAL_NAME;
import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_PRIVILEGES;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.REP_ACCESS_CONTROLLED_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.REP_NUM_PERMISSIONS;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PermissionStoreEditorTest extends AbstractSecurityTest {

    private static final String PRINCIPAL_NAME = "principalName";

    private PrivilegeBitsProvider bitsProvider;
    private RestrictionProvider restrictionProvider;

    private TypePredicate isACE;
    private TypePredicate isGrantACE;

    @Before
    public void before() throws Exception {
        super.before();

        bitsProvider = new PrivilegeBitsProvider(root);
        restrictionProvider = getConfig(AuthorizationConfiguration.class).getRestrictionProvider();

        NodeState rootState = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));
        isACE = spy(new TypePredicate(rootState, NT_REP_ACE));
        isGrantACE = spy(new TypePredicate(rootState, NT_REP_GRANT_ACE));
    }

    @NotNull
    private static NodeState mockACE(@NotNull String principalName) {
        NodeState ace = mock(NodeState.class);
        when(ace.getName(JCR_PRIMARYTYPE)).thenReturn(NT_REP_DENY_ACE);
        when(ace.getNames(REP_PRIVILEGES)).thenReturn(ImmutableSet.of(JCR_READ));
        when(ace.getString(REP_PRINCIPAL_NAME)).thenReturn(principalName);
        return ace;
    }

    @NotNull
    private static NodeState mockNodeState(@NotNull NodeState ace) {
        NodeState nodeState = mock(NodeState.class);
        when(nodeState.getNames(OAK_CHILD_ORDER)).thenReturn(ImmutableSet.of("c1"));
        when(nodeState.getChildNodeCount(anyLong())).thenReturn(Long.valueOf(1));
        when(nodeState.getChildNodeNames()).thenReturn(ImmutableSet.of("c1"));
        when(nodeState.getChildNode(anyString())).thenReturn(ace);
        return nodeState;
    }

    @NotNull
    private PermissionStoreEditor createPermissionStoreEditor(@NotNull NodeState nodeState, @NotNull NodeBuilder permissionRoot) {
        return new PermissionStoreEditor("", AccessControlConstants.REP_REPO_POLICY, nodeState, permissionRoot, isACE, isGrantACE, bitsProvider, restrictionProvider, getTreeProvider());
    }

    @Test
    public void testCreateWithNonAceChildren() {
        NodeState nodeState = mock(NodeState.class);
        when(nodeState.getNames(OAK_CHILD_ORDER)).thenReturn(ImmutableSet.of("c1", "c2", "c3"));
        when(nodeState.getName(JCR_PRIMARYTYPE)).thenReturn(NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        when(nodeState.getNames(JCR_MIXINTYPES)).thenReturn(Collections.emptySet());
        when(nodeState.getChildNode(anyString())).thenReturn(nodeState);

        new PermissionStoreEditor("/test", AccessControlConstants.REP_POLICY, nodeState, mock(NodeBuilder.class), isACE, isGrantACE, bitsProvider, restrictionProvider, getTreeProvider());

        verify(nodeState, times(3)).getChildNode(anyString());
        verify(isACE, times(3)).apply(nodeState);
        verify(isGrantACE, never()).apply(nodeState);
    }

    @Test
    public void testCreateWithIncompleteChildOrder() {
        NodeState ace = mockACE(PRINCIPAL_NAME);

        NodeState nodeState = mock(NodeState.class);
        when(nodeState.getNames(OAK_CHILD_ORDER)).thenReturn(ImmutableSet.of("c1", "c2"));
        when(nodeState.getChildNodeCount(anyLong())).thenReturn(Long.valueOf(3));
        when(nodeState.getChildNodeNames()).thenReturn(ImmutableSet.of("c1", "c2", "c3"));
        when(nodeState.getChildNode(anyString())).thenReturn(ace);

        createPermissionStoreEditor(nodeState, mock(NodeBuilder.class));

        verify(nodeState, times(3)).getChildNode(anyString());
        verify(isACE, times(3)).apply(ace);
        verify(isGrantACE, times(3)).apply(ace);
    }

    @Test
    public void testRemovePermissionEntriesUnknownPrincipalName() {
        NodeState ace = mockACE("unknownPrincipal");
        NodeState nodeState = mockNodeState(ace);
        NodeBuilder permissionsRoot = mock(NodeBuilder.class);

        PermissionStoreEditor editor = createPermissionStoreEditor(nodeState, permissionsRoot);
        editor.removePermissionEntries();

        verify(permissionsRoot, times(1)).hasChildNode("unknownPrincipal");
        verify(permissionsRoot, never()).getChildNode("unknownPrincipal");
    }

    @Test
    public void testRemovePermissionEntriesNonExistingParent() {
        NodeState ace = mockACE(PRINCIPAL_NAME);
        NodeState nodeState = mockNodeState(ace);

        NodeBuilder parent = when(mock(NodeBuilder.class).exists()).thenReturn(false).getMock();
        NodeBuilder principalRoot = when(mock(NodeBuilder.class).getChildNode(anyString())).thenReturn(parent).getMock();

        NodeBuilder permissionsRoot = mock(NodeBuilder.class);
        when(permissionsRoot.hasChildNode(PRINCIPAL_NAME)).thenReturn(true);
        when(permissionsRoot.getChildNode(PRINCIPAL_NAME)).thenReturn(principalRoot);

        PermissionStoreEditor editor = createPermissionStoreEditor(nodeState, permissionsRoot);
        editor.removePermissionEntries();

        verify(permissionsRoot, times(1)).hasChildNode(PRINCIPAL_NAME);
        verify(permissionsRoot, times(1)).getChildNode(PRINCIPAL_NAME);
        verify(principalRoot, times(1)).getChildNode(anyString());
        verify(parent, times(1)).exists();
        verify(parent, never()).getProperty(anyString());
    }

    @Test
    public void testRemovePermissionEntriesNoMatchingEntry() {
        NodeState ace = mockACE(PRINCIPAL_NAME);
        NodeState nodeState = mockNodeState(ace);

        PropertyState nonMatchingPathProp = PropertyStates.createProperty(REP_ACCESS_CONTROLLED_PATH, "/noMatch");
        NodeBuilder parent = when(mock(NodeBuilder.class).exists()).thenReturn(true).getMock();
        when(parent.getProperty(REP_ACCESS_CONTROLLED_PATH)).thenReturn(nonMatchingPathProp);
        when(parent.getChildNodeNames()).thenReturn(ImmutableSet.of("collision"));
        when(parent.getChildNode(anyString())).thenReturn(parent);

        NodeBuilder principalRoot = when(mock(NodeBuilder.class).getChildNode(anyString())).thenReturn(parent).getMock();
        NodeBuilder permissionsRoot = mock(NodeBuilder.class);
        when(permissionsRoot.hasChildNode(PRINCIPAL_NAME)).thenReturn(true);
        when(permissionsRoot.getChildNode(PRINCIPAL_NAME)).thenReturn(principalRoot);

        PermissionStoreEditor editor = createPermissionStoreEditor(nodeState, permissionsRoot);
        editor.removePermissionEntries();

        verify(parent, times(1)).exists();
        verify(parent, times(1)).getChildNodeNames();
        verify(parent, times(1)).getChildNode("collision");
        verify(parent, times(2)).getProperty(REP_ACCESS_CONTROLLED_PATH);
        verify(parent, never()).remove();
    }

    @Test
    public void testUpdateNumEntriesPrincipalRootExisted() {
        NodeState ace = mockACE(PRINCIPAL_NAME);
        NodeState nodeState = mockNodeState(ace);

        NodeBuilder parent = when(mock(NodeBuilder.class).exists()).thenReturn(true).getMock();
        when(parent.remove()).thenReturn(true);

        NodeBuilder principalRoot = when(mock(NodeBuilder.class).getChildNode(anyString())).thenReturn(parent).getMock();
        when(principalRoot.isNew()).thenReturn(false);

        NodeBuilder permissionsRoot = mock(NodeBuilder.class);
        when(permissionsRoot.hasChildNode(PRINCIPAL_NAME)).thenReturn(true);
        when(permissionsRoot.getChildNode(PRINCIPAL_NAME)).thenReturn(principalRoot);

        PermissionStoreEditor editor = createPermissionStoreEditor(nodeState, permissionsRoot);

        PropertyState pathProp = PropertyStates.createProperty(REP_ACCESS_CONTROLLED_PATH, editor.getPath());
        when(parent.getProperty(REP_ACCESS_CONTROLLED_PATH)).thenReturn(pathProp);

        editor.removePermissionEntries();

        verify(parent, times(1)).remove();
        verify(principalRoot, times(1)).getProperty(REP_NUM_PERMISSIONS);
        verify(principalRoot, never()).removeProperty(REP_NUM_PERMISSIONS);
        verify(principalRoot, never()).setProperty(anyString(), anyLong(), any(Type.class));
    }

    @Test
    public void testUpdateNumEntriesTurnsNegativ() {
        NodeState ace = mockACE(PRINCIPAL_NAME);
        NodeState nodeState = mockNodeState(ace);

        NodeBuilder parent = when(mock(NodeBuilder.class).exists()).thenReturn(true).getMock();
        when(parent.remove()).thenReturn(true);

        NodeBuilder principalRoot = when(mock(NodeBuilder.class).getChildNode(anyString())).thenReturn(parent).getMock();
        when(principalRoot.isNew()).thenReturn(true);
        when(principalRoot.getProperty(REP_NUM_PERMISSIONS)).thenReturn(PropertyStates.createProperty(REP_NUM_PERMISSIONS, Long.valueOf(0), Type.LONG));

        NodeBuilder permissionsRoot = mock(NodeBuilder.class);
        when(permissionsRoot.hasChildNode(PRINCIPAL_NAME)).thenReturn(true);
        when(permissionsRoot.getChildNode(PRINCIPAL_NAME)).thenReturn(principalRoot);

        PermissionStoreEditor editor = createPermissionStoreEditor(nodeState, permissionsRoot);

        PropertyState pathProp = PropertyStates.createProperty(REP_ACCESS_CONTROLLED_PATH, editor.getPath());
        when(parent.getProperty(REP_ACCESS_CONTROLLED_PATH)).thenReturn(pathProp);

        editor.removePermissionEntries();

        verify(parent, times(1)).remove();
        verify(principalRoot, times(1)).getProperty(REP_NUM_PERMISSIONS);
        verify(principalRoot, times(1)).removeProperty(REP_NUM_PERMISSIONS);
        verify(principalRoot, never()).setProperty(anyString(), anyLong(), any(Type.class));
    }

    @Test
    public void testUpdatePermissionEntriesWhenCollisionChildMatches() {
        NodeState ace = mockACE(PRINCIPAL_NAME);
        NodeState nodeState = mockNodeState(ace);

        NodeBuilder collision = mock(NodeBuilder.class);
        when(collision.child(anyString())).thenReturn(collision);
        when(collision.setProperty(anyString(), any())).thenReturn(collision);
        when(collision.setProperty(anyString(), any(), any(Type.class))).thenReturn(collision);

        NodeBuilder parent = mock(NodeBuilder.class);
        when(parent.hasProperty(REP_ACCESS_CONTROLLED_PATH)).thenReturn(true);
        when(parent.getProperty(REP_ACCESS_CONTROLLED_PATH)).thenReturn(PropertyStates.createProperty(REP_ACCESS_CONTROLLED_PATH, "/noMatch"));
        when(parent.getChildNodeNames()).thenReturn(ImmutableSet.of("collision"));
        when(parent.getChildNode(anyString())).thenReturn(collision);
        when(parent.child(anyString())).thenReturn(collision);

        NodeBuilder principalRoot = when(mock(NodeBuilder.class).child(anyString())).thenReturn(parent).getMock();
        NodeBuilder permissionsRoot = mock(NodeBuilder.class);
        when(permissionsRoot.child(PRINCIPAL_NAME)).thenReturn(principalRoot);

        PermissionStoreEditor editor = createPermissionStoreEditor(nodeState, permissionsRoot);

        when(collision.getProperty(REP_ACCESS_CONTROLLED_PATH)).thenReturn(PropertyStates.createProperty(REP_ACCESS_CONTROLLED_PATH, editor.getPath()));

        editor.updatePermissionEntries();

        // the collision node gets updated not the parent
        verify(parent, never()).setProperty(REP_ACCESS_CONTROLLED_PATH, editor.getPath());
        verify(parent, never()).child(anyString());

        verify(collision, times(1)).setProperty(REP_ACCESS_CONTROLLED_PATH, editor.getPath());
        verify(collision, times(1)).child(anyString());
    }

    @Test
    public void testUpdatePermissionEntriesDoesNotRemoveCollisions() {
        NodeState ace = mockACE(PRINCIPAL_NAME);
        NodeState nodeState = mockNodeState(ace);

        NodeBuilder child = mock(NodeBuilder.class);
        when(child.setProperty(anyString(), any())).thenReturn(child);
        when(child.setProperty(anyString(), any(), any(Type.class))).thenReturn(child);
        NodeBuilder parent = mock(NodeBuilder.class);
        when(parent.getChildNodeNames()).thenReturn(ImmutableSet.of("collision", "entry"));
        when(parent.getChildNode(anyString())).thenReturn(child);
        when(parent.child(anyString())).thenReturn(child);

        NodeBuilder principalRoot = when(mock(NodeBuilder.class).child(anyString())).thenReturn(parent).getMock();
        NodeBuilder permissionsRoot = mock(NodeBuilder.class);
        when(permissionsRoot.child(PRINCIPAL_NAME)).thenReturn(principalRoot);

        PermissionStoreEditor editor = createPermissionStoreEditor(nodeState, permissionsRoot);
        editor.updatePermissionEntries();

        // only the existing 'entry' child gets removed. the collision is not touched
        verify(child, times(1)).remove();
    }
}