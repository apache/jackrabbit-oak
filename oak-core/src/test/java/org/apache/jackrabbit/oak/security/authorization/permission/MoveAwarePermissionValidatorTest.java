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
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.authorization.ProviderCtx;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.VisibleValidator;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.security.AccessControlManager;
import java.security.Principal;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MoveAwarePermissionValidatorTest extends AbstractSecurityTest {

    private Tree t;
    private PermissionProvider pp;
    private JackrabbitAccessControlList acl;

    @Before
    public void before() throws Exception {
        super.before();

        Tree rootTree = root.getTree(PathUtils.ROOT_PATH);
        TreeUtil.addChild(rootTree, "src", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        TreeUtil.addChild(rootTree, "dest", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        root.commit();

    }

    @After
    public void after() throws Exception {
        try {
            if (acl != null) {
                getAccessControlManager(root).removePolicy(acl.getPath(), acl);
            }
            Tree src = root.getTree("/src");
            if (src.exists()) {
                src.remove();
            }
            Tree dest = root.getTree("/dest");
            if (dest.exists()) {
                dest.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    private void grant(@NotNull String path, @NotNull Principal principal, @NotNull String... privilegeNames) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        acl.addEntry(principal, AccessControlUtils.privilegesFromNames(acMgr, privilegeNames), true);
        acMgr.setPolicy(path, acl);
        root.commit();
        this.acl = acl;
    }

    @NotNull
    private MoveAwarePermissionValidator createRootValidator(@NotNull Set<Principal> principals, @NotNull MoveTracker tracker) {
        ProviderCtx ctx = mock(ProviderCtx.class);
        when(ctx.getSecurityProvider()).thenReturn(getSecurityProvider());
        when(ctx.getTreeProvider()).thenReturn(getTreeProvider());
        when(ctx.getRootProvider()).thenReturn(getRootProvider());

        String wspName = root.getContentSession().getWorkspaceName();
        Root readonlyRoot = getRootProvider().createReadOnlyRoot(root);
        t = readonlyRoot.getTree(PathUtils.ROOT_PATH);
        pp = spy(new PermissionProviderImpl(readonlyRoot, wspName, principals, RestrictionProvider.EMPTY, ConfigurationParameters.EMPTY, Context.DEFAULT, ctx));

        PermissionValidatorProvider pvp = new PermissionValidatorProvider(wspName, principals, tracker, ctx);
        NodeState ns = getTreeProvider().asNodeState(t);
        return new MoveAwarePermissionValidator(ns, ns, pp, pvp, tracker);
    }


    @Test
    public void testChildNodeAddedNoMatchingMove() throws Exception {
        MoveAwarePermissionValidator maValidator = spy(createRootValidator(adminSession.getAuthInfo().getPrincipals(), new MoveTracker()));
        Validator validator = maValidator.childNodeAdded("name", mock(NodeState.class));

        assertTrue(validator instanceof VisibleValidator);
        verify(maValidator, times(1)).checkPermissions(t.getChild("name"), false, Permissions.ADD_NODE);
    }

    @Test
    public void testChildNodeAddedNonExistingSrc() throws Exception {
        MoveTracker moveTracker = new MoveTracker();
        moveTracker.addMove("/srcNonExisting", "/dest");

        MoveAwarePermissionValidator maValidator = spy(createRootValidator(adminSession.getAuthInfo().getPrincipals(), moveTracker));
        Validator validator = maValidator.childNodeAdded("dest", mock(NodeState.class));

        assertTrue(validator instanceof VisibleValidator);
        verify(maValidator, times(1)).checkPermissions(t.getChild("dest"), false, Permissions.ADD_NODE);
        verify(pp, never()).isGranted(t.getChild("src"), null, Permissions.REMOVE_NODE);
    }

    @Test
    public void testChildNodeAddedExistingSrc() throws Exception {
        MoveTracker moveTracker = new MoveTracker();
        moveTracker.addMove("/src", "/dest");

        MoveAwarePermissionValidator maValidator = spy(createRootValidator(adminSession.getAuthInfo().getPrincipals(), moveTracker));
        Validator validator = maValidator.childNodeAdded("dest", mock(NodeState.class));

        assertNull(validator);
        verify(maValidator, times(1)).checkPermissions(t.getChild("dest"), false, Permissions.ADD_NODE|Permissions.NODE_TYPE_MANAGEMENT);
        verify(pp, times(1)).isGranted(t.getChild("src"), null, Permissions.REMOVE_NODE);
    }

    @Test
    public void testChildNodeAddedNullPraent() throws Exception {
        MoveTracker moveTracker = new MoveTracker();
        moveTracker.addMove("/src", "/dest");

        MoveAwarePermissionValidator maValidator = spy(createRootValidator(adminSession.getAuthInfo().getPrincipals(), moveTracker));
        when(maValidator.getParentAfter()).thenReturn(null);

        Validator validator = maValidator.childNodeAdded("dest", mock(NodeState.class));

        assertTrue(validator instanceof VisibleValidator);
        verify(maValidator, times(1)).checkPermissions(t.getChild("dest"), false, Permissions.ADD_NODE);
        verify(pp, never()).isGranted(t.getChild("src"), null, Permissions.REMOVE_NODE);
    }

    @Test(expected = CommitFailedException.class)
    public void testChildNodeAddedMissingPermissionAtSrc() throws Exception {
        grant("/", EveryonePrincipal.getInstance(), PrivilegeConstants.JCR_ADD_CHILD_NODES, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT);

        MoveTracker moveTracker = new MoveTracker();
        moveTracker.addMove("/src", "/dest");

        MoveAwarePermissionValidator maValidator = spy(createRootValidator(ImmutableSet.of(EveryonePrincipal.getInstance()), moveTracker));
        try {
            maValidator.childNodeAdded("dest", mock(NodeState.class));
        } catch (CommitFailedException e){
            verify(maValidator, times(1)).checkPermissions(t.getChild("dest"), false, Permissions.ADD_NODE|Permissions.NODE_TYPE_MANAGEMENT);
            verify(pp, times(1)).isGranted(t.getChild("src"), null, Permissions.REMOVE_NODE);
            assertTrue(e.isAccessViolation());
            assertEquals(0, e.getCode());
            throw e;
        }
    }

    @Test
    public void testChildNodeDeletedNoMatchingMove() throws Exception {
        MoveAwarePermissionValidator maValidator = spy(createRootValidator(adminSession.getAuthInfo().getPrincipals(), new MoveTracker()));
        Validator validator = maValidator.childNodeDeleted("name", mock(NodeState.class));

        assertNull(validator);
        verify(maValidator, times(1)).checkPermissions(t.getChild("name"), true, Permissions.REMOVE_NODE);
    }

    @Test
    public void testChildNodeDeletedNonExistingDestination() throws Exception {
        MoveTracker moveTracker = new MoveTracker();
        moveTracker.addMove("/src", "/nonExistingDest");

        MoveAwarePermissionValidator maValidator = spy(createRootValidator(adminSession.getAuthInfo().getPrincipals(), moveTracker));
        Validator validator = maValidator.childNodeDeleted("src", mock(NodeState.class));

        assertNull(validator);
        verify(maValidator, times(1)).checkPermissions(t.getChild("src"), true, Permissions.REMOVE_NODE);
        verify(pp, never()).isGranted(t.getChild("nonExistingDest"), null, Permissions.ADD_NODE|Permissions.NODE_TYPE_MANAGEMENT);
    }

    @Test
    public void testChildNodeDeletedExistingDestination() throws Exception {
        MoveTracker moveTracker = new MoveTracker();
        moveTracker.addMove("/src", "/dest");

        MoveAwarePermissionValidator maValidator = spy(createRootValidator(adminSession.getAuthInfo().getPrincipals(), moveTracker));
        Validator validator = maValidator.childNodeDeleted("src", mock(NodeState.class));

        assertNull(validator);
        verify(maValidator, times(1)).checkPermissions(t.getChild("src"), true, Permissions.REMOVE_NODE);
        verify(pp, times(1)).isGranted(t.getChild("dest"), null, Permissions.ADD_NODE|Permissions.NODE_TYPE_MANAGEMENT);
    }

    @Test
    public void testChildNodeDeletedNullParent() throws Exception {
        MoveTracker moveTracker = new MoveTracker();
        moveTracker.addMove("/src", "/dest");

        MoveAwarePermissionValidator maValidator = spy(createRootValidator(adminSession.getAuthInfo().getPrincipals(), moveTracker));
        when(maValidator.getParentBefore()).thenReturn(null);

        Validator validator = maValidator.childNodeDeleted("src", mock(NodeState.class));

        assertNull(validator);
        verify(maValidator, times(1)).checkPermissions(t.getChild("src"), true, Permissions.REMOVE_NODE);
        verify(pp, never()).isGranted(t.getChild("dest"), null, Permissions.ADD_NODE|Permissions.NODE_TYPE_MANAGEMENT);
    }

    @Test(expected = CommitFailedException.class)
    public void testChildNodeDeletedMissingPermissionAtDestination() throws Exception {
        grant(PathUtils.ROOT_PATH, EveryonePrincipal.getInstance(), PrivilegeConstants.JCR_REMOVE_CHILD_NODES, PrivilegeConstants.JCR_REMOVE_NODE);

        MoveTracker moveTracker = new MoveTracker();
        moveTracker.addMove("/src", "/dest");

        MoveAwarePermissionValidator maValidator = spy(createRootValidator(ImmutableSet.of(EveryonePrincipal.getInstance()), moveTracker));
        try {
            maValidator.childNodeDeleted("src", mock(NodeState.class));
        } catch (CommitFailedException e){
            verify(maValidator, times(1)).checkPermissions(t.getChild("src"), true, Permissions.REMOVE_NODE);
            verify(pp, times(1)).isGranted(t.getChild("dest"), null, Permissions.ADD_NODE|Permissions.NODE_TYPE_MANAGEMENT);
            assertTrue(e.isAccessViolation());
            assertEquals(0, e.getCode());
            throw e;
        }
    }
}