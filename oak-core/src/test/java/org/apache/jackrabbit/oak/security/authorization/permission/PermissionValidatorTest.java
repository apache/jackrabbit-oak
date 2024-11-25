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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.authorization.ProviderCtx;
import org.apache.jackrabbit.oak.security.authorization.monitor.AuthorizationMonitor;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.security.AccessControlManager;
import java.security.Principal;
import java.util.Set;

import static org.apache.jackrabbit.JcrConstants.JCR_CREATED;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_CREATEDBY;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.MIX_CREATED;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.NODE_TYPES_PATH;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.REP_VERSIONSTORAGE;
import static org.apache.jackrabbit.oak.spi.version.VersionConstants.VERSION_STORE_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PermissionValidatorTest extends AbstractSecurityTest {

    private static final String TEST_ROOT_PATH = "/testRoot";
    private static final String TEST_CHILD_PATH = "/testRoot/child";

    private Principal testPrincipal;

    private final AuthorizationMonitor monitor = mock(AuthorizationMonitor.class);

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        Tree rootNode = root.getTree("/");
        Tree testTree = TreeUtil.addChild(rootNode, "testRoot", NT_UNSTRUCTURED);
        TreeUtil.addChild(testTree, "child", NT_UNSTRUCTURED);
        root.commit();

        testPrincipal = getTestUser().getPrincipal();
    }

    @After
    @Override
    public void after() throws Exception {
        try {
            clearInvocations(monitor);
            // revert uncommitted changes
            root.refresh();

            // remove all test content
            root.getTree(TEST_ROOT_PATH).remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    private void grant(@NotNull String... privilegeNames) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, TEST_ROOT_PATH);
        acl.addEntry(testPrincipal, AccessControlUtils.privilegesFromNames(acMgr, privilegeNames), true);
        acMgr.setPolicy(TEST_ROOT_PATH, acl);
        root.commit();
    }

    @NotNull
    private ProviderCtx mockProviderCtx() {
        ProviderCtx ctx = mock(ProviderCtx.class);
        when(ctx.getSecurityProvider()).thenReturn(getSecurityProvider());
        when(ctx.getTreeProvider()).thenReturn(getTreeProvider());
        when(ctx.getMonitor()).thenReturn(monitor);
        return ctx;
    }

    private PermissionValidator createValidator(@NotNull Set<Principal> principals, @NotNull String path) {
        Tree t = root.getTree(PathUtils.ROOT_PATH);
        NodeState ns = getTreeProvider().asNodeState(t);

        String wspName = root.getContentSession().getWorkspaceName();
        PermissionProvider pp = getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, wspName, principals);

        PermissionValidatorProvider pvp = new PermissionValidatorProvider(wspName, principals, new MoveTracker(), mockProviderCtx());
        PermissionValidator validator = new PermissionValidator(ns, ns, pp, pvp);
        TreePermission tp = pp.getTreePermission(t, TreePermission.EMPTY);
        for (String name : PathUtils.elements(path)) {
            t = t.getChild(name);
            ns = ns.getChildNode(name);
            tp = tp.getChildPermission(name, ns);
            validator = new PermissionValidator(t, t, tp, validator);
        }
        return validator;
    }

    private void verifyMonitor() {
        verify(monitor).accessViolation();
        verifyNoMoreInteractions(monitor);
    }

    @Test(expected = CommitFailedException.class)
    public void testLockPermissions() throws Exception {
        // grant the test session the ability to read/write that node but don't allow jcr:lockManagement
        grant(PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE);

        try (ContentSession testSession = createTestSession()) {
            Root testRoot = testSession.getLatestRoot();
            Tree testChild = testRoot.getTree(TEST_CHILD_PATH);
            testChild.setProperty(PropertyStates.createProperty(JcrConstants.JCR_LOCKOWNER, "lockOwner"));
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
            assertEquals(0, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRepositoryPermissionsNamespaces() throws Exception {
        try (ContentSession testSession = createTestSession()) {
            PermissionValidator validator = createValidator(testSession.getAuthInfo().getPrincipals(), NamespaceConstants.NAMESPACES_PATH);
            validator.childNodeAdded("any", mock(NodeState.class));
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
            assertEquals(0, e.getCode());
            throw e;
        } finally {
            verifyMonitor();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRepositoryPermissionsNodeTypes() throws Exception {
        try (ContentSession testSession = createTestSession()) {
            PermissionValidator validator = createValidator(testSession.getAuthInfo().getPrincipals(), NodeTypeConstants.NODE_TYPES_PATH);
            validator.childNodeDeleted("any", mock(NodeState.class));
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
            assertEquals(0, e.getCode());
            throw e;
        } finally {
            verifyMonitor();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRepositoryPermissionsPrivileges() throws Exception {
        try (ContentSession testSession = createTestSession()) {
            PermissionValidator validator = createValidator(testSession.getAuthInfo().getPrincipals(), PrivilegeConstants.PRIVILEGES_PATH);
            validator.propertyAdded(PropertyStates.createProperty("any", "value"));
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
            assertEquals(0, e.getCode());
            throw e;
        } finally {
            verifyMonitor();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testRemoveVersionStorageTree() throws Exception {
        Tree t = root.getTree(PathUtils.ROOT_PATH);
        NodeState ns = getTreeProvider().asNodeState(t);
        ProviderCtx ctx = mockProviderCtx();

        PermissionValidatorProvider pvp = new PermissionValidatorProvider("wspName", Set.of(), new MoveTracker(), ctx);
        PermissionValidator validator = new PermissionValidator(ns, ns, mock(PermissionProvider.class), pvp);
        for (String name : PathUtils.elements(VERSION_STORE_PATH)) {
            t = t.getChild(name);
            ns = ns.getChildNode(name);
            validator = new PermissionValidator(t, t, TreePermission.EMPTY, validator);
        }
        try {
            TreeUtil.addChild(t, "any", REP_VERSIONSTORAGE);
            validator.childNodeDeleted("any", mock(NodeState.class));
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
            assertEquals(22, e.getCode());
            throw e;
        } finally {
            verifyMonitor();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddVersionStorageTreeWithoutHistory() throws Exception {
        PermissionValidator validator = createValidator(Set.of(), VERSION_STORE_PATH);
        try {
            Tree t = root.getTree(VERSION_STORE_PATH);
            TreeUtil.addChild(t, "any", REP_VERSIONSTORAGE);
            validator.childNodeAdded("any", mock(NodeState.class));
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
            assertEquals(21, e.getCode());
            throw e;
        } finally {
            verifyMonitor();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddVersionStorageTreeUnexpectedNode() throws Exception {
        PermissionValidator validator = createValidator(Set.of(), VERSION_STORE_PATH);
        try {
            Tree t = root.getTree(VERSION_STORE_PATH);
            Tree storageT = TreeUtil.addChild(t, "any", REP_VERSIONSTORAGE);
            TreeUtil.addChild(storageT, "unexpected", NT_UNSTRUCTURED);
            validator.childNodeAdded("any", mock(NodeState.class));
        } catch (CommitFailedException e) {
            assertTrue(e.isOfType("Misc"));
            assertEquals(0, e.getCode());
            throw e;
        } finally {
            verifyNoInteractions(monitor);
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testChangePrimaryTypeToPolicyNode() throws Exception {
        // grant the test session the ability to read/write at test node but don't
        // allow to modify access control content
        grant(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_READ_ACCESS_CONTROL, PrivilegeConstants.REP_WRITE);

        // create a rep:policy node that is not detected as access control content
        TreeUtil.addChild(root.getTree(TEST_CHILD_PATH), AccessControlConstants.REP_POLICY, NT_UNSTRUCTURED);
        root.commit();

        try (ContentSession testSession = createTestSession()) {
            Root testRoot = testSession.getLatestRoot();

            Tree testChild = testRoot.getTree(TEST_CHILD_PATH);
            testChild.setProperty(PropertyStates.createProperty(JcrConstants.JCR_MIXINTYPES, ImmutableList.of(AccessControlConstants.MIX_REP_ACCESS_CONTROLLABLE), Type.NAMES));

            Tree testPolicy = testChild.getChild(AccessControlConstants.REP_POLICY);
            testPolicy.setOrderableChildren(true);
            testPolicy.setProperty(JCR_PRIMARYTYPE, AccessControlConstants.NT_REP_ACL, Type.NAME);
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
            assertEquals(0, e.getCode());
            throw e;
        }
    }

    @Test
    public void testAddImmutablePropertyWithDeclaringMixin() throws Exception {
        // grant the test session the ability to read/write at test node but don't
        // allow to modify access control content
        grant(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_NODE_TYPE_MANAGEMENT);

        try (ContentSession testSession = createTestSession()) {
            Root testRoot = testSession.getLatestRoot();

            Tree testTree = testRoot.getTree(TEST_ROOT_PATH);
            TreeUtil.addMixin(testTree, MIX_CREATED, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "uid");
            assertTrue(testTree.hasProperty(JCR_CREATEDBY));
            assertTrue(testTree.hasProperty(JCR_CREATED));
            testRoot.commit();
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testAddImmutablePropertyWithoutDeclaringMixin() throws Exception {
        // grant the test session the ability to read/write at test node but don't
        // allow to modify access control content
        grant(PrivilegeConstants.JCR_READ);

        try (ContentSession testSession = createTestSession()) {
            Root testRoot = testSession.getLatestRoot();

            // adding jcr:created and jcr:createdBy without mix:created present will trigger regular permission eval
            // as without mixin they are not considered immutable properties
            Tree testTree = testRoot.getTree(TEST_ROOT_PATH);
            testTree.setProperty(PropertyStates.createProperty(JCR_CREATED, "mixCreatedIsMissing", Type.DATE));
            testTree.setProperty(PropertyStates.createProperty(JCR_CREATEDBY, "mixCreatedIsMissing", Type.STRING));
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
            assertEquals(0, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testChangeImmutableProperty() throws Exception {
        TreeUtil.addMixin(root.getTree(TEST_ROOT_PATH), MIX_CREATED, root.getTree(NODE_TYPES_PATH), "uid");
        // grant the test session the ability to read and write properties at test node but
        // not to add/remove nodes
        grant(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_MODIFY_PROPERTIES);

        try (ContentSession testSession = createTestSession()) {
            Root testRoot = testSession.getLatestRoot();
            Tree testTree = testRoot.getTree(TEST_ROOT_PATH);
            testTree.setProperty(PropertyStates.createProperty(JCR_CREATEDBY, "anotherUid", Type.STRING));
            testRoot.commit();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
            assertEquals(0, e.getCode());
            throw e;
        }
    }
}