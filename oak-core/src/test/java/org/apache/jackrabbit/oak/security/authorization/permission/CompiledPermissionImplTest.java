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

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.nodetype.ReadOnlyNodeTypeManager;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeAware;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.plugins.version.ReadOnlyVersionManager;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.ProviderCtx;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.authorization.monitor.AuthorizationMonitor;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.Context;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import java.lang.reflect.Field;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.JcrConstants.JCR_FROZENNODE;
import static org.apache.jackrabbit.JcrConstants.JCR_ISCHECKEDOUT;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.MIX_VERSIONABLE;
import static org.apache.jackrabbit.JcrConstants.NT_VERSION;
import static org.apache.jackrabbit.oak.plugins.tree.TreeUtil.addChild;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.PARAM_READ_PATHS;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants.PERMISSIONS_STORE_PATH;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.SET_PROPERTY;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions.VERSION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.authorization.permission.TreePermission.ALL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_VERSION_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class CompiledPermissionImplTest extends AbstractSecurityTest {

    private static String TEST_PATH = "/test";
    private static String SUBTREE_PATH = TEST_PATH + "/subtree";
    private static String ACCESS_CONTROLLED_PATH = TEST_PATH + "/accessControlled";

    private ContentSession testSession;
    private Set<String> accessControlledPaths = new HashSet<>();

    @Override
    public void before() throws Exception {
        super.before();

        Tree t = addChild(root.getTree(PathUtils.ROOT_PATH), "test", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        addChild(t, "subtree", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        addChild(t, "accessControlled", NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        grant(ACCESS_CONTROLLED_PATH, EveryonePrincipal.getInstance(), JCR_READ, JCR_WRITE);
        root.commit();

        testSession = createTestSession();
    }

    @Override
    public void after() throws Exception {
        try {
            testSession.close();
            AccessControlManager acMgr = getAccessControlManager(root);
            for (String path : accessControlledPaths) {
                if (root.getTree(path).exists()) {
                    AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
                    if (acl != null) {
                        acMgr.removePolicy(path, acl);
                    }
                }
            }
            root.getTree(TEST_PATH).remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @NotNull
    private PermissionStore mockPermissionStore(@NotNull Root r, @NotNull String wspName) {
        return spy(new PermissionStoreImpl(r, wspName, getConfig(AuthorizationConfiguration.class).getRestrictionProvider(), mock(AuthorizationMonitor.class)));
    }

    private CompiledPermissionImpl create(@NotNull Root r, @NotNull String workspaceName, @NotNull Set<Principal> principals, @NotNull PermissionStore store, @NotNull ConfigurationParameters options) {
        AuthorizationConfiguration config = getConfig(AuthorizationConfiguration.class);
        assertTrue(config instanceof CompositeAuthorizationConfiguration);

        AuthorizationConfiguration defConfig = ((CompositeAuthorizationConfiguration) config).getDefaultConfig();
        assertTrue(defConfig instanceof AuthorizationConfigurationImpl);

        CompiledPermissions cp = CompiledPermissionImpl.create(r, workspaceName, store, principals, options, config.getContext(), (AuthorizationConfigurationImpl) defConfig);
        assertTrue(cp instanceof CompiledPermissionImpl);

        return (CompiledPermissionImpl) cp;
    }

    @NotNull
    private CompiledPermissionImpl createForTestSession(@NotNull ConfigurationParameters options) {
        Root r = getRootProvider().createReadOnlyRoot(testSession.getLatestRoot());
        String workspaceName = testSession.getWorkspaceName();
        PermissionStore pStore = mockPermissionStore(r, workspaceName);

        return create(r, workspaceName, testSession.getAuthInfo().getPrincipals(), pStore, options);
    }

    @NotNull
    private Tree createReadonlyTree(@NotNull String path) {
        return getRootProvider().createReadOnlyRoot(root).getTree(path);
    }

    @NotNull
    private TreePermission createTreePermission(@NotNull CompiledPermissionImpl cp, @NotNull String path) {
        Tree t = createReadonlyTree(PathUtils.ROOT_PATH);
        TreePermission tp = cp.getTreePermission(t, TreePermission.EMPTY);
        for (String elem : PathUtils.elements(path)) {
            Tree child = t.getChild(elem);
            tp = cp.getTreePermission(child, tp);
            t = child;
        }
        return tp;
    }

    private void grant(@Nullable String path, @NotNull Principal principal, @NotNull String... privNames) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        acl.addAccessControlEntry(principal, privilegesFromNames(privNames));
        acMgr.setPolicy(acl.getPath(), acl);
        accessControlledPaths.add(path);
    }

    @NotNull
    private Tree createVersions(@NotNull String path) throws Exception {
        Tree tree = root.getTree(path);
        TreeUtil.addMixin(tree, MIX_VERSIONABLE, root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "uid");
        root.commit();
        for (int i = 0; i < 3; i++) {
            tree.setProperty(PropertyStates.createProperty(JCR_ISCHECKEDOUT, false));
            root.commit();
            tree.setProperty(PropertyStates.createProperty(JCR_ISCHECKEDOUT, true));
            root.commit();
        }

        ReadOnlyVersionManager vm = ReadOnlyVersionManager.getInstance(root, getNamePathMapper());
        return requireNonNull(vm.getVersionHistory(tree));
    }

    @Test
    public void testCreateFromEmptyPrincipals() {
        Set<Principal> principals = Set.of();
        assertSame(NoPermissions.getInstance(), CompiledPermissionImpl.create(root, "wspName", mock(PermissionStore.class), principals, ConfigurationParameters.EMPTY, mock(Context.class), mock(ProviderCtx.class)));
    }

    @Test
    public void testCreateNonExistingPermissionStore() {
        Tree t = when(mock(Tree.class).exists()).thenReturn(false).getMock();
        Root r = when(mock(Root.class).getTree(anyString())).thenReturn(t).getMock();
        Set<Principal> principals = Set.of(new PrincipalImpl("principalName"));

        assertSame(NoPermissions.getInstance(), CompiledPermissionImpl.create(r, "wspName", mock(PermissionStore.class), principals, ConfigurationParameters.EMPTY, mock(Context.class), mock(ProviderCtx.class)));
    }

    @Test
    public void testEmpyReadPaths() {
        CompiledPermissionImpl cp = createForTestSession(ConfigurationParameters.of(PARAM_READ_PATHS, Set.of()));
        // cp with EmptyReadPolicy
        for (String readPath : PermissionConstants.DEFAULT_READ_PATHS) {
            assertFalse(cp.isGranted(readPath, Permissions.READ_NODE));
            assertFalse(cp.isGranted(readPath, Permissions.READ));

            Tree t = createReadonlyTree(readPath);
            assertFalse(cp.isGranted(t, null, Permissions.READ_NODE));
            assertFalse(cp.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));

            assertFalse(cp.hasPrivileges(t, PrivilegeConstants.REP_READ_NODES));
            assertFalse(cp.hasPrivileges(t, JCR_READ));

            assertTrue(cp.getPrivileges(t).isEmpty());

            TreePermission tp = createTreePermission(cp, readPath);
            assertFalse(tp.canRead());
            assertFalse(tp.canRead(mock(PropertyState.class)));
            assertFalse(tp.canReadAll());
        }
    }

    @Test
    public void testDefaultReadPath() {
        // cp with DefaultReadPolicy
        CompiledPermissionImpl cp = createForTestSession(ConfigurationParameters.EMPTY);
        for (String readPath : PermissionConstants.DEFAULT_READ_PATHS) {
            assertReadPath(cp, readPath);
        }
    }

    @Test
    public void testNonDefaultReadPath() {
        // cp with DefaultReadPolicy but not default paths
        CompiledPermissionImpl cp = createForTestSession(ConfigurationParameters.of(PARAM_READ_PATHS, Set.of(TEST_PATH, "/another", "/yet/another")));

        for (String readPath : new String[]{TEST_PATH, SUBTREE_PATH, TEST_PATH + "/nonExisting"}) {
            assertReadPath(cp, readPath);
        }
    }

    private void assertReadPath(@NotNull CompiledPermissionImpl cp, @NotNull String readPath) {
        assertTrue(cp.isGranted(readPath, Permissions.READ_NODE));

        Tree t = createReadonlyTree(readPath);
        assertTrue(cp.isGranted(t, null, Permissions.READ_NODE));
        assertTrue(cp.isGranted(t, t.getProperty(JCR_PRIMARYTYPE), Permissions.READ_PROPERTY));

        assertTrue(cp.hasPrivileges(t, PrivilegeConstants.REP_READ_NODES));
        assertTrue(cp.hasPrivileges(t, JCR_READ));

        assertEquals(Set.of(JCR_READ), cp.getPrivileges(t));

        TreePermission tp = createTreePermission(cp, readPath);
        assertTrue(tp.canRead());
        assertTrue(tp.canRead(mock(PropertyState.class)));
        assertFalse(tp.canReadAll());
    }

    @Test
    public void testHidden() {
        CompiledPermissionImpl cp = createForTestSession(ConfigurationParameters.of(PARAM_READ_PATHS, Set.of()));

        String hiddenPath = "/oak:index/acPrincipalName/:index";
        Tree hiddenTree = createReadonlyTree(hiddenPath);
        assertTrue(hiddenTree.exists());

        assertTrue(cp.isGranted(hiddenTree, null, Permissions.ALL));
        // isGranted(String, long) serves as fallback for non-existing items
        // -> just regular permission eval without tree-type handling
        assertFalse(cp.isGranted(hiddenPath, Permissions.ALL));
        assertTrue(cp.getPrivileges(hiddenTree).isEmpty());

        TreePermission tp = createTreePermission(cp, hiddenPath);
        assertSame(ALL, tp);
    }

    @Test
    public void testInternal() throws Exception {
        // grant read permissions at root path
        grant(PathUtils.ROOT_PATH, EveryonePrincipal.getInstance(), JCR_READ);
        root.commit();

        CompiledPermissionImpl cp = createForTestSession(ConfigurationParameters.EMPTY);

        String[] internalPaths = new String[] {
                PERMISSIONS_STORE_PATH,
                PathUtils.concat(PERMISSIONS_STORE_PATH, testSession.getWorkspaceName())
        };
        for (String internalPath : internalPaths) {
            Tree internalTree = createReadonlyTree(internalPath);
            assertTrue(internalTree.exists());

            assertFalse(cp.isGranted(internalTree, null, Permissions.READ_NODE));
            // isGranted(String, long) serves as fallback for non-existing items
            // -> just regular permission eval without tree-type handling
            assertTrue(cp.isGranted(internalPath, Permissions.READ_NODE));
            assertTrue(cp.getPrivileges(internalTree).isEmpty());

            TreePermission tp = createTreePermission(cp, internalPath);
            assertSame(InternalTreePermission.INSTANCE, tp);
        }
    }

    @Test
    public void testVersionHistory() throws Exception {
        Tree versionHistory = createVersions(SUBTREE_PATH);

        // subtree path is made readable through PARAM_READ_PATHS
        CompiledPermissionImpl cp = createForTestSession(ConfigurationParameters.of(PARAM_READ_PATHS, Set.of(TEST_PATH)));

        assertTrue(cp.isGranted(versionHistory, null, Permissions.READ));
        // isGranted(String, long) serves as fallback when no versionable node available
        // -> just regular permission eval based on path, no tree-type taken into account
        assertFalse(cp.isGranted(versionHistory.getPath(), Permissions.READ));
        assertEquals(Set.of(JCR_READ), cp.getPrivileges(versionHistory));
        assertTrue(cp.hasPrivileges(versionHistory, JCR_READ));

        TreePermission tp = createTreePermission(cp, versionHistory.getPath());
        assertTrue(tp instanceof VersionTreePermission);
    }

    @Test
    public void testVersion() throws Exception {
        Tree version = createVersions(SUBTREE_PATH).getChild("1.2");
        assertEquals(NT_VERSION, TreeUtil.getPrimaryTypeName(version));

        // subtree path is made readable through PARAM_READ_PATHS
        CompiledPermissionImpl cp = createForTestSession(ConfigurationParameters.of(PARAM_READ_PATHS, Set.of(TEST_PATH)));

        assertTrue(cp.isGranted(version, null, Permissions.READ));
        // isGranted(String, long) serves as fallback when no versionable node available
        // -> just regular permission eval based on path, no tree-type taken into account
        assertFalse(cp.isGranted(version.getPath(), Permissions.READ));
        assertEquals(Set.of(JCR_READ), cp.getPrivileges(version));
        assertTrue(cp.hasPrivileges(version, JCR_READ));

        TreePermission tp = createTreePermission(cp, version.getPath());
        assertTrue(tp instanceof VersionTreePermission);
    }

    @Test
    public void testFrozenNode() throws Exception {
        Tree version = createVersions(TEST_PATH).getChildren().iterator().next();
        Tree frozenNode = version.getChild(JCR_FROZENNODE);

        // default read-paths -> only accessControlled tree readable
        CompiledPermissionImpl cp = createForTestSession(ConfigurationParameters.EMPTY);

        for (Tree tree : new Tree[]{
                frozenNode,
                frozenNode.getChild("subtree"),
                frozenNode.getChild("nonExistingChild")}) {

            String path = tree.getPath();
            assertFalse(path, cp.isGranted(tree, null, Permissions.READ));
            assertFalse(path, cp.isGranted(path, Permissions.READ));
            assertTrue(path, cp.getPrivileges(tree).isEmpty());
            assertFalse(path, cp.hasPrivileges(tree, JCR_READ));

            TreePermission tp = createTreePermission(cp, path);
            assertTrue(tp instanceof VersionTreePermission);
        }
    }

    @Test
    public void testAccessControlledChildInFrozenNode() throws Exception {
        Tree version = createVersions(TEST_PATH).getChild("1.2");
        Tree frozenNode = version.getChild(JCR_FROZENNODE);
        Tree copiedAccessControlledChild = frozenNode.getChild("accessControlled");

        // default read-paths -> only accessControlled tree readable
        CompiledPermissionImpl cp = createForTestSession(ConfigurationParameters.EMPTY);

        assertTrue(cp.isGranted(copiedAccessControlledChild, null, Permissions.READ | Permissions.SET_PROPERTY));
        // isGranted(String, long) serves as fallback when no versionable node available
        // -> just regular permission eval based on path, no tree-type taken into account
        assertFalse(cp.isGranted(copiedAccessControlledChild.getPath(), Permissions.READ));
        assertEquals(Set.of(JCR_READ, JCR_WRITE), cp.getPrivileges(copiedAccessControlledChild));
        assertTrue(cp.hasPrivileges(copiedAccessControlledChild, JCR_READ, JCR_WRITE));

        TreePermission tp = createTreePermission(cp, version.getPath());
        assertTrue(tp instanceof VersionTreePermission);
    }

    @Test
    public void testVersionStoreTree() throws Exception {
        Tree versionStoreTree = createVersions(SUBTREE_PATH).getParent();

        // subtree path is made readable through PARAM_READ_PATHS
        CompiledPermissionImpl cp = createForTestSession(ConfigurationParameters.of(PARAM_READ_PATHS, Set.of(TEST_PATH)));

        // but: permissions for version store tree is evaluated based on regular permissions
        // and not tied to a versionable tree
        assertFalse(cp.isGranted(versionStoreTree, null, Permissions.READ));
        // isGranted(String, long) serves as fallback when no versionable node available
        // -> just regular permission eval based on path, no tree-type taken into account
        assertFalse(cp.isGranted(versionStoreTree.getPath(), Permissions.READ));
        assertTrue(cp.getPrivileges(versionStoreTree).isEmpty());
        assertFalse(cp.hasPrivileges(versionStoreTree, JCR_READ));

        TreePermission tp = createTreePermission(cp, versionStoreTree.getPath());
        assertFalse(tp instanceof VersionTreePermission);
    }

    @Test
    public void testVersionableTreeRemoved() throws Exception {
        Tree version = createVersions(ACCESS_CONTROLLED_PATH).getChild("1.2");
        assertEquals(NT_VERSION, TreeUtil.getPrimaryTypeName(version));

        Tree accessControlled = root.getTree(ACCESS_CONTROLLED_PATH);
        PropertyState property = accessControlled.getProperty(JCR_PRIMARYTYPE);
        accessControlled.remove();
        root.commit();

        CompiledPermissionImpl cp = createForTestSession(ConfigurationParameters.EMPTY);

        assertFalse(cp.isGranted(version, null, Permissions.READ));
        assertFalse(cp.isGranted(version, property, Permissions.READ));
        // isGranted(String, long) serves as fallback when no versionable node available
        // -> just regular permission eval based on path, no tree-type taken into account
        assertFalse(cp.isGranted(version.getPath(), Permissions.READ));
        assertTrue(cp.getPrivileges(version).isEmpty());
        assertFalse(cp.hasPrivileges(version, JCR_READ));

        TreePermission tp = createTreePermission(cp, version.getPath());
        assertTrue(tp instanceof VersionTreePermission);
    }

    @Test
    public void testCacheBuildOnDemand() throws Exception {
        grant(ACCESS_CONTROLLED_PATH, getTestUser().getPrincipal(), JCR_VERSION_MANAGEMENT);
        grant(ACCESS_CONTROLLED_PATH, EveryonePrincipal.getInstance(), JCR_READ);
        root.commit();

        Root readOnlyRoot = getRootProvider().createReadOnlyRoot(testSession.getLatestRoot());
        String wspName = testSession.getWorkspaceName();

        PermissionStore store = mockPermissionStore(readOnlyRoot, wspName);
        Set<Principal> principals = Set.of(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        CompiledPermissionImpl cp = create(readOnlyRoot, wspName, principals, store, ConfigurationParameters.EMPTY);

        // verify lazy initialization of the permission cache
        verify(store, never()).getNumEntries(anyString(), anyLong());

        cp.isGranted(ACCESS_CONTROLLED_PATH, Permissions.VERSION_MANAGEMENT);
        verify(store, times(2)).getNumEntries(anyString(), anyLong());

        cp.isGranted(ACCESS_CONTROLLED_PATH, Permissions.READ);
        verify(store, times(2)).getNumEntries(anyString(), anyLong());

        clearInvocations(store);

        // subsequent reads must not trigger re-loading of the permission cache.
        cp.isGranted(ACCESS_CONTROLLED_PATH, Permissions.WRITE);
        verify(store, never()).getNumEntries(anyString(), anyLong());

        // flush must not eagerly re-load permission cache
        cp.refresh(readOnlyRoot, wspName);
        verify(store, times(1)).flush(readOnlyRoot);
        verify(store, never()).getNumEntries(anyString(), anyLong());

        // verifying permissions that requires init of both user and group store
        cp.isGranted(ACCESS_CONTROLLED_PATH, Permissions.MODIFY_PROPERTY);
        verify(store, times(2)).getNumEntries(anyString(), anyLong());
    }

    @Test
    public void testMissingGroupStore() throws Exception {
        grant(ACCESS_CONTROLLED_PATH, getTestUser().getPrincipal(), JCR_VERSION_MANAGEMENT);
        root.commit();

        Root readOnlyRoot = getRootProvider().createReadOnlyRoot(testSession.getLatestRoot());
        String wspName = testSession.getWorkspaceName();

        // create cp for user principal only (no group principals that hold the permission setup)
        PermissionStore store = mockPermissionStore(readOnlyRoot, wspName);
        CompiledPermissionImpl cp = create(readOnlyRoot, wspName, Set.of(getTestUser().getPrincipal()), store, ConfigurationParameters.EMPTY);

        verify(store, never()).getNumEntries(anyString(), anyLong());

        cp.isGranted(ACCESS_CONTROLLED_PATH, Permissions.READ_NODE);
        verify(store, times(1)).getNumEntries(anyString(), anyLong());

        cp.refresh(readOnlyRoot, wspName);
        verify(store, times(1)).getNumEntries(anyString(), anyLong());

        cp.isGranted(ACCESS_CONTROLLED_PATH, Permissions.READ_NODE);
        verify(store, times(2)).getNumEntries(anyString(), anyLong());

        assertFalse(cp.isGranted(ACCESS_CONTROLLED_PATH, SET_PROPERTY));
        assertTrue(cp.isGranted(ACCESS_CONTROLLED_PATH, VERSION_MANAGEMENT));
        Tree t = createReadonlyTree(ACCESS_CONTROLLED_PATH);
        assertFalse(cp.hasPrivileges(t, JCR_WRITE));
        assertTrue(cp.hasPrivileges(t, JCR_VERSION_MANAGEMENT));
        assertEquals(Set.of(JCR_VERSION_MANAGEMENT), cp.getPrivileges(createReadonlyTree(ACCESS_CONTROLLED_PATH)));

        TreePermission tp = createTreePermission(cp, ACCESS_CONTROLLED_PATH);
        assertTrue(tp.isGranted(VERSION_MANAGEMENT));
        assertFalse(tp.canRead());
        assertFalse(tp.canRead(mock(PropertyState.class)));
        assertFalse(tp.canReadAll());

        verify(store, times(2)).getNumEntries(anyString(), anyLong());
        verify(store, times(2)).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test
    public void testMissingUserStore() {
        Root readOnlyRoot = getRootProvider().createReadOnlyRoot(testSession.getLatestRoot());
        String wspName = testSession.getWorkspaceName();

        // create cp for group principal only (no user principal)
        PermissionStore store = mockPermissionStore(readOnlyRoot, wspName);
        CompiledPermissionImpl cp = create(readOnlyRoot, wspName, Set.of(EveryonePrincipal.getInstance()), store, ConfigurationParameters.EMPTY);

        verify(store, never()).getNumEntries(anyString(), anyLong());

        cp.isGranted(ACCESS_CONTROLLED_PATH, Permissions.READ_NODE);
        verify(store, times(1)).getNumEntries(anyString(), anyLong());

        cp.refresh(readOnlyRoot, wspName);
        verify(store, times(1)).getNumEntries(anyString(), anyLong());

        cp.isGranted(ACCESS_CONTROLLED_PATH, Permissions.READ_NODE);
        verify(store, times(2)).getNumEntries(anyString(), anyLong());

        assertTrue(cp.isGranted(ACCESS_CONTROLLED_PATH, SET_PROPERTY));
        assertTrue(cp.hasPrivileges(createReadonlyTree(ACCESS_CONTROLLED_PATH), JCR_WRITE));
        assertEquals(Set.of(JCR_READ, JCR_WRITE), cp.getPrivileges(createReadonlyTree(ACCESS_CONTROLLED_PATH)));

        TreePermission tp = createTreePermission(cp, ACCESS_CONTROLLED_PATH);
        assertTrue(tp.isGranted(SET_PROPERTY));
        assertTrue(tp.canRead());
        assertTrue(tp.canRead(mock(PropertyState.class)));
        assertFalse(tp.canReadAll());

        verify(store, times(2)).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetTreePermissionInvalidParent() {
        String wspName = adminSession.getWorkspaceName();
        CompiledPermissionImpl cp = create(root, wspName, Collections.singleton(EveryonePrincipal.getInstance()), mockPermissionStore(root, wspName), ConfigurationParameters.EMPTY);
        TreePermission invalidParentTreePermission = mock(TreePermission.class);
        cp.getTreePermission(root.getTree("/jcr:system"), invalidParentTreePermission);
    }

    @Test
    public void testGetTreePermissionForHiddenVersionable() throws Exception {
        String wspName = adminSession.getWorkspaceName();
        CompiledPermissionImpl cp = create(root, wspName, Collections.singleton(EveryonePrincipal.getInstance()), mockPermissionStore(root, wspName), ConfigurationParameters.EMPTY);

        Tree hidden = mock(Tree.class, withSettings().extraInterfaces(TreeTypeAware.class));
        when(((TreeTypeAware) hidden).getType()).thenReturn(TreeType.HIDDEN);
        when(hidden.exists()).thenReturn(true);

        setVersionManager(cp, hidden);

        Tree t = when(mock(Tree.class).exists()).thenReturn(true).getMock();
        TreePermission tp = cp.getTreePermission(t, TreeType.VERSION, TreePermission.EMPTY);
        assertTrue(tp instanceof VersionTreePermission);
        assertTrue(tp.canReadAll());
    }

    @Test
    public void testGetTreePermissionForInternalVersionable() throws Exception {
        String wspName = adminSession.getWorkspaceName();
        CompiledPermissionImpl cp = create(root, wspName, Collections.singleton(EveryonePrincipal.getInstance()), mockPermissionStore(root, wspName), ConfigurationParameters.EMPTY);

        Tree internal = mock(Tree.class, withSettings().extraInterfaces(TreeTypeAware.class));
        when(((TreeTypeAware) internal).getType()).thenReturn(TreeType.INTERNAL);
        when(internal.exists()).thenReturn(true);

        setVersionManager(cp, internal);

        Tree t = when(mock(Tree.class).exists()).thenReturn(true).getMock();
        TreePermission tp = cp.getTreePermission(t, TreeType.VERSION, TreePermission.EMPTY);
        assertTrue(tp instanceof VersionTreePermission);
        assertFalse(tp.canRead());
        assertFalse(tp.canReadProperties());
        assertFalse(tp.canReadAll());
        assertFalse(tp.isGranted(Permissions.NO_PERMISSION));
    }

    private static void setVersionManager(@NotNull CompiledPermissionImpl cp, @NotNull Tree t) throws Exception {
        ReadOnlyVersionManager versionManager = createVersionManager(t);
        Field f = CompiledPermissionImpl.class.getDeclaredField("versionManager");
        f.setAccessible(true);
        f.set(cp, versionManager);
    }

    private static ReadOnlyVersionManager createVersionManager(final @Nullable Tree t) {
        return new ReadOnlyVersionManager() {
            @Override
            protected @NotNull Tree getVersionStorage() {
                throw new UnsupportedOperationException();
            }

            @Override
            protected @NotNull Root getWorkspaceRoot() {
                throw new UnsupportedOperationException();
            }

            @Override
            protected @NotNull ReadOnlyNodeTypeManager getNodeTypeManager() {
                throw new UnsupportedOperationException();
            }

            @Override
            public @Nullable Tree getVersionable(@NotNull Tree versionTree, @NotNull String workspaceName) {
                return t;
            }
        };
    }
}