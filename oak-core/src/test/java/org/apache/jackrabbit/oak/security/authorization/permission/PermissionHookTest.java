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

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.security.authorization.ProviderCtx;
import org.apache.jackrabbit.oak.security.authorization.monitor.AuthorizationMonitor;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.JcrAllUtil;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Testing the {@code PermissionHook}
 */
public class PermissionHookTest extends AbstractSecurityTest implements AccessControlConstants, PermissionConstants, PrivilegeConstants {

    protected String testPath = "/testPath";
    protected String childPath = "/testPath/childNode";

    protected Principal testPrincipal;
    protected List<Principal> principals = new ArrayList<>();

    private AuthorizationMonitor monitor;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        testPrincipal = getTestUser().getPrincipal();
        Tree rootNode = root.getTree("/");
        Tree testNode = TreeUtil.addChild(rootNode, "testPath", JcrConstants.NT_UNSTRUCTURED);
        TreeUtil.addChild(testNode, "childNode", JcrConstants.NT_UNSTRUCTURED);

        addACE(testPath, testPrincipal, JCR_ADD_CHILD_NODES);
        addACE(testPath, EveryonePrincipal.getInstance(), JCR_READ);
        root.commit();

        PrivilegeBitsProvider bitsProvider = new PrivilegeBitsProvider(root);
        monitor = mock(AuthorizationMonitor.class);
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            root.refresh();
            Tree test = root.getTree(testPath);
            if (test.exists()) {
                test.remove();
            }

            for (Principal principal : principals) {
                getUserManager(root).getAuthorizable(principal).remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    private ProviderCtx mockProviderContext(@NotNull MountInfoProvider mip, @NotNull RootProvider rootProvider, @NotNull TreeProvider treeProvider) {
        ProviderCtx ctx = mock(ProviderCtx.class);
        when(ctx.getMountInfoProvider()).thenReturn(mip);
        when(ctx.getRootProvider()).thenReturn(rootProvider);
        when(ctx.getTreeProvider()).thenReturn(treeProvider);
        when(ctx.getMonitor()).thenReturn(monitor);
        return ctx;
    }

    private PermissionHook createPermissionHook(@NotNull String wspName) {
        return new PermissionHook(wspName, RestrictionProvider.EMPTY, mockProviderContext(Mounts.defaultMountInfoProvider(), getRootProvider(), getTreeProvider()));
    }

    private void addACE(@NotNull String path, @NotNull Principal principal, @NotNull String... privilegeNames) throws RepositoryException {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        acl.addAccessControlEntry(principal, privilegesFromNames(privilegeNames));
        acMgr.setPolicy(path, acl);
    }

    private Tree getPrincipalRoot(@NotNull Principal principal) {
        return root.getTree(PERMISSIONS_STORE_PATH).getChild(adminSession.getWorkspaceName()).getChild(principal.getName());
    }

    private Tree getEntry(@NotNull Principal principal, String accessControlledPath, long index) throws Exception {
        Tree principalRoot = getPrincipalRoot(principal);
        Tree parent = principalRoot.getChild(PermissionUtil.getEntryName(accessControlledPath));
        Tree entry = parent.getChild(String.valueOf(index));
        if (!entry.exists()) {
            throw new RepositoryException("no such entry");
        }
        return entry;
    }

    private long cntEntries(Tree parent) {
        long cnt = parent.getChildrenCount(Long.MAX_VALUE);
        for (Tree child : parent.getChildren()) {
            cnt += cntEntries(child);
        }
        return cnt;
    }

    private void createPrincipals() throws Exception {
        if (principals.isEmpty()) {
            for (int i = 0; i < 10; i++) {
                Group gr = getUserManager(root).createGroup("testGroup" + i);
                principals.add(gr.getPrincipal());
            }
            root.commit();
        }
    }

    private static void assertIndex(int expected, Tree entry) {
        assertEquals(expected, Integer.parseInt(entry.getName()));
    }

    @NotNull
    private static Set<String> getAccessControlledPaths(@NotNull Tree principalTree) {
        Set<String> s = new HashSet<>();
        for (Tree tree : principalTree.getChildren()) {
            String path = getAccessControlledPath(tree);
            if (path != null) {
                s.add(path);
            }
            for (Tree child : tree.getChildren()) {
                if (child.getName().startsWith("c")) {
                    String childPath = getAccessControlledPath(child);
                    if (childPath != null) {
                        s.add(childPath);
                    }
                }
            }
        }
        return s;
    }

    @Nullable
    private static String getAccessControlledPath(@NotNull Tree t) {
        PropertyState pathProp = t.getProperty(REP_ACCESS_CONTROLLED_PATH);
        return (pathProp == null) ? null : pathProp.getValue(Type.STRING);

    }

    private static void assertNumPermissionsProperty(long expectedValue, @NotNull Tree parent) {
        PropertyState p = parent.getProperty(REP_NUM_PERMISSIONS);
        assertNotNull(p);
        assertEquals(expectedValue, p.getValue(Type.LONG).longValue());
    }

    @Test
    public void testModifyRestrictions() throws Exception {
        Tree testAce = root.getTree(testPath + "/rep:policy").getChildren().iterator().next();
        assertEquals(testPrincipal.getName(), testAce.getProperty(REP_PRINCIPAL_NAME).getValue(Type.STRING));

        // add a new restriction node through the OAK API instead of access control manager
        Tree restrictions = TreeUtil.addChild(testAce, REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        restrictions.setProperty(REP_GLOB, "*");
        String restrictionsPath = restrictions.getPath();
        root.commit();

        Tree principalRoot = getPrincipalRoot(testPrincipal);
        assertEquals(2, cntEntries(principalRoot));
        Tree parent = principalRoot.getChildren().iterator().next();
        assertEquals("*", parent.getChildren().iterator().next().getProperty(REP_GLOB).getValue(Type.STRING));

        // modify the restrictions node
        Tree restrictionsNode = root.getTree(restrictionsPath);
        restrictionsNode.setProperty(REP_GLOB, "/*/jcr:content/*");
        root.commit();

        principalRoot = getPrincipalRoot(testPrincipal);
        assertEquals(2, cntEntries(principalRoot));
        parent = principalRoot.getChildren().iterator().next();
        assertEquals("/*/jcr:content/*", parent.getChildren().iterator().next().getProperty(REP_GLOB).getValue(Type.STRING));

        // remove the restriction again
        root.getTree(restrictionsPath).remove();
        root.commit();

        principalRoot = getPrincipalRoot(testPrincipal);
        assertEquals(2, cntEntries(principalRoot));
        parent = principalRoot.getChildren().iterator().next();
        assertNull(parent.getChildren().iterator().next().getProperty(REP_GLOB));
    }

    @Test
    public void testReorderAce() throws Exception {
        Tree entry = getEntry(testPrincipal, testPath, 0);
        assertIndex(0, entry);

        Tree aclTree = root.getTree(testPath + "/rep:policy");
        aclTree.getChildren().iterator().next().orderBefore(null);

        root.commit();

        entry = getEntry(testPrincipal, testPath, 1);
        assertIndex(1, entry);
    }

    @Test
    public void testReorderAndAddAce() throws Exception {
        Tree entry = getEntry(testPrincipal, testPath, 0);
        assertIndex(0, entry);

        Tree aclTree = root.getTree(testPath + "/rep:policy");
        // reorder
        aclTree.getChildren().iterator().next().orderBefore(null);

        // add a new entry
        Tree ace = TreeUtil.addChild(aclTree, "denyEveryoneLockMgt", NT_REP_DENY_ACE);
        ace.setProperty(REP_PRINCIPAL_NAME, EveryonePrincipal.NAME);
        ace.setProperty(AccessControlConstants.REP_PRIVILEGES, Collections.singleton(JCR_LOCK_MANAGEMENT), Type.NAMES);
        root.commit();

        entry = getEntry(testPrincipal, testPath, 1);
        assertIndex(1, entry);
    }

    @Test
    public void testReorderAddAndRemoveAces() throws Exception {
        Tree entry = getEntry(testPrincipal, testPath, 0);
        assertIndex(0, entry);

        Tree aclTree = root.getTree(testPath + "/rep:policy");

        // reorder testPrincipal entry to the end
        aclTree.getChildren().iterator().next().orderBefore(null);

        Iterator<Tree> aceIt = aclTree.getChildren().iterator();
        // remove the everyone entry
        aceIt.next().remove();
        // remember the name of the testPrincipal entry.
        String name = aceIt.next().getName();

        // add a new entry
        Tree ace = TreeUtil.addChild(aclTree, "denyEveryoneLockMgt", NT_REP_DENY_ACE);
        ace.setProperty(REP_PRINCIPAL_NAME, EveryonePrincipal.NAME);
        ace.setProperty(AccessControlConstants.REP_PRIVILEGES, Collections.singleton(JCR_LOCK_MANAGEMENT), Type.NAMES);

        // reorder the new entry before the remaining existing entry
        ace.orderBefore(name);

        root.commit();

        entry = getEntry(testPrincipal, testPath, 1);
        assertIndex(1, entry);
    }

    /**
     * ACE    :  0   1   2   3   4   5   6   7
     * Before :  tp  ev  p0  p1  p2  p3
     * After  :      ev      p2  p1  p3  p4  p5
     */
    @Test
    public void testReorderAddAndRemoveAces2() throws Exception {
        createPrincipals();

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        for (int i = 0; i < 4; i++) {
            acl.addAccessControlEntry(principals.get(i), privilegesFromNames(JCR_READ));
        }
        acMgr.setPolicy(testPath, acl);
        root.commit();

        AccessControlEntry[] aces = acl.getAccessControlEntries();
        acl.removeAccessControlEntry(aces[0]);
        acl.removeAccessControlEntry(aces[2]);
        acl.orderBefore(aces[4], aces[3]);
        acl.addAccessControlEntry(principals.get(4), privilegesFromNames(JCR_READ));
        acl.addAccessControlEntry(principals.get(5), privilegesFromNames(JCR_READ));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        Tree entry = getEntry(principals.get(2), testPath, 1);
        assertIndex(1, entry);

        entry = getEntry(principals.get(1), testPath, 2);
        assertIndex(2, entry);
    }

    /**
     * ACE    :  0   1   2   3   4   5   6   7
     * Before :  tp  ev  p0  p1  p2  p3
     * After  :      p1      ev  p3  p2
     */
    @Test
    public void testReorderAndRemoveAces() throws Exception {
        createPrincipals();

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        for (int i = 0; i < 4; i++) {
            acl.addAccessControlEntry(principals.get(i), privilegesFromNames(JCR_READ));
        }
        acMgr.setPolicy(testPath, acl);
        root.commit();

        AccessControlEntry[] aces = acl.getAccessControlEntries();
        acl.removeAccessControlEntry(aces[0]);
        acl.removeAccessControlEntry(aces[2]);
        acl.orderBefore(aces[4], null);
        acl.orderBefore(aces[3], aces[1]);
        acMgr.setPolicy(testPath, acl);
        root.commit();

        Tree entry = getEntry(EveryonePrincipal.getInstance(), testPath, 1);
        assertIndex(1, entry);

        entry = getEntry(principals.get(2), testPath, 3);
        assertIndex(3, entry);

        for (Principal p : new Principal[]{testPrincipal, principals.get(0)}) {
            try {
                getEntry(p, testPath, 0);
                fail();
            } catch (RepositoryException e) {
                // success
            }
        }
    }

    @Test
    public void testImplicitAceRemoval() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(JCR_READ, REP_WRITE));
        acMgr.setPolicy(testPath, acl);

        acl = AccessControlUtils.getAccessControlList(acMgr, childPath);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_READ));
        acMgr.setPolicy(childPath, acl);
        root.commit();

        assertTrue(root.getTree(childPath + "/rep:policy").exists());

        Tree principalRoot = getPrincipalRoot(EveryonePrincipal.getInstance());
        assertEquals(4, cntEntries(principalRoot));

        ContentSession testSession = createTestSession();
        Root testRoot = testSession.getLatestRoot();

        assertTrue(testRoot.getTree(childPath).exists());
        assertFalse(testRoot.getTree(childPath + "/rep:policy").exists());

        testRoot.getTree(childPath).remove();
        testRoot.commit();
        testSession.close();

        root.refresh();
        assertFalse(root.getTree(testPath).hasChild("childNode"));
        assertFalse(root.getTree(childPath + "/rep:policy").exists());
        // aces must be removed in the permission store even if the editing
        // session wasn't able to access them.
        principalRoot = getPrincipalRoot(EveryonePrincipal.getInstance());
        assertEquals(2, cntEntries(principalRoot));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2015">OAK-2015</a>
     */
    @Test
    public void testDynamicJcrAll() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);

        // grant 'everyone' jcr:all at the child path.
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, childPath);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_ALL));
        acMgr.setPolicy(childPath, acl);
        root.commit();

        // verify that the permission store contains an entry for everyone at childPath
        // and the privilegeBits for jcr:all are reflect with a placeholder value.
        Tree allEntry = getEntry(EveryonePrincipal.getInstance(), childPath, 0);
        assertTrue(allEntry.exists());
        PropertyState ps = allEntry.getProperty(PermissionConstants.REP_PRIVILEGE_BITS);
        assertEquals(1, ps.count());
        assertTrue(JcrAllUtil.denotesDynamicJcrAll(ps));

        // verify that the permission provider still exposes the correct privilege
        // (jcr:all) for the given childPath irrespective of the dynamic nature of
        // the privilege bits in the persisted permission entry.
        Set<Principal> principalSet = ImmutableSet.<Principal>of(EveryonePrincipal.getInstance());
        PermissionProvider permissionProvider = getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, root.getContentSession().getWorkspaceName(), principalSet);
        Tree childTree = root.getTree(childPath);
        assertTrue(permissionProvider.hasPrivileges(childTree, PrivilegeConstants.JCR_ALL));
        assertTrue(permissionProvider.getPrivileges(childTree).contains(PrivilegeConstants.JCR_ALL));

        // also verify the permission evaluation
        long diff = Permissions.diff(Permissions.ALL, Permissions.REMOVE_NODE|Permissions.ADD_NODE);
        assertFalse(permissionProvider.isGranted(childTree, null, Permissions.REMOVE_NODE));
        assertFalse(permissionProvider.isGranted(childTree, null, Permissions.ADD_NODE));
        assertTrue(permissionProvider.isGranted(childTree, null, diff));

        // remove the ACE again
        acl = AccessControlUtils.getAccessControlList(acMgr, childPath);
        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            if (EveryonePrincipal.NAME.equals(ace.getPrincipal().getName())) {
                acl.removeAccessControlEntry(ace);
            }
        }
        acMgr.setPolicy(childPath, acl);
        root.commit();

        // verify that the corresponding permission entry has been removed.
        Tree everyoneRoot = getPrincipalRoot(EveryonePrincipal.getInstance());
        Tree parent = everyoneRoot.getChild(PermissionUtil.getEntryName(childPath));
        if (parent.exists()) {
            assertFalse(parent.getChild("0").exists());
        }
    }

    @Test
    public void testNumPermissionsProperty() throws Exception {
        Tree everyoneRoot = getPrincipalRoot(EveryonePrincipal.getInstance());
        Tree testRoot = getPrincipalRoot(testPrincipal);

        // initial state after setup
        assertNumPermissionsProperty(1, everyoneRoot);
        assertNumPermissionsProperty(1, testRoot);

        // add another acl with an entry for everyone
        addACE(childPath, EveryonePrincipal.getInstance(), JCR_READ);
        root.commit();

        assertNumPermissionsProperty(2, everyoneRoot);
        assertNumPermissionsProperty(1, testRoot);

        // adding another ACE at an existing ACL must not change num-permissions
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, childPath);
        acl = AccessControlUtils.getAccessControlList(acMgr, childPath);
        acl.addEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_READ), false, Map.of(REP_GLOB, getValueFactory(root).createValue("/*/jcr:content")));
        acMgr.setPolicy(childPath, acl);
        root.commit();

        assertNumPermissionsProperty(2, everyoneRoot);
        assertNumPermissionsProperty(1, testRoot);

        // remove policy at 'testPath'
        acMgr.removePolicy(testPath, AccessControlUtils.getAccessControlList(acMgr, testPath));
        root.commit();

        assertNumPermissionsProperty(1, everyoneRoot);
        assertNumPermissionsProperty(0, testRoot);

        // remove all ACEs on the childPath policy -> same effect as policy removal on permission store
        acl = AccessControlUtils.getAccessControlList(acMgr, childPath);
        for (AccessControlEntry entry : acl.getAccessControlEntries()) {
            acl.removeAccessControlEntry(entry);
        }
        acMgr.setPolicy(childPath, acl);
        root.commit();

        assertNumPermissionsProperty(0, everyoneRoot);
        assertNumPermissionsProperty(0, testRoot);
    }

    @Test
    public void testCollisions() throws Exception {
        Tree testRoot = getPrincipalRoot(testPrincipal);
        assertNumPermissionsProperty(1, testRoot);

        String aaPath = testPath + "/Aa";
        String bbPath = testPath + "/BB";

        if (aaPath.hashCode() == bbPath.hashCode()) {
            try {
                Tree parent = root.getTree(testPath);
                Tree aa = TreeUtil.addChild(parent, "Aa", JcrConstants.NT_UNSTRUCTURED);
                addACE(aa.getPath(), testPrincipal, JCR_READ);

                Tree bb = TreeUtil.addChild(parent, "BB", JcrConstants.NT_UNSTRUCTURED);
                addACE(bb.getPath(), testPrincipal, JCR_READ);
                root.commit();

                assertEquals(2, testRoot.getChildrenCount(Long.MAX_VALUE));
                assertNumPermissionsProperty(3, testRoot);

                Set<String> accessControlledPaths = Set.of(testPath, aa.getPath(), bb.getPath());
                assertEquals(accessControlledPaths, getAccessControlledPaths(testRoot));
            } finally {
                root.getTree(aaPath).remove();
                root.getTree(bbPath).remove();
                root.commit();
            }
        } else {
            fail();
        }

    }

    @Test
    public void testCollisionRemoval() throws Exception {
        Tree testRoot = getPrincipalRoot(testPrincipal);
        assertNumPermissionsProperty(1, testRoot);

        String aaPath = testPath + "/Aa";
        String bbPath = testPath + "/BB";

        if (aaPath.hashCode() == bbPath.hashCode()) {
            Tree parent = root.getTree(testPath);
            Tree aa = TreeUtil.addChild(parent, "Aa", JcrConstants.NT_UNSTRUCTURED);
            addACE(aa.getPath(), testPrincipal, JCR_READ);

            Tree bb = TreeUtil.addChild(parent, "BB", JcrConstants.NT_UNSTRUCTURED);
            addACE(bb.getPath(), testPrincipal, JCR_READ);
            root.commit();

            root.getTree(aaPath).remove();
            root.commit();

            assertEquals(2, testRoot.getChildrenCount(Long.MAX_VALUE));
            assertTrue(testRoot.hasChild(bbPath.hashCode() + ""));

            assertEquals(Set.of(testPath, bb.getPath()), getAccessControlledPaths(testRoot));
            assertNumPermissionsProperty(2, testRoot);
        }
    }

    @Test
    public void testCollisionRemoval2() throws Exception {
        Tree testRoot = getPrincipalRoot(testPrincipal);
        assertNumPermissionsProperty(1, testRoot);

        String aaPath = testPath + "/Aa";
        String bbPath = testPath + "/BB";

        if (aaPath.hashCode() == bbPath.hashCode()) {
            Tree parent = root.getTree(testPath);
            Tree aa = TreeUtil.addChild(parent, "Aa", JcrConstants.NT_UNSTRUCTURED);
            addACE(aa.getPath(), testPrincipal, JCR_READ);

            Tree bb = TreeUtil.addChild(parent, "BB", JcrConstants.NT_UNSTRUCTURED);
            addACE(bb.getPath(), testPrincipal, JCR_READ);
            root.commit();

            root.getTree(bbPath).remove();
            root.commit();

            assertEquals(2, testRoot.getChildrenCount(Long.MAX_VALUE));
            assertTrue(testRoot.hasChild(aaPath.hashCode() + ""));

            assertEquals(Set.of(testPath, aa.getPath()), getAccessControlledPaths(testRoot));
            assertNumPermissionsProperty(2, testRoot);
        }
    }

    @Test
    public void testCollisionRemoval3() throws Exception {
        Tree testRoot = getPrincipalRoot(testPrincipal);
        assertNumPermissionsProperty(1, testRoot);

        String aaPath = testPath + "/Aa";
        String bbPath = testPath + "/BB";

        if (aaPath.hashCode() == bbPath.hashCode()) {
            Tree parent = root.getTree(testPath);
            Tree aa = TreeUtil.addChild(parent, "Aa", JcrConstants.NT_UNSTRUCTURED);
            addACE(aa.getPath(), testPrincipal, JCR_READ);

            Tree bb = TreeUtil.addChild(parent, "BB", JcrConstants.NT_UNSTRUCTURED);
            addACE(bb.getPath(), testPrincipal, JCR_READ);
            root.commit();

            root.getTree(aaPath).remove();
            root.getTree(bbPath).remove();
            root.commit();

            assertEquals(1, testRoot.getChildrenCount(Long.MAX_VALUE));
            assertFalse(testRoot.hasChild(aaPath.hashCode() + ""));
            assertFalse(testRoot.hasChild(bbPath.hashCode() + ""));

            assertEquals(Set.of(testPath), getAccessControlledPaths(testRoot));
            assertNumPermissionsProperty(1, testRoot);
        }
    }

    @Test
    public void testCollisionRemoval4() throws Exception {
        Tree testRoot = getPrincipalRoot(testPrincipal);

        String aPath = testPath + "/AaAa";
        String bPath = testPath + "/BBBB";
        String cPath = testPath + "/AaBB";

        if (aPath.hashCode() == bPath.hashCode() && bPath.hashCode() == cPath.hashCode()) {
            String name = aPath.hashCode() + "";

            Tree parent = root.getTree(testPath);
            Tree aa = TreeUtil.addChild(parent, "AaAa", JcrConstants.NT_UNSTRUCTURED);
            addACE(aa.getPath(), testPrincipal, JCR_READ);

            Tree bb = TreeUtil.addChild(parent, "BBBB", JcrConstants.NT_UNSTRUCTURED);
            addACE(bb.getPath(), testPrincipal, JCR_READ);

            Tree cc = TreeUtil.addChild(parent, "AaBB", JcrConstants.NT_UNSTRUCTURED);
            addACE(cc.getPath(), testPrincipal, JCR_READ);
            root.commit();

            Set<String> paths = CollectionUtils.toSet(aPath, bPath, cPath);
            paths.add(testPath);

            assertEquals(2, testRoot.getChildrenCount(Long.MAX_VALUE));
            assertEquals(paths, getAccessControlledPaths(testRoot));
            assertNumPermissionsProperty(paths.size(), testRoot);

            String toRemove = null;
            for (String path : paths) {
                if (testRoot.hasChild(name) && path.equals(getAccessControlledPath(testRoot.getChild(name)))) {
                    toRemove = path;
                    break;
                }
            }

            assertNotNull(toRemove);
            paths.remove(toRemove);

            root.getTree(toRemove).remove();
            root.commit();

            assertEquals(2, testRoot.getChildrenCount(Long.MAX_VALUE));
            assertTrue(testRoot.hasChild(toRemove.hashCode() + ""));
            assertNotEquals(toRemove, getAccessControlledPath(testRoot.getChild(name)));

            assertEquals(paths, getAccessControlledPaths(testRoot));
            assertNumPermissionsProperty(paths.size(), testRoot);
        }
    }

    @Test
    public void testCollisionRemovalSubsequentAdd() throws Exception {
        Tree testRoot = getPrincipalRoot(testPrincipal);

        String aPath = testPath + "/AaAa";
        String bPath = testPath + "/BBBB";
        String cPath = testPath + "/AaBB";
        String dPath = testPath + "/BBAa";

        if (aPath.hashCode() == bPath.hashCode() && bPath.hashCode() == cPath.hashCode() && cPath.hashCode() == dPath.hashCode()) {
            String name = aPath.hashCode() + "";

            Tree parent = root.getTree(testPath);
            Tree aa = TreeUtil.addChild(parent, "AaAa", JcrConstants.NT_UNSTRUCTURED);
            addACE(aa.getPath(), testPrincipal, JCR_READ);

            Tree bb = TreeUtil.addChild(parent, "BBBB", JcrConstants.NT_UNSTRUCTURED);
            addACE(bb.getPath(), testPrincipal, JCR_READ);

            Tree cc = TreeUtil.addChild(parent, "AaBB", JcrConstants.NT_UNSTRUCTURED);
            addACE(cc.getPath(), testPrincipal, JCR_READ);
            root.commit();

            Set<String> paths = CollectionUtils.toSet(aPath, bPath, cPath);
            paths.add(testPath);

            assertEquals(2, testRoot.getChildrenCount(Long.MAX_VALUE));
            assertEquals(paths, getAccessControlledPaths(testRoot));

            String toRemove = null;
            for (String path : paths) {
                if (testRoot.hasChild(name) && path.equals(getAccessControlledPath(testRoot.getChild(name)))) {
                    toRemove = path;
                    break;
                }
            }

            paths.remove(toRemove);
            root.getTree(toRemove).remove();
            root.commit();

            Tree dd = TreeUtil.addChild(parent, "BBAa", JcrConstants.NT_UNSTRUCTURED);
            addACE(dd.getPath(), testPrincipal, JCR_READ);
            root.commit();

            assertEquals(2, testRoot.getChildrenCount(Long.MAX_VALUE));
            paths.add(dPath);
            assertEquals(paths, getAccessControlledPaths(testRoot));
        } else {
            fail();
        }
    }

    @Test
    public void testPolicyNodeNoLongerOfTypeRepACL() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acMgr.removePolicy(acl.getPath(), acl);

        Tree test = root.getTree(testPath);
        test.removeProperty(JCR_MIXINTYPES);
        TreeUtil.addChild(test, AccessControlConstants.REP_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED);

        Tree principalPermissionStore = root.getTree(PermissionConstants.PERMISSIONS_STORE_PATH).getChild(adminSession.getWorkspaceName()).getChild(testPrincipal.getName());
        Tree permissionEntry = principalPermissionStore.getChildren().iterator().next();
        assertTrue(permissionEntry.exists());
        String path = permissionEntry.getPath();

        root.commit();

        permissionEntry = root.getTree(path);
        assertFalse(permissionEntry.exists());
    }

    @Test
    public void testInvalidPolicyNodeBecomesTypeRepACL() throws Exception {
        Tree t = root.getTree(testPath).getChild("childNode");
        TreeUtil.addChild(t, AccessControlConstants.REP_POLICY, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        root.commit();

        Tree principalPermissionStore = root.getTree(PermissionConstants.PERMISSIONS_STORE_PATH).getChild(adminSession.getWorkspaceName()).getChild(testPrincipal.getName());
        assertEquals(1, principalPermissionStore.getChildrenCount(10));

        AccessControlManager acMgr = getAccessControlManager(root);
        t.getChild(REP_POLICY).remove();
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, t.getPath());
        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_READ));
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        principalPermissionStore = root.getTree(PermissionConstants.PERMISSIONS_STORE_PATH).getChild(adminSession.getWorkspaceName()).getChild(testPrincipal.getName());
        assertEquals(2, principalPermissionStore.getChildrenCount(10));
        Iterable<String> paths = Iterables.transform(principalPermissionStore.getChildren(), tree -> tree.getProperty(REP_ACCESS_CONTROLLED_PATH).getValue(Type.STRING));

        assertEquals(Set.of(testPath, t.getPath()), ImmutableSet.copyOf(paths));
    }

    @Test
    public void testToString() {
        PermissionHook h1 = createPermissionHook("wspName");
        PermissionHook h2 = new PermissionHook("default", mock(RestrictionProvider.class), mockProviderContext(mock(MountInfoProvider.class), mock(RootProvider.class), mock(TreeProvider.class)));
        assertEquals(h1.toString(), h2.toString());
    }

    @Test
    public void testHiddenChildNodeAdded() throws Exception {
        PermissionHook ph = createPermissionHook(adminSession.getWorkspaceName());

        NodeState before = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));
        NodeState after = spy(before);

        NodeState child = mock(NodeState.class);
        Iterable newCnes = Collections.singleton(new MemoryChildNodeEntry(":hidden", child));
        Iterable cnes = Iterables.concat(newCnes, before.getChildNodeEntries());
        when(after.getChildNodeEntries()).thenReturn(cnes);
        when(after.getChildNode(":hidden")).thenReturn(child);

        ph.processCommit(before, after, new CommitInfo("sid", null));

        verify(child, never()).getProperty(anyString());
    }

    @Test
    public void testHiddenChildNodeChanged() {
        PermissionHook ph = createPermissionHook(adminSession.getWorkspaceName());

        NodeState nodeState = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));

        NodeState after = spy(nodeState);
        NodeState before = spy(nodeState);

        NodeState child = mock(NodeState.class);
        Iterable hidden = Collections.singleton(new MemoryChildNodeEntry(":hidden", child));
        Iterable cnes = Iterables.concat(hidden, nodeState.getChildNodeEntries());
        when(before.getChildNodeEntries()).thenReturn(cnes);
        when(before.getChildNode(":hidden")).thenReturn(child);

        NodeState child2 = when(mock(NodeState.class).exists()).thenReturn(true).getMock();
        hidden = Collections.singleton(new MemoryChildNodeEntry(":hidden", child2));
        cnes = Iterables.concat(hidden, nodeState.getChildNodeEntries());
        when(after.getChildNodeEntries()).thenReturn(cnes);
        when(after.getChildNode(":hidden")).thenReturn(child2);

        ph.processCommit(before, after, new CommitInfo("sid", null));

        verify(child, never()).getProperty(anyString());
        verify(child2, never()).getProperty(anyString());
    }

    @Test
    public void testHiddenChildNodeDeleted() {
        PermissionHook ph = createPermissionHook(adminSession.getWorkspaceName());

        NodeState after = getTreeProvider().asNodeState(root.getTree(PathUtils.ROOT_PATH));
        NodeState before = spy(after);

        NodeState child = mock(NodeState.class);
        Iterable deletedCnes = Collections.singleton(new MemoryChildNodeEntry(":hidden", child));
        Iterable cnes = Iterables.concat(deletedCnes, after.getChildNodeEntries());
        when(before.getChildNodeEntries()).thenReturn(cnes);
        when(before.getChildNode(":hidden")).thenReturn(child);

        ph.processCommit(before, after, new CommitInfo("sid", null));

        verify(child, never()).getProperty(anyString());
    }
}
