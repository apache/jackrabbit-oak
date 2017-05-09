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

import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableSet;
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
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeBitsProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Testing the {@code PermissionHook}
 */
public class PermissionHookTest extends AbstractSecurityTest implements AccessControlConstants, PermissionConstants, PrivilegeConstants {

    protected String testPath = "/testPath";
    protected String childPath = "/testPath/childNode";

    protected Principal testPrincipal;
    protected PrivilegeBitsProvider bitsProvider;
    protected List<Principal> principals = new ArrayList<Principal>();

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        testPrincipal = getTestUser().getPrincipal();
        NodeUtil rootNode = new NodeUtil(root.getTree("/"), namePathMapper);
        NodeUtil testNode = rootNode.addChild("testPath", JcrConstants.NT_UNSTRUCTURED);
        testNode.addChild("childNode", JcrConstants.NT_UNSTRUCTURED);

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addAccessControlEntry(testPrincipal, privilegesFromNames(JCR_ADD_CHILD_NODES));
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privilegesFromNames(JCR_READ));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        bitsProvider = new PrivilegeBitsProvider(root);
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

    protected Tree getPrincipalRoot(@Nonnull Principal principal) {
        return root.getTree(PERMISSIONS_STORE_PATH).getChild(adminSession.getWorkspaceName()).getChild(principal.getName());
    }

    protected Tree getEntry(@Nonnull Principal principal, String accessControlledPath, long index) throws Exception {
        Tree principalRoot = getPrincipalRoot(principal);
        Tree parent = principalRoot.getChild(PermissionUtil.getEntryName(accessControlledPath));
        Tree entry = parent.getChild(String.valueOf(index));
        if (!entry.exists()) {
            throw new RepositoryException("no such entry");
        }
        return entry;
    }

    protected long cntEntries(Tree parent) {
        long cnt = parent.getChildrenCount(Long.MAX_VALUE);
        for (Tree child : parent.getChildren()) {
            cnt += cntEntries(child);
        }
        return cnt;
    }

    protected void createPrincipals() throws Exception {
        if (principals.isEmpty()) {
            for (int i = 0; i < 10; i++) {
                Group gr = getUserManager(root).createGroup("testGroup" + i);
                principals.add(gr.getPrincipal());
            }
            root.commit();
        }
    }

    static protected void assertIndex(int expected, Tree entry) {
        assertEquals(expected, Integer.parseInt(entry.getName()));
    }

    @Test
    public void testModifyRestrictions() throws Exception {
        Tree testAce = root.getTree(testPath + "/rep:policy").getChildren().iterator().next();
        assertEquals(testPrincipal.getName(), testAce.getProperty(REP_PRINCIPAL_NAME).getValue(Type.STRING));

        // add a new restriction node through the OAK API instead of access control manager
        NodeUtil node = new NodeUtil(testAce);
        NodeUtil restrictions = node.addChild(REP_RESTRICTIONS, NT_REP_RESTRICTIONS);
        restrictions.setString(REP_GLOB, "*");
        String restrictionsPath = restrictions.getTree().getPath();
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
        NodeUtil ace = new NodeUtil(aclTree).addChild("denyEveryoneLockMgt", NT_REP_DENY_ACE);
        ace.setString(REP_PRINCIPAL_NAME, EveryonePrincipal.NAME);
        ace.setNames(AccessControlConstants.REP_PRIVILEGES, JCR_LOCK_MANAGEMENT);
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
        NodeUtil ace = new NodeUtil(aclTree).addChild("denyEveryoneLockMgt", NT_REP_DENY_ACE);
        ace.setString(REP_PRINCIPAL_NAME, EveryonePrincipal.NAME);
        ace.setNames(AccessControlConstants.REP_PRIVILEGES, JCR_LOCK_MANAGEMENT);

        // reorder the new entry before the remaining existing entry
        ace.getTree().orderBefore(name);

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
        assertEquals(PermissionStore.DYNAMIC_ALL_BITS, ps.getValue(Type.LONG, 0).longValue());

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
}