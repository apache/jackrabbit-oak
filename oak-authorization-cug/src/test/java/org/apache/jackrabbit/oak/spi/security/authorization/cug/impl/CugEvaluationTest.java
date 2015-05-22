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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.Permissions;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CugEvaluationTest extends AbstractCugTest implements NodeTypeConstants {

    private static final String TEST_GROUP_ID = "testGroup";
    private static final String TEST_USER2_ID = "testUser2";

    private ContentSession testSession;
    private Root testRoot;
    private Principal testGroupPrincipal;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        Group testGroup = getUserManager(root).createGroup(TEST_GROUP_ID);
        testGroupPrincipal = testGroup.getPrincipal();
        User testUser2 = getUserManager(root).createUser(TEST_USER2_ID, TEST_USER2_ID);
        testGroup.addMember(testUser2);
        root.commit();

        // add more child nodes
        NodeUtil n = new NodeUtil(root.getTree(SUPPORTED_PATH));
        n.addChild("a", NT_OAK_UNSTRUCTURED).addChild("b", NT_OAK_UNSTRUCTURED).addChild("c", NT_OAK_UNSTRUCTURED);
        n.addChild("aa", NT_OAK_UNSTRUCTURED).addChild("bb", NT_OAK_UNSTRUCTURED).addChild("cc", NT_OAK_UNSTRUCTURED);

        // create cugs
        createCug("/content/a", testGroupPrincipal);
        createCug("/content/aa/bb", testGroupPrincipal);
        createCug("/content/a/b/c", EveryonePrincipal.getInstance());
        createCug("/content2", EveryonePrincipal.getInstance());

        // setup regular acl at /content
        AccessControlManager acMgr = getAccessControlManager(root);
        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/content");
        acl.addAccessControlEntry(getTestUser().getPrincipal(), privilegesFromNames(
                PrivilegeConstants.JCR_READ));
        acl.addAccessControlEntry(testGroupPrincipal, privilegesFromNames(
                PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL)
        );
        acMgr.setPolicy("/content", acl);

        root.commit();

        testSession = createTestSession();
        testRoot = testSession.getLatestRoot();
    }

    @Override
    public void after() throws Exception {
        try {
            // revert transient pending changes (that might be invalid)
            root.refresh();

            // remove the test group and second test user
            Authorizable testGroup = getUserManager(root).getAuthorizable(TEST_GROUP_ID);
            if (testGroup != null) {
                testGroup.remove();
            }
            Authorizable testUser2 = getUserManager(root).getAuthorizable(TEST_USER2_ID);
            if (testUser2 != null) {
                testUser2.remove();
            }
            root.commit();
        } finally {
            if (testSession != null) {
                testSession.close();
            }
            super.after();
        }
    }

    private PermissionProvider createPermissionProvider(Set<Principal> principals) {
        return getSecurityProvider().getConfiguration(AuthorizationConfiguration.class).getPermissionProvider(root, adminSession.getWorkspaceName(), principals);
    }

    @Test
    public void testRead() throws Exception {
        List<String> noAccess = ImmutableList.of(
                "/", UNSUPPORTED_PATH, /* no access */
                "/content/a", "/content/a/b", "/content/aa/bb", /* granted by ace, denied by cug */
                "/content2"            /* granted by cug only */
        );
        for (String p : noAccess) {
            assertFalse(p, testRoot.getTree(p).exists());
        }

        List<String> readAccess = ImmutableList.of("/content", "/content/subtree", "/content/a/b/c", "/content/aa");
        for (String p : readAccess) {
            assertTrue(p, testRoot.getTree(p).exists());
        }
    }

    @Test
    public void testReadAcl() throws Exception {
        assertFalse(testRoot.getTree("/content/rep:policy").exists());
    }

    @Test
    public void testReadAcl2() throws Exception {
        ContentSession cs = login(new SimpleCredentials(TEST_USER2_ID, TEST_USER2_ID.toCharArray()));
        try {
            Root r = cs.getLatestRoot();

            assertTrue(r.getTree("/content/rep:policy").exists());
            assertFalse(r.getTree("/content2/rep:cugPolicy").exists());
        } finally {
            cs.close();
        }
    }

    @Ignore("FIXME: cugpolicy not detected as ac-content") // FIXME
    @Test
    public void testReadCug() throws Exception {
        List<String> noAccess = ImmutableList.of(
                "/content/a/rep:cugPolicy", "/content/aa/bb/rep:cugPolicy", "/content2/rep:cugPolicy"
        );
        for (String p : noAccess) {
            assertFalse(p, testRoot.getTree(p).exists());
        }
    }

    @Test
    public void testReadCug2() throws Exception {
        ContentSession cs = login(new SimpleCredentials(TEST_USER2_ID, TEST_USER2_ID.toCharArray()));
        try {
            Root r = cs.getLatestRoot();

            assertTrue(r.getTree("/content/a/rep:cugPolicy").exists());
            assertFalse(r.getTree("/content2/rep:cugPolicy").exists());
        } finally {
            cs.close();
        }
    }

    @Test
    public void testWrite() throws Exception {
        List<String> readOnly = ImmutableList.of("/content", "/content/a/b/c");
        for (String p : readOnly) {
            try {
                NodeUtil content = new NodeUtil(testRoot.getTree(p));
                content.addChild("writeTest", NT_OAK_UNSTRUCTURED);
                testRoot.commit();
                fail();
            } catch (CommitFailedException e) {
                assertTrue(e.isAccessViolation());
            } finally {
                testRoot.refresh();
            }
        }
    }

    @Test
    public void testWrite2() throws Exception {
        ContentSession cs = login(new SimpleCredentials(TEST_USER2_ID, TEST_USER2_ID.toCharArray()));
        Root r = cs.getLatestRoot();
        try {
            List<String> readOnly = ImmutableList.of("/content", "/content/a/b/c");
            for (String p : readOnly) {
                NodeUtil content = new NodeUtil(r.getTree(p));
                content.addChild("writeTest", NT_OAK_UNSTRUCTURED);
                r.commit();
            }
        } finally {
            r.refresh();
            cs.close();
        }
    }

    @Test
    public void testWriteAcl() throws Exception {
        ContentSession cs = login(new SimpleCredentials(TEST_USER2_ID, TEST_USER2_ID.toCharArray()));
        Root r = cs.getLatestRoot();
        try {
            Tree tree = r.getTree("/content/a/b/c");
            tree.setProperty(JCR_MIXINTYPES, ImmutableList.of(MIX_REP_CUG_MIXIN, AccessControlConstants.MIX_REP_ACCESS_CONTROLLABLE), Type.NAMES);
            tree.addChild(AccessControlConstants.REP_POLICY).setProperty(JCR_PRIMARYTYPE, AccessControlConstants.NT_REP_ACL, Type.NAME);
            r.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
        } finally {
            r.refresh();
        }
    }

    @Ignore("FIXME: cugpolicy not detected as ac-content") // FIXME
    @Test
    public void testWriteCug() throws Exception {
        ContentSession cs = login(new SimpleCredentials(TEST_USER2_ID, TEST_USER2_ID.toCharArray()));
        Root r = cs.getLatestRoot();
        try {
            // modify the existing cug
            Tree tree = r.getTree("/content/a/rep:cugPolicy");
            tree.setProperty(REP_PRINCIPAL_NAMES, ImmutableList.of(EveryonePrincipal.NAME, testGroupPrincipal.getName()), Type.STRINGS);
            r.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
        } finally {
            r.refresh();
        }
    }

    @Test
    public void testIsGranted() throws Exception {
        Tree content = root.getTree("/content");
        Tree a = root.getTree("/content/a");
        Tree c = root.getTree("/content/a/b/c");

        // testGroup
        Set<Principal> principals = ImmutableSet.of(testGroupPrincipal);
        PermissionProvider pp = createPermissionProvider(principals);

        assertTrue(pp.isGranted(content, null, Permissions.READ));
        assertTrue(pp.isGranted(a, null, Permissions.READ));
        assertFalse(pp.isGranted(c, null, Permissions.READ));

        assertTrue(pp.isGranted(content, null, Permissions.READ_ACCESS_CONTROL));
        assertTrue(pp.isGranted(a, null, Permissions.READ_ACCESS_CONTROL));
        assertTrue(pp.isGranted(c, null, Permissions.READ_ACCESS_CONTROL));

        // everyone
        principals = ImmutableSet.<Principal>of(EveryonePrincipal.getInstance());
        pp = createPermissionProvider(principals);

        assertFalse(pp.isGranted(content, null, Permissions.READ));
        assertFalse(pp.isGranted(a, null, Permissions.READ));
        assertFalse(pp.isGranted(c, null, Permissions.READ));

        assertFalse(pp.isGranted(content, null, Permissions.READ_ACCESS_CONTROL));
        assertFalse(pp.isGranted(a, null, Permissions.READ_ACCESS_CONTROL));
        assertFalse(pp.isGranted(c, null, Permissions.READ_ACCESS_CONTROL));

        // testGroup + everyone
        principals = ImmutableSet.of(testGroupPrincipal, EveryonePrincipal.getInstance());
        pp = createPermissionProvider(principals);

        assertTrue(pp.isGranted(content, null, Permissions.READ));
        assertTrue(pp.isGranted(a, null, Permissions.READ));
        assertTrue(pp.isGranted(c, null, Permissions.READ));

        assertTrue(pp.isGranted(content, null, Permissions.READ_ACCESS_CONTROL));
        assertTrue(pp.isGranted(a, null, Permissions.READ_ACCESS_CONTROL));
        assertTrue(pp.isGranted(c, null, Permissions.READ_ACCESS_CONTROL));

        // testUser + everyone
        principals = ImmutableSet.of(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        pp = createPermissionProvider(principals);

        assertTrue(pp.isGranted(content, null, Permissions.READ));
        assertFalse(pp.isGranted(a, null, Permissions.READ));
        assertTrue(pp.isGranted(c, null, Permissions.READ));

        assertFalse(pp.isGranted(content, null, Permissions.READ_ACCESS_CONTROL));
        assertFalse(pp.isGranted(a, null, Permissions.READ_ACCESS_CONTROL));
        assertFalse(pp.isGranted(c, null, Permissions.READ_ACCESS_CONTROL));
    }

    @Test
    public void testHasPrivileges() throws Exception {
        Tree content = root.getTree("/content");
        Tree a = root.getTree("/content/a");
        Tree c = root.getTree("/content/a/b/c");

        // testGroup
        Set<Principal> principals = ImmutableSet.of(testGroupPrincipal);
        PermissionProvider pp = createPermissionProvider(principals);

        assertTrue(pp.hasPrivileges(content, PrivilegeConstants.JCR_READ));
        assertTrue(pp.hasPrivileges(a, PrivilegeConstants.JCR_READ));
        assertFalse(pp.hasPrivileges(c, PrivilegeConstants.JCR_READ));

        assertTrue(pp.hasPrivileges(content, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        assertTrue(pp.hasPrivileges(a, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        assertTrue(pp.hasPrivileges(c, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));

        // everyone
        principals = ImmutableSet.<Principal>of(EveryonePrincipal.getInstance());
        pp = createPermissionProvider(principals);

        assertFalse(pp.hasPrivileges(content, PrivilegeConstants.JCR_READ));
        assertFalse(pp.hasPrivileges(a, PrivilegeConstants.JCR_READ));
        assertFalse(pp.hasPrivileges(c, PrivilegeConstants.JCR_READ));

        assertFalse(pp.hasPrivileges(content, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        assertFalse(pp.hasPrivileges(a, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        assertFalse(pp.hasPrivileges(c, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));

        // testGroup + everyone
        principals = ImmutableSet.of(testGroupPrincipal, EveryonePrincipal.getInstance());
        pp = createPermissionProvider(principals);

        assertTrue(pp.hasPrivileges(content, PrivilegeConstants.JCR_READ));
        assertTrue(pp.hasPrivileges(a, PrivilegeConstants.JCR_READ));
        assertTrue(pp.hasPrivileges(c, PrivilegeConstants.JCR_READ));

        assertTrue(pp.hasPrivileges(content, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        assertTrue(pp.hasPrivileges(a, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        assertTrue(pp.hasPrivileges(c, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));

        // testUser + everyone
        principals = ImmutableSet.of(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        pp = createPermissionProvider(principals);

        assertTrue(pp.hasPrivileges(content, PrivilegeConstants.JCR_READ));
        assertFalse(pp.hasPrivileges(a, PrivilegeConstants.JCR_READ));
        assertTrue(pp.hasPrivileges(c, PrivilegeConstants.JCR_READ));

        assertFalse(pp.hasPrivileges(content, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        assertFalse(pp.hasPrivileges(a, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
        assertFalse(pp.hasPrivileges(c, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL));
    }

    @Test
    public void testHasAllPrivileges() throws Exception {
        // testGroup
        Set<Principal> principals = ImmutableSet.of(testGroupPrincipal);
        PermissionProvider pp = createPermissionProvider(principals);

        assertFalse(pp.hasPrivileges(root.getTree("/content"), PrivilegeConstants.JCR_ALL));
        assertFalse(pp.hasPrivileges(root.getTree("/content/a"), PrivilegeConstants.JCR_ALL));
        assertFalse(pp.hasPrivileges(root.getTree("/content/b/c"), PrivilegeConstants.JCR_ALL));
    }

    @Test
    public void testHasAllPrivileges2() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/content/a");
        acl.addAccessControlEntry(testGroupPrincipal, privilegesFromNames(PrivilegeConstants.JCR_ALL));
        acMgr.setPolicy("/content/a", acl);
        root.commit();

        // testGroup
        Set<Principal> principals = ImmutableSet.of(testGroupPrincipal);
        PermissionProvider pp = createPermissionProvider(principals);

        assertFalse(pp.hasPrivileges(root.getTree("/content"), PrivilegeConstants.JCR_ALL));
        assertTrue(pp.hasPrivileges(root.getTree("/content/a"), PrivilegeConstants.JCR_ALL));
        assertTrue(pp.hasPrivileges(root.getTree("/content/a/b"), PrivilegeConstants.JCR_ALL));
        assertFalse(pp.hasPrivileges(root.getTree("/content/a/b/c"), PrivilegeConstants.JCR_ALL));
    }

    @Test
    public void testHasAllPrivileges3() throws Exception {
        // admin principal
        Set<Principal> principals = adminSession.getAuthInfo().getPrincipals();
        PermissionProvider pp = createPermissionProvider(principals);

        assertTrue(pp.hasPrivileges(root.getTree("/content"), PrivilegeConstants.JCR_ALL));
        assertTrue(pp.hasPrivileges(root.getTree("/content/a"), PrivilegeConstants.JCR_ALL));
        assertTrue(pp.hasPrivileges(root.getTree("/content/a/b/c"), PrivilegeConstants.JCR_ALL));
    }

    @Ignore() // FIXME
    @Test
    public void testGetPrivileges() throws Exception {
        Tree content = root.getTree("/content");
        Tree a = root.getTree("/content/a");
        Tree c = root.getTree("/content/a/b/c");

        Set<String> r = ImmutableSet.of(PrivilegeConstants.JCR_READ);
        Set<String> w_rac = ImmutableSet.of(PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);
        Set<String> r_w_rac = ImmutableSet.of(PrivilegeConstants.JCR_READ, PrivilegeConstants.REP_WRITE, PrivilegeConstants.JCR_READ_ACCESS_CONTROL);

        // testGroup
        Set<Principal> principals = ImmutableSet.of(testGroupPrincipal);
        PermissionProvider pp = createPermissionProvider(principals);

        assertEquals(r_w_rac, pp.getPrivileges(content));
        assertEquals(r_w_rac, pp.getPrivileges(a));
        assertEquals(w_rac, pp.getPrivileges(c));

        // everyone
        principals = ImmutableSet.<Principal>of(EveryonePrincipal.getInstance());
        pp = createPermissionProvider(principals);

        assertTrue(pp.getPrivileges(content).isEmpty());
        assertTrue(pp.getPrivileges(a).isEmpty());
        assertTrue(pp.getPrivileges(c).isEmpty());

        // testGroup + everyone
        principals = ImmutableSet.of(testGroupPrincipal, EveryonePrincipal.getInstance());
        pp = createPermissionProvider(principals);

        assertEquals(r_w_rac, pp.getPrivileges(content));
        assertEquals(r_w_rac, pp.getPrivileges(a));
        assertEquals(r_w_rac, pp.getPrivileges(c));

        // testUser + everyone
        principals = ImmutableSet.of(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
        pp = createPermissionProvider(principals);

        assertEquals(r, pp.getPrivileges(content));
        assertTrue(pp.getPrivileges(a).isEmpty());
        assertEquals(r, pp.getPrivileges(c));
    }
}