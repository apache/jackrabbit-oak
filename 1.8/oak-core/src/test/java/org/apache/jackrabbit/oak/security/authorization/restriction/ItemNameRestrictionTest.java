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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ItemNameRestrictionTest extends AbstractSecurityTest {

    private ValueFactory vf;
    private ContentSession testSession;

    private Principal testPrincipal;
    private Group testGroup;

    @Override
    public void before() throws Exception {
        super.before();

        Tree rootTree = root.getTree("/");
        NodeUtil f = new NodeUtil(rootTree).getOrAddTree("a/d/b/e/c/f", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        NodeUtil c = f.getParent();
        c.setString("prop", "value");
        c.setString("a", "value");

        testPrincipal = getTestUser().getPrincipal();

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/a");

        vf = new ValueFactoryImpl(root, NamePathMapper.DEFAULT);
        acl.addEntry(testPrincipal,
                privilegesFromNames(
                        PrivilegeConstants.JCR_READ,
                        PrivilegeConstants.REP_ADD_PROPERTIES,
                        PrivilegeConstants.JCR_ADD_CHILD_NODES,
                        PrivilegeConstants.JCR_REMOVE_NODE), true,
                Collections.<String, Value>emptyMap(),
                ImmutableMap.of(AccessControlConstants.REP_ITEM_NAMES, new Value[] {
                        vf.createValue("a", PropertyType.NAME),
                        vf.createValue("b", PropertyType.NAME),
                        vf.createValue("c", PropertyType.NAME)}));
        acMgr.setPolicy(acl.getPath(), acl);

        UserManager uMgr = getUserManager(root);
        testGroup = uMgr.createGroup("testGroup" + UUID.randomUUID());

        root.commit();
        testSession = createTestSession();
    }

    @Override
    public void after() throws Exception {
        try {
            testSession.close();
            root.refresh();
            Tree a = root.getTree("/a");
            if (a.exists()) {
                a.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testRead() {
        Root testRoot = testSession.getLatestRoot();

        List<String> visible = ImmutableList.of("/a", "/a/d/b", "/a/d/b/e/c");
        for (String p : visible) {
            assertTrue(testRoot.getTree(p).exists());
        }

        List<String> invisible = ImmutableList.of("/", "/a/d", "/a/d/b/e", "/a/d/b/e/c/f");
        for (String p : invisible) {
            assertFalse(testRoot.getTree(p).exists());
        }

        Tree c = testRoot.getTree("/a/d/b/e/c");
        assertNull(c.getProperty(JcrConstants.JCR_PRIMARYTYPE));
        assertNull(c.getProperty("prop"));
        assertNotNull(c.getProperty("a"));
    }

    @Test
    public void testAddProperty() throws Exception {
        Root testRoot = testSession.getLatestRoot();

        List<String> paths = ImmutableList.of("/a", "/a/d/b", "/a/d/b/e/c");
        for (String p : paths) {
            Tree t = testRoot.getTree(p);
            t.setProperty("b", "anyvalue");
            testRoot.commit();
        }

        for (String p : paths) {
            Tree t = testRoot.getTree(p);
            try {
                t.setProperty("notAllowed", "anyvalue");
                testRoot.commit();
                fail();
            } catch (CommitFailedException e) {
                // success
                assertTrue(e.isAccessViolation());
            } finally {
                testRoot.refresh();
            }
        }
    }

    @Test
    public void testModifyProperty() throws Exception {
        Root testRoot = testSession.getLatestRoot();
        Tree c = testRoot.getTree("/a/d/b/e/c");

        try {
            c.setProperty("a", "anyvalue");
            testRoot.commit();
            fail();
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessViolation());
        } finally {
            testRoot.refresh();
        }
    }

    @Test
    public void testAddChild() throws Exception {
        Root testRoot = testSession.getLatestRoot();

        List<String> paths = ImmutableList.of("/a", "/a/d/b", "/a/d/b/e/c");
        for (String p : paths) {
            NodeUtil t = new NodeUtil(testRoot.getTree(p));
            t.addChild("c", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            testRoot.commit();
        }
    }

    @Test
    public void testRemoveTree() throws Exception {
        Root testRoot = testSession.getLatestRoot();
        List<String> paths = ImmutableList.of("/a/d/b/e/c", "/a/d/b", "/a");
        for (String p : paths) {
            try {
                testRoot.getTree(p).remove();
                testRoot.commit();
                fail();
            } catch (CommitFailedException e) {
                // success
                assertTrue(e.isAccessViolation());
            } finally {
                testRoot.refresh();
            }
        }
    }

    @Test
    public void testRemoveTree2() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/a");

        acl.addEntry(testPrincipal,
                privilegesFromNames(PrivilegeConstants.JCR_READ, PrivilegeConstants.JCR_REMOVE_CHILD_NODES), true);
        acMgr.setPolicy(acl.getPath(), acl);
        root.commit();

        Root testRoot = testSession.getLatestRoot();
        List<String> paths = ImmutableList.of("/a/d/b/e/c", "/a/d/b");
        for (String p : paths) {
            testRoot.getTree(p).remove();
            testRoot.commit();
        }

        try {
            testRoot.getTree("/a").remove();
            testRoot.commit();
            fail();
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessViolation());
        } finally {
            testRoot.refresh();
        }
    }

    @Test
    public void testModifyMembersOnly() throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        String path = PathUtils.getAncestorPath(UserConstants.DEFAULT_USER_PATH, 1);
        try {
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
            acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_READ), true);
            acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.REP_USER_MANAGEMENT), true,
                    Collections.<String,Value>emptyMap(),
                    ImmutableMap.<String,Value[]>of (AccessControlConstants.REP_ITEM_NAMES, new Value[] {
                                            vf.createValue(UserConstants.REP_MEMBERS, PropertyType.NAME)}));
            acMgr.setPolicy(acl.getPath(), acl);
            root.commit();

            Root testRoot = testSession.getLatestRoot();

            UserManager uMgr = getUserManager(testRoot);

            // adding a group member must succeed
            Group gr = uMgr.getAuthorizable(testGroup.getID(), Group.class);
            User u = uMgr.getAuthorizable(getTestUser().getID(), User.class);
            gr.addMember(u);
            testRoot.commit();

            // changing the pw property of the test user must fail
            try {
                u.changePassword("blub");
                testRoot.commit();
                fail();
            } catch (CommitFailedException e) {
                // success
                assertTrue(e.isAccessViolation());
            } finally {
                testRoot.refresh();
            }
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessViolation());
        } finally {
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
            if (acl != null) {
                acMgr.removePolicy(acl.getPath(), acl);
                root.commit();
            }
        }
    }
}