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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ItemNameRestrictionTest extends AbstractRestrictionTest {

    @Override
    boolean addEntry(@NotNull JackrabbitAccessControlList acl) throws RepositoryException {
        return acl.addEntry(testPrincipal,
                privilegesFromNames(
                        PrivilegeConstants.JCR_READ,
                        PrivilegeConstants.REP_ADD_PROPERTIES,
                        PrivilegeConstants.JCR_ADD_CHILD_NODES,
                        PrivilegeConstants.JCR_REMOVE_NODE), true,
                Collections.emptyMap(),
                Map.of(AccessControlConstants.REP_ITEM_NAMES, new Value[] {
                        vf.createValue("a", PropertyType.NAME),
                        vf.createValue("b", PropertyType.NAME),
                        vf.createValue("c", PropertyType.NAME)}));
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

    @Test(expected = CommitFailedException.class)
    public void testModifyProperty() throws Exception {
        Root testRoot = testSession.getLatestRoot();
        Tree c = testRoot.getTree("/a/d/b/e/c");

        try {
            c.setProperty("a", "anyvalue");
            testRoot.commit();
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessViolation());
            throw e;
        }
    }

    @Test
    public void testAddChild() throws Exception {
        Root testRoot = testSession.getLatestRoot();

        List<String> paths = ImmutableList.of("/a", "/a/d/b", "/a/d/b/e/c");
        for (String p : paths) {
            Tree t = testRoot.getTree(p);
            TreeUtil.addChild(t, "c", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            testRoot.commit();
        }
    }

    @Test
    public void testRemoveTree() {
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

    @Test(expected = CommitFailedException.class)
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
        } catch (CommitFailedException e) {
            // success
            assertTrue(e.isAccessViolation());
            throw e;
        }
    }

    @Test
    public void testModifyMembersOnly() throws Exception {
        Group testGroup = getUserManager(root).createGroup("testGroup" + UUID.randomUUID());
        root.commit();
        
        AccessControlManager acMgr = getAccessControlManager(root);
        String path = PathUtils.getAncestorPath(UserConstants.DEFAULT_USER_PATH, 1);
        try {
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
            acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.JCR_READ), true);
            acl.addEntry(testPrincipal, privilegesFromNames(PrivilegeConstants.REP_USER_MANAGEMENT), true,
                    Collections.emptyMap(),
                    Map.of(AccessControlConstants.REP_ITEM_NAMES, new Value[] {
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