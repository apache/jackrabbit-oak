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
package org.apache.jackrabbit.oak.security.user;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtility;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * UserManagerImplTest...
 */
public class UserManagerImplTest extends AbstractSecurityTest {

    private Root root;
    private UserManagerImpl userMgr;

    @Before
    public void before() throws Exception {
        super.before();

        root = admin.getLatestRoot();
        userMgr = new UserManagerImpl(null, root, NamePathMapper.DEFAULT, getSecurityProvider());
    }

    @Test
    public void testSetPassword() throws Exception {
        User user = userMgr.createUser("a", "pw");
        root.commit();

        List<String> pwds = new ArrayList<String>();
        pwds.add("pw");
        pwds.add("");
        pwds.add("{sha1}pw");

        Tree userTree = root.getTree(user.getPath());
        for (String pw : pwds) {
            userMgr.setPassword(userTree, pw, true);
            String pwHash = userTree.getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
            assertNotNull(pwHash);
            assertTrue(PasswordUtility.isSame(pwHash, pw));
        }

        for (String pw : pwds) {
            userMgr.setPassword(userTree, pw, false);
            String pwHash = userTree.getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
            assertNotNull(pwHash);
            if (!pw.startsWith("{")) {
                assertTrue(PasswordUtility.isSame(pwHash, pw));
            } else {
                assertFalse(PasswordUtility.isSame(pwHash, pw));
                assertEquals(pw, pwHash);
            }
        }
    }

    @Test
    public void setPasswordNull() throws Exception {
        User user = userMgr.createUser("a", null);
        root.commit();

        Tree userTree = root.getTree(user.getPath());
        try {
            userMgr.setPassword(userTree, null, true);
            fail("setting null password should fail");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            userMgr.setPassword(userTree, null, false);
            fail("setting null password should fail");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testGetPasswordHash() throws Exception {
        User user = userMgr.createUser("a", null);
        root.commit();

        Tree userTree = root.getTree(user.getPath());
        assertNull(userTree.getProperty(UserConstants.REP_PASSWORD));
    }

    @Test
    public void testIsAutoSave() throws Exception {
        assertFalse(userMgr.isAutoSave());
    }

    @Test
    public void testAutoSave() throws Exception {
        try {
            userMgr.autoSave(true);
            fail("should fail");
        } catch (UnsupportedRepositoryOperationException e) {
            // success
        }
    }

    @Test
    public void testEnforceAuthorizableFolderHierarchy() throws RepositoryException, CommitFailedException {
        User user = userMgr.createUser("testUser", null);
        root.commit();

        NodeUtil userNode = new NodeUtil(root.getTree(user.getPath()));

        NodeUtil folder = userNode.addChild("folder", UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
        String path = folder.getTree().getPath();
        try {
            // authNode - authFolder -> create User
            try {
                Principal p = new TestPrincipal("test2");
                Authorizable a = userMgr.createUser(p.getName(), p.getName(), p, path);
                root.commit();

                fail("Users may not be nested.");
            } catch (CommitFailedException e) {
                // success
            } finally {
                root.refresh();
                Authorizable a = userMgr.getAuthorizable("test2");
                if (a != null) {
                    a.remove();
                    root.commit();
                }
            }
        } finally {
            root.refresh();
            folder.getTree().remove();
            root.commit();
        }

        NodeUtil someContent = userNode.addChild("mystuff", JcrConstants.NT_UNSTRUCTURED);
        path = someContent.getTree().getPath();
        try {
            // authNode - anyNode -> create User
            try {
                Principal p = new TestPrincipal("test3");
                userMgr.createUser(p.getName(), p.getName(), p, path);
                root.commit();

                fail("Users may not be nested.");
            } catch (CommitFailedException e) {
                // success
            } finally {
                root.refresh();
                Authorizable a = userMgr.getAuthorizable("test3");
                if (a != null) {
                    a.remove();
                    root.commit();
                }
            }

            // authNode - anyNode - authFolder -> create User
            folder = someContent.addChild("folder", UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
            root.commit(); // this time save node structure
            try {
                Principal p = new TestPrincipal("test4");
                userMgr.createUser(p.getName(), p.getName(), p, folder.getTree().getPath());
                root.commit();

                fail("Users may not be nested.");
            } catch (CommitFailedException e) {
                // success
            } finally {
                root.refresh();
                Authorizable a = userMgr.getAuthorizable("test4");
                if (a != null) {
                    a.remove();
                    root.commit();
                }
            }
        } finally {
            root.refresh();
            Tree t = root.getTree(path);
            if (t != null) {
                t.remove();
                root.commit();
            }
        }
    }

    private class TestPrincipal implements Principal {

        private final String name;

        private TestPrincipal(String name) {
            this.name = name;
        }
        @Override
        public String getName() {
            return name;
        }
    }
}