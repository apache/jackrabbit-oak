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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @since OAK 1.0
 */
public class UserManagerImplTest extends AbstractSecurityTest {

    private UserManagerImpl userMgr;
    private String testUserId = "testUser";
    private Set<String> beforeAuthorizables = new HashSet<String>();

    @Before
    public void before() throws Exception {
        super.before();

        userMgr = new UserManagerImpl(root, namePathMapper, getSecurityProvider());
        beforeAuthorizables.clear();
        Iterator<Authorizable> iter = userMgr.findAuthorizables("jcr:primaryType", null, UserManager.SEARCH_TYPE_AUTHORIZABLE);
        while (iter.hasNext()) {
            beforeAuthorizables.add(iter.next().getID());
        }
    }

    @After
    public void after() throws Exception {
        Iterator<Authorizable> iter = userMgr.findAuthorizables("jcr:primaryType", null, UserManager.SEARCH_TYPE_AUTHORIZABLE);
        while (iter.hasNext()) {
            Authorizable auth = iter.next();
            if (!beforeAuthorizables.remove(auth.getID())) {
                try {
                    auth.remove();
                } catch (RepositoryException e) {
                    // ignore
                }
            }
        }
        super.after();
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-3191">OAK-3191</a>
     */
    @Test
    public void testGetAuthorizableByEmptyId() throws Exception {
        assertNull(userMgr.getAuthorizable(""));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-3191">OAK-3191</a>
     */
    @Test
    public void testGetTypedAuthorizableByEmptyId() throws Exception {
        assertNull(userMgr.getAuthorizable("", User.class));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-3191">OAK-3191</a>
     */
    @Test
    public void testGetAuthorizableByNullId() throws Exception {
        assertNull(userMgr.getAuthorizable((String) null));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-3191">OAK-3191</a>
     */
    @Test
    public void testGetTypedAuthorizableByNullId() throws Exception {
        assertNull(userMgr.getAuthorizable(null, User.class));
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-3191">OAK-3191</a>
     */
    @Test
    public void testGetTypedAuthorizableByNullPrincipal() throws Exception {
        assertNull(userMgr.getAuthorizable((Principal) null));
    }

    @Test
    public void testSetPassword() throws Exception {
        User user = userMgr.createUser(testUserId, "pw");
        root.commit();

        List<String> pwds = new ArrayList<String>();
        pwds.add("pw");
        pwds.add("");
        pwds.add("{sha1}pw");

        Tree userTree = root.getTree(user.getPath());
        for (String pw : pwds) {
            userMgr.setPassword(userTree, testUserId, pw, true);
            String pwHash = userTree.getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
            assertNotNull(pwHash);
            assertTrue(PasswordUtil.isSame(pwHash, pw));
        }

        for (String pw : pwds) {
            userMgr.setPassword(userTree, testUserId, pw, false);
            String pwHash = userTree.getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
            assertNotNull(pwHash);
            if (!pw.startsWith("{")) {
                assertTrue(PasswordUtil.isSame(pwHash, pw));
            } else {
                assertFalse(PasswordUtil.isSame(pwHash, pw));
                assertEquals(pw, pwHash);
            }
        }
    }

    @Test
    public void setPasswordNull() throws Exception {
        User user = userMgr.createUser(testUserId, null);
        root.commit();

        Tree userTree = root.getTree(user.getPath());
        try {
            userMgr.setPassword(userTree, testUserId, null, true);
            fail("setting null password should fail");
        } catch (NullPointerException e) {
            // expected
        }

        try {
            userMgr.setPassword(userTree, testUserId, null, false);
            fail("setting null password should fail");
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void testGetPasswordHash() throws Exception {
        User user = userMgr.createUser(testUserId, null);
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
        User user = userMgr.createUser(testUserId, null);
        root.commit();

        NodeUtil userNode = new NodeUtil(root.getTree(user.getPath()));

        NodeUtil folder = userNode.addChild("folder", UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
        String path = folder.getTree().getPath();

        // authNode - authFolder -> create User
        try {
            Principal p = new PrincipalImpl("test2");
            userMgr.createUser(p.getName(), p.getName(), p, path);
            root.commit();

            fail("Users may not be nested.");
        } catch (CommitFailedException e) {
            // success
        } finally {
            Authorizable a = userMgr.getAuthorizable("test2");
            if (a != null) {
                a.remove();
                root.commit();
            }
        }

        NodeUtil someContent = userNode.addChild("mystuff", JcrConstants.NT_UNSTRUCTURED);
        path = someContent.getTree().getPath();
        try {
            // authNode - anyNode -> create User
            try {
                Principal p = new PrincipalImpl("test3");
                userMgr.createUser(p.getName(), p.getName(), p, path);
                root.commit();

                fail("Users may not be nested.");
            } catch (CommitFailedException e) {
                // success
            } finally {
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
                Principal p = new PrincipalImpl("test4");
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
            if (t.exists()) {
                t.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testFindWithNullValue() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null);
        assertTrue(result.hasNext());
    }

    @Test
    public void testFindWithNullValue2() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables("./" + UserConstants.REP_PRINCIPAL_NAME, null);
        assertTrue(result.hasNext());
    }

    @Test
    public void testConcurrentCreateUser() throws Exception {
        final List<Exception> exceptions = new ArrayList<Exception>();
        List<Thread> workers = new ArrayList<Thread>();
        for (int i=0; i<10; i++) {
            final String userId = "foo-user-" + i;
            workers.add(new Thread(new Runnable() {
                public void run() {
                    try {
                        ContentSession admin = login(getAdminCredentials());
                        Root root = admin.getLatestRoot();
                        UserManager userManager = new UserManagerImpl(root, namePathMapper, getSecurityProvider());
                        userManager.createUser(userId, "pass");
                        root.commit();
                        admin.close();
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            }));
        }
        for (Thread t : workers) {
            t.start();
        }
        for (Thread t : workers) {
            t.join();
        }
        for (Exception e : exceptions) {
            e.printStackTrace();
        }
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
    }

    /**
     * Test related to OAK-1922: Asserting that the default behavior is such that
     * no rep:pwd node is created upon user-creation.
     *
     * @since Oak 1.1
     */
    @Test
    public void testNewUserHasNoPwdNode() throws Exception {
        String newUserId = "newuser" + UUID.randomUUID();
        User user = null;
        try {
            user = getUserManager(root).createUser(newUserId, newUserId);
            root.commit();

            Assert.assertFalse(root.getTree(user.getPath()).hasChild(UserConstants.REP_PWD));
            Assert.assertFalse(user.hasProperty(UserConstants.REP_PWD + "/" + UserConstants.REP_PASSWORD_LAST_MODIFIED));
        } finally {
            if (user != null) {
                user.remove();
                root.commit();
            }
        }
    }
}