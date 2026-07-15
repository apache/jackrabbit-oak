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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.impl.LocalNameMapper;
import org.apache.jackrabbit.oak.namepath.impl.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.plugins.value.jcr.PartialValueFactory;
import org.apache.jackrabbit.oak.security.user.monitor.UserMonitor;
import org.apache.jackrabbit.oak.security.user.monitor.UserMonitorImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipService;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @since OAK 1.0
 */
public class UserManagerImplTest extends AbstractSecurityTest {

    private UserManagerImpl userMgr;
    private final String testUserId = "testUser";

    @Before
    public void before() throws Exception {
        super.before();

        userMgr = createUserManager(root, getPartialValueFactory());
    }

    @After
    public void after() throws Exception {
        try {
            root.refresh();
        } finally {
            super.after();
        }
    }

    private UserManagerImpl createUserManager(@NotNull Root root, @NotNull PartialValueFactory pvf) {
        return new UserManagerImpl(root, pvf, getSecurityProvider(), UserMonitor.NOOP, mock(DynamicMembershipService.class));
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
    public void testGetAuthorizableByPath() throws Exception {
        Authorizable authorizable = getTestUser();
        Authorizable byPath = userMgr.getAuthorizableByPath(authorizable.getPath());

        assertEquals(authorizable.getPath(), byPath.getPath());
    }

    @Test(expected = RepositoryException.class)
    public void testAuthorizableByUnresolvablePath() throws Exception {
        NamePathMapper mapper = new NamePathMapperImpl(new LocalNameMapper(root, Map.of("a","internal")));
        UserManagerImpl um = createUserManager(root, new PartialValueFactory(mapper));
        um.getAuthorizableByPath(getTestUser().getPath());
    }

    @Test
    public void testGetAuthorizableFromTree() throws Exception {
        assertNotNull(userMgr.getAuthorizable(root.getTree(getTestUser().getPath())));
    }

    @Test
    public void testGetAuthorizableFromNullTree() throws Exception {
        assertNull(userMgr.getAuthorizable((Tree) null));
    }

    @Test
    public void testGetAuthorizableFromNonExistingTree() throws Exception {
        Tree t = when(mock(Tree.class).exists()).thenReturn(false).getMock();
        assertNull(userMgr.getAuthorizable(t));
    }

    @Test
    public void testGtAuthorizableFromInvalidTree() throws Exception {
        assertNull(userMgr.getAuthorizable(root.getTree(PathUtils.ROOT_PATH)));
    }

    @Test
    public void testSetPassword() throws Exception {
        User user = userMgr.createUser(testUserId, "pw");
        List<String> pwds = new ArrayList<String>();
        pwds.add("pw");
        pwds.add("");
        pwds.add("{sha1}pw");

        Tree userTree = root.getTree(user.getPath());
        for (String pw : pwds) {
            userMgr.setPassword(userTree, testUserId, pw, false);
            String pwHash = userTree.getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
            assertNotNull(pwHash);
            assertTrue(PasswordUtil.isSame(pwHash, pw));
        }

        for (String pw : pwds) {
            userMgr.setPassword(userTree, testUserId, pw, true);
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
    public void testGetPasswordHash() throws Exception {
        User user = userMgr.createUser(testUserId, null);
        Tree userTree = root.getTree(user.getPath());
        assertNull(userTree.getProperty(UserConstants.REP_PASSWORD));
    }

    @Test
    public void testIsAutoSave() {
        assertFalse(userMgr.isAutoSave());
    }

    @Test(expected = UnsupportedRepositoryOperationException.class)
    public void testAutoSave() throws Exception {
        userMgr.autoSave(true);
    }

    @Test
    public void testEnforceAuthorizableFolderHierarchy() throws Exception {
        User user = getTestUser();
        Tree userNode = root.getTree(user.getPath());

        Tree folder = TreeUtil.addChild(userNode, "folder", UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
        String path = folder.getPath();

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

        Tree someContent = TreeUtil.addChild(userNode, "mystuff", JcrConstants.NT_UNSTRUCTURED);
        path = someContent.getPath();
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
            folder = TreeUtil.addChild(someContent,"folder", UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
            root.commit(); // this time save node structure
            try {
                Principal p = new PrincipalImpl("test4");
                userMgr.createUser(p.getName(), p.getName(), p, folder.getPath());
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
        try {
            final List<Exception> exceptions = new ArrayList<Exception>();
            List<Thread> workers = new ArrayList<Thread>();
            for (int i = 0; i < 10; i++) {
                final String userId = "foo-user-" + i;
                workers.add(new Thread(new Runnable() {
                    public void run() {
                        try {
                            ContentSession admin = login(getAdminCredentials());
                            Root root = admin.getLatestRoot();
                            UserManager userManager = createUserManager(root, getPartialValueFactory());
                            userManager.createUser(userId, "pass", new PrincipalImpl(userId), "relPath");
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
        } finally {
            root.refresh();
            Tree t = root.getTree(UserConstants.DEFAULT_USER_PATH + "/relPath");
            if (t.exists()) {
                t.remove();
                root.commit();
            }
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
        User user = getUserManager(root).createUser(newUserId, newUserId);

        Assert.assertFalse(root.getTree(user.getPath()).hasChild(UserConstants.REP_PWD));
        Assert.assertFalse(user.hasProperty(UserConstants.REP_PWD + "/" + UserConstants.REP_PASSWORD_LAST_MODIFIED));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateUserWithEmptyId() throws RepositoryException {
        userMgr.createUser("", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateUserWithNullId() throws RepositoryException {
        userMgr.createUser(null, null, new PrincipalImpl("userPrincipalName"), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSystemUserWithEmptyId() throws RepositoryException {
        userMgr.createSystemUser("", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateSystemUserWithNullId() throws RepositoryException {
        userMgr.createSystemUser(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateGroupWithEmptyId() throws RepositoryException {
        userMgr.createGroup("", new PrincipalImpl("groupPrincipalName"), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateGroupWithNullId() throws RepositoryException {
        userMgr.createGroup(null, new PrincipalImpl("groupPrincipalName"), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateUserWithEmptyPrincipalName() throws Exception {
        userMgr.createUser("another", null, () -> "", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateGroupWithNullPrincipal() throws Exception {
        userMgr.createGroup("another", null, null);
    }

    @Test(expected = AuthorizableExistsException.class)
    public void testCreateUserWithExistingPrincipal() throws Exception {
        User u = getTestUser();
        userMgr.createUser("another", null, u.getPrincipal(), null);
    }

    @Test(expected = AuthorizableExistsException.class)
    public void testCreateGroupWithExistingPrincipal() throws Exception {
        User u = getTestUser();
        userMgr.createGroup(u.getPrincipal());
    }

    @Test
    public void testGetMonitor() {
        assertSame(UserMonitor.NOOP, userMgr.getMonitor());

        // initialize monitor as done in 'SecurityProviderRegistration'
        UserConfiguration uc = getConfig(UserConfiguration.class);
        uc.getMonitors(StatisticsProvider.NOOP);
        UserManager umgr = uc.getUserManager(root, getNamePathMapper());
        assertTrue(umgr instanceof UserManagerImpl);
        assertTrue(((UserManagerImpl) umgr).getMonitor() instanceof UserMonitorImpl);
    }
}