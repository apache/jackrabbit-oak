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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jcr.Credentials;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

/**
 * Tests for the {@link UserManager} implementation.
 */
public class UserManagerTest extends AbstractUserTest {

    @Test
    public void testGetNewAuthorizable() throws RepositoryException, NotExecutableException {
        String uid = createUserId();
        User user = null;

        try {
            user = userMgr.createUser(uid, uid);
            assertEquals(uid, user.getID());
            assertNotNull(userMgr.getAuthorizable(uid));
            assertEquals(user, userMgr.getAuthorizable(uid));

            assertNotNull(getNode(user, superuser));
        } finally {
            if (user != null) {
                user.remove();
            }
        }
    }

    @Test
    public void testGetAuthorizable() throws RepositoryException, NotExecutableException {
        String uid = createUserId();
        User user = null;

        try {
            user = userMgr.createUser(uid, uid);
            superuser.save();

            assertEquals(uid, user.getID());
            assertNotNull(userMgr.getAuthorizable(uid));
            assertEquals(user, userMgr.getAuthorizable(uid));

            assertNotNull(getNode(user, superuser));
        } finally {
            if (user != null) {
                user.remove();
            }
        }
    }

    @Test
    public void testGetAuthorizableByPath() throws RepositoryException, NotExecutableException {
        String uid = superuser.getUserID();
        Authorizable a = userMgr.getAuthorizable(uid);
        if (a == null) {
            throw new NotExecutableException();
        }
        try {
            String path = a.getPath();
            Authorizable a2 = userMgr.getAuthorizableByPath(path);
            assertNotNull(a2);
            assertEquals(a.getID(), a2.getID());
        } catch (UnsupportedRepositoryOperationException e) {
            throw new NotExecutableException();
        }
    }

    @Test
    public void testUserIDFromSession() throws RepositoryException, NotExecutableException {
        User u = null;
        Session uSession = null;
        try {
            String uid = createUserId();
            u = userMgr.createUser(uid, "pw");
            superuser.save();

            uSession = superuser.getRepository().login(new SimpleCredentials(uid, "pw".toCharArray()));
            assertEquals(u.getID(), uSession.getUserID());
        } finally {
            if (uSession != null) {
                uSession.logout();
            }
            if (u != null) {
                u.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateUserPrincipalNameEqualsUserID() throws RepositoryException, NotExecutableException {
        User u = null;
        try {
            String uid = createUserId();
            u = userMgr.createUser(uid, "pw");
            superuser.save();

            String msg = "User.getID() must return the userID pass to createUser.";
            assertEquals(msg, uid, u.getID());

            msg = "Principal name must be the same as userID.";
            assertEquals(msg, uid, u.getPrincipal().getName());
        } finally {
            if (u != null) {
                u.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateUserIdDifferentFromPrincipalName() throws RepositoryException, NotExecutableException {
        User u = null;
        Session uSession = null;
        try {
            Principal p = getTestPrincipal();
            String uid = createUserId();

            u = userMgr.createUser(uid, "pw", p, null);
            superuser.save();

            String msg = "Creating a User with principal-name distinct from Principal-name must succeed as long as both are unique.";
            assertEquals(msg, u.getID(), uid);
            assertEquals(msg, p.getName(), u.getPrincipal().getName());
            assertFalse(msg, u.getID().equals(u.getPrincipal().getName()));

            // make sure the userID exposed by a Session corresponding to that
            // user is equal to the users ID.
            uSession = superuser.getRepository().login(new SimpleCredentials(uid, "pw".toCharArray()));
            assertEquals(uid, uSession.getUserID());
        } finally {
            if (uSession != null) {
                uSession.logout();
            }
            if (u != null) {
                u.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateGroupWithInvalidIdOrPrincipal() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = p.getName();

        Principal emptyNamePrincipal = new PrincipalImpl("");

        Map<String, Principal> fail = new HashMap<String, Principal>();
        fail.put(uid, null);
        fail.put(uid, emptyNamePrincipal);
        fail.put(null, p);
        fail.put("", p);

        for (String id : fail.keySet()) {
            Group g = null;
            try {
                Principal princ = fail.get(id);
                g = userMgr.createGroup(id, princ, null);
                fail("Creating group with id '" + id + "' and principal '" + princ.getName() + "' should fail");
            } catch (IllegalArgumentException e) {
                // success
            } finally {
                if (g != null) {
                    g.remove();
                    superuser.save();
                }
            }
        }
    }

    @Test
    public void testCreateEveryoneUser() throws Exception {
        User u = null;
        try {
            u = userMgr.createUser(EveryonePrincipal.NAME, "pw");
            fail("everyone is a reserved group name");
        } catch (IllegalArgumentException e) {
            // success
        } finally {
            if (u != null) {
                u.remove();
            }
        }

    }

    /**
     * @since OAK 1.0
     */
    @Test
    public void testCreateUserWithoutPassword() throws Exception {
        try {
            User u = userMgr.createUser(createUserId(), null);
        } finally {
            superuser.refresh(false);
        }
    }

    @Test
    public void testCreateGroupWithId() throws RepositoryException, NotExecutableException {
        Group gr = null;
        try {
            String id = createGroupId();

            // assert group creation with exact ID
            gr = userMgr.createGroup(id);
            superuser.save();
            assertEquals("Expect group with exact ID", id, gr.getID());

        } finally {
            if (gr != null) {
                gr.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateGroupWithIdAndPrincipal() throws RepositoryException, NotExecutableException {
        Group gr = null;
        try {
            Principal p = getTestPrincipal();
            String uid = p.getName();

            // assert group creation with exact ID
            gr = userMgr.createGroup(uid, p, null);
            superuser.save();

            assertEquals("Expect group with exact ID", uid, gr.getID());
            assertEquals("Expected group with exact principal name", p.getName(), gr.getPrincipal().getName());

        } finally {
            if (gr != null) {
                gr.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateGroupIdDifferentFromPrincipalName() throws RepositoryException, NotExecutableException {
        Group g = null;
        try {
            Principal p = getTestPrincipal();

            g = userMgr.createGroup("testGroup", p, null);
            superuser.save();

            String msg = "Creating a Group with principal-name distinct from Principal-name must succeed as long as both are unique.";
            assertEquals(msg, g.getID(), "testGroup");
            assertEquals(msg, p.getName(), g.getPrincipal().getName());
            assertFalse(msg, g.getID().equals(g.getPrincipal().getName()));

        } finally {
            if (g != null) {
                g.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateGroupWithExistingPrincipal() throws RepositoryException, NotExecutableException {
        User u = null;
        try {
            Principal p = getTestPrincipal();
            String uid = p.getName();

            // create a user with the given ID
            u = userMgr.createUser(uid, "pw", p, null);
            superuser.save();

            // assert AuthorizableExistsException for principal that is already in use
            Group gr = null;
            try {
                gr = userMgr.createGroup(p);
                fail("Principal " + p.getName() + " is already in use -> must throw AuthorizableExistsException.");
            } catch (AuthorizableExistsException e) {
                // expected this
            } finally {
                if (gr != null) {
                    gr.remove();
                    superuser.save();
                }
            }

        } finally {
            if (u != null) {
                u.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateGroupWithExistingPrincipal2() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = createUserId();

        assertFalse(uid.equals(p.getName()));

        User u = null;
        try {
            // create a user with the given ID
            u = userMgr.createUser(uid, "pw", p, null);
            superuser.save();

            // assert AuthorizableExistsException for principal that is already in use
            Group gr = null;
            try {
                gr = userMgr.createGroup(p);
                fail("Principal " + p.getName() + " is already in use -> must throw AuthorizableExistsException.");
            } catch (AuthorizableExistsException e) {
                // expected this
            } finally {
                if (gr != null) {
                    gr.remove();
                    superuser.save();
                }
            }

        } finally {
            if (u != null) {
                u.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateGroupWithExistingPrincipal3() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = createUserId();

        assertFalse(uid.equals(p.getName()));

        User u = null;
        try {
            // create a user with the given ID
            u = userMgr.createUser(uid, "pw", p, null);
            superuser.save();

            // assert AuthorizableExistsException for principal that is already in use
            Group gr = null;
            try {
                gr = userMgr.createGroup(createGroupId(), p, null);
                fail("Principal " + p.getName() + " is already in use -> must throw AuthorizableExistsException.");
            } catch (AuthorizableExistsException e) {
                // expected this
            } finally {
                if (gr != null) {
                    gr.remove();
                    superuser.save();
                }
            }

        } finally {
            if (u != null) {
                u.remove();
                superuser.save();
            }
        }
    }

    /**
     * @since oak 1.0 : if collision is added within the same set of transient
     *        modifications it will only be detected upon save. in this case RepositoryException
     *        is thrown instead of AuthorizableExistsException as the violation is
     *        detected by the uniqueness constraint on the corresponding property index.
     */
    @Test
    public void testCreateGroupWithExistingPrincipal4() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = createUserId();

        assertFalse(uid.equals(p.getName()));

        User u = null;
        Group gr = null;
        try {
            // create a user with the given ID
            u = userMgr.createUser(uid, "pw", p, null);
            gr = userMgr.createGroup(createGroupId(), p, null);
            superuser.save();

            fail("Principal " + p.getName() + " is already in use -> must throw AuthorizableExistsException.");
        } catch (RepositoryException e) {
            // expected this
        } finally {
            if (gr != null) {
                gr.remove();
            }
            if (u != null) {
                u.remove();
            }
            if (superuser.hasPendingChanges()) {
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateGroupWithExistingUserID() throws RepositoryException, NotExecutableException {
        User u = null;
        try {
            String uid = createUserId();

            // create a user with the given ID
            u = userMgr.createUser(uid, "pw");
            superuser.save();

            // assert AuthorizableExistsException for id that is already in use
            Group gr = null;
            try {
                gr = userMgr.createGroup(uid);
                fail("ID " + uid + " is already in use -> must throw AuthorizableExistsException.");
            } catch (AuthorizableExistsException aee) {
                // expected this
            } finally {
                if (gr != null) {
                    gr.remove();
                    superuser.save();
                }
            }

        } finally {
            if (u != null) {
                u.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateGroupWithExistingGroupID() throws RepositoryException, NotExecutableException {
        Group g = null;
        try {
            String id = createGroupId();

            // create a user with the given ID
            g = userMgr.createGroup(id);
            superuser.save();

            // assert AuthorizableExistsException for id that is already in use
            Group gr = null;
            try {
                gr = userMgr.createGroup(id);
                fail("ID " + id + " is already in use -> must throw AuthorizableExistsException.");
            } catch (AuthorizableExistsException aee) {
                // expected this
            } finally {
                if (gr != null) {
                    gr.remove();
                    superuser.save();
                }
            }

        } finally {
            if (g != null) {
                g.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateGroupWithExistingGroupID2() throws RepositoryException, NotExecutableException {

        Group g = null;
        try {
            String id = createGroupId();

            // create a group with the given ID
            g = userMgr.createGroup(id);
            superuser.save();

            // assert AuthorizableExistsException for id that is already in use
            Group gr = null;
            try {
                gr = userMgr.createGroup(id, getTestPrincipal(), null);
                fail("ID " + id + " is already in use -> must throw AuthorizableExistsException.");
            } catch (AuthorizableExistsException aee) {
                // expected this
            } finally {
                if (gr != null) {
                    gr.remove();
                    superuser.save();
                }
            }

        } finally {
            if (g != null) {
                g.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testNewUserCanLogin() throws RepositoryException, NotExecutableException {
        String uid = createUserId();
        User u = null;
        Session s = null;
        try {
            u = userMgr.createUser(uid, "pw");
            superuser.save();

            Credentials creds = new SimpleCredentials(uid, "pw".toCharArray());
            s = superuser.getRepository().login(creds);
        } finally {
            if (u != null) {
                u.remove();
                superuser.save();
            }
            if (s != null) {
                s.logout();
            }
        }
    }

    @Test
    public void testUnknownUserLogin() throws RepositoryException {
        String uid = createUserId();
        assertNull(userMgr.getAuthorizable(uid));
        try {
            Session s = superuser.getRepository().login(new SimpleCredentials(uid, uid.toCharArray()));
            s.logout();

            fail("An unknown user should not be allowed to execute the login.");
        } catch (Exception e) {
            // ok.
        }
    }

    @Test
    public void testCleanup() throws RepositoryException, NotExecutableException {
        Session s = getHelper().getSuperuserSession();
        try {
            UserManager umgr = getUserManager(s);
            s.logout();

            // after logging out the session, the user manager must have been
            // released as well and it's underlying session must not be available
            // any more -> accessing users must fail.
            try {
                umgr.getAuthorizable("any userid");
                fail("After having logged out the original session, the user manager must not be live any more.");
            } catch (RepositoryException e) {
                // success
            }
        } finally {
            if (s.isLive()) {
                s.logout();
            }
        }
    }

    @Test
    public void testCleanupForAllWorkspaces() throws RepositoryException, NotExecutableException {
        String[] workspaceNames = superuser.getWorkspace().getAccessibleWorkspaceNames();

        for (String workspaceName1 : workspaceNames) {
            Session s = getHelper().getSuperuserSession(workspaceName1);
            try {
                UserManager umgr = getUserManager(s);
                s.logout();

                // after logging out the session, the user manager must have been
                // released as well and it's underlying session must not be available
                // any more -> accessing users must fail.
                try {
                    umgr.getAuthorizable("any userid");
                    fail("After having logged out the original session, the user manager must not be live any more.");
                } catch (RepositoryException e) {
                    // success
                }
            } finally {
                if (s.isLive()) {
                    s.logout();
                }
            }
        }
    }

    @Test
    public void testCreateWithRelativePath() throws Exception {
        Principal p = getTestPrincipal();
        String uid = p.getName();

        List<String> invalid = new ArrayList<String>();
        invalid.add("../../path");
        invalid.add(UserConstants.DEFAULT_USER_PATH + "/../test");
        invalid.add("../../../home/users/test");

        for (String path : invalid) {
            try {
                User user = userMgr.createUser(uid, "pw", p, path);
                superuser.save();

                fail("intermediate path '" + path + "' outside of the user tree.");
            } catch (Exception e) {
                // success
            } finally {
                // revert transient changes
                superuser.refresh(false);
                // clean up
                Authorizable testUser = userMgr.getAuthorizable(uid);
                if (testUser != null) {
                    testUser.remove();
                    superuser.save();
                }
            }
        }
    }

    @Test
    public void testCreateWithAbsoluteIntermediatePath() throws Exception {
        Principal p = getTestPrincipal();
        String uid = p.getName();

        User test = null;
        try {
            test = userMgr.createUser(uid, "pw", p, UserConstants.DEFAULT_USER_PATH + "/test");
            superuser.save();
            assertTrue(test.getPath().startsWith(UserConstants.DEFAULT_USER_PATH + "/test"));
        } finally {
            if (test != null) {
                test.remove();
                superuser.save();
            }
        }
    }

    public void testAutoSave() throws RepositoryException, NotExecutableException {
        if (userMgr.isAutoSave()) {
            try {
                userMgr.autoSave(false);
            } catch (RepositoryException e) {
                throw new NotExecutableException();
            }
        }

        Principal p = getTestPrincipal();
        String uid = p.getName();
        User user = userMgr.createUser(uid, "pw");

        String gid = createGroupId();
        Group group = userMgr.createGroup(gid);
        superuser.refresh(false);

        // transient changes must be gone after the refresh-call.
        assertNull(userMgr.getAuthorizable(uid));
        assertNull(userMgr.getAuthorizable(p));
        assertNull(userMgr.getAuthorizable(gid));
    }
}
