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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Ignore;
import org.junit.Test;

public class UserManagerTest extends AbstractUserTest {

    private String getTestUserId(Principal p) throws RepositoryException {
        String hint = "UID" + p.getName();
        String userId = hint;
        int i = 0;
        while (userMgr.getAuthorizable(userId) != null) {
            userId = hint + i++;
        }
        return userId;
    }

    @Test
    public void testGetNewAuthorizable() throws RepositoryException, NotExecutableException {
        String uid = "testGetNewAuthorizable";
        User user = userMgr.createUser(uid, uid);

        assertEquals(uid, user.getID());
        assertNotNull(userMgr.getAuthorizable(uid));
        assertEquals(user,  userMgr.getAuthorizable(uid));

        assertNotNull(getNode(user, superuser));
    }

    public void testGetAuthorizable() throws RepositoryException, NotExecutableException {
        String uid = "testGetNewAuthorizable";
        User user = userMgr.createUser(uid, uid);
        superuser.save();

        assertEquals(uid, user.getID());
        assertNotNull(userMgr.getAuthorizable(uid));
        assertEquals(user,  userMgr.getAuthorizable(uid));

        assertNotNull(getNode(user, superuser));
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
    public void testPrincipalNameEqualsUserID() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        User u = null;
        try {
            u = userMgr.createUser(p.getName(), "pw");
            superuser.save();

            String msg = "User.getID() must return the userID pass to createUser.";
            assertEquals(msg, u.getID(), p.getName());
        } finally {
            if (u != null) {
                u.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testUserIDFromSession() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        User u = null;
        Session uSession = null;
        try {
            String uid = p.getName();
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
    public void testCreateUserIdDifferentFromPrincipalName() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = getTestUserId(p);

        User u = null;
        Session uSession = null;
        try {
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

        Principal emptyNamePrincipal = new Principal() {
            @Override
            public String getName() {
                 return "";
            }
        };
        
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

    @Test
    public void testCreateGroupWithId() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = p.getName();

        Group gr = null;
        try {

        	// assert group creation with exact ID
            gr = userMgr.createGroup(uid);
            superuser.save();
            assertEquals("Expect group with exact ID", uid, gr.getID());

        } finally {
            if (gr != null) {
                gr.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateGroupWithIdAndPrincipal() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = p.getName();

        Group gr = null;
        try {

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
        Principal p = getTestPrincipal();
        Group g = null;
        try {
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
        Principal p = getTestPrincipal();
        String uid = p.getName();

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
    public void testCreateGroupWithExistingPrincipal2() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = getTestUserId(p);

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
    public void testCreateGroupWithExistingPrincipal3() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = getTestUserId(p);

        assertFalse(uid.equals(p.getName()));

        User u = null;
        try {
        	// create a user with the given ID
            u = userMgr.createUser(uid, "pw", p, null);
            superuser.save();

            // assert AuthorizableExistsException for principal that is already in use
            Group gr = null;
            try {
            	gr = userMgr.createGroup(getTestPrincipal().getName(), p, null);
            	fail("Principal " + p.getName() + " is already in use -> must throw AuthorizableExistsException.");
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
    public void testCreateGroupWithExistingUserID() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = getTestUserId(p);

        User u = null;
        try {
        	// create a user with the given ID
            u = userMgr.createUser(uid, "pw", p, null);
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
        Principal p = getTestPrincipal();
        String uid = getTestUserId(p);

        Group g = null;
        try {
        	// create a user with the given ID
            g = userMgr.createGroup(uid);
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
            if (g != null) {
                g.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testCreateGroupWithExistingGroupID2() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = getTestUserId(p);

        Group g = null;
        try {
        	// create a user with the given ID
            g = userMgr.createGroup(uid, p, null);
            superuser.save();

            // assert AuthorizableExistsException for id that is already in use
            Group gr = null;
            try {
            	gr = userMgr.createGroup(uid, getTestPrincipal(), null);
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
            if (g != null) {
                g.remove();
                superuser.save();
            }
        }
    }

    @Ignore // TODO
    @Test
    public void testFindAuthorizableByAddedProperty() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        Authorizable auth = null;

        try {
            auth= userMgr.createGroup(p);
            auth.setProperty("E-Mail", new Value[] { superuser.getValueFactory().createValue("anyVal")});
            superuser.save();

            boolean found = false;
            Iterator<Authorizable> result = userMgr.findAuthorizables("E-Mail", "anyVal");
            while (result.hasNext()) {
                Authorizable a = result.next();
                if (a.getID().equals(auth.getID())) {
                    found = true;
                }
            }

            assertTrue(found);
        } finally {
            // remove the create group again.
            if (auth != null) {
                auth.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testFindAuthorizableByRelativePath() throws NotExecutableException, RepositoryException {
        Principal p = getTestPrincipal();
        Authorizable auth = null;

        try {
            auth= userMgr.createGroup(p);
            Value[] vs = new Value[] {
                    superuser.getValueFactory().createValue("v1"),
                    superuser.getValueFactory().createValue("v2")
            };

            String relPath = "relPath/" + propertyName1;
            String relPath2 = "another/" + propertyName1;
            String relPath3 = "relPath/relPath/" + propertyName1;
            auth.setProperty(relPath, vs);
            auth.setProperty(relPath2, vs);
            auth.setProperty(relPath3, superuser.getValueFactory().createValue("v3"));
            superuser.save();

            // relPath = "prop1", v = "v1" -> should find the target group
            Iterator<Authorizable> result = userMgr.findAuthorizables(propertyName1, "v1");
            assertTrue("expected result", result.hasNext());
            assertEquals(auth.getID(), result.next().getID());
            assertFalse("expected no more results", result.hasNext());

            // relPath = "prop1", v = "v1" -> should find the target group
            result = userMgr.findAuthorizables(propertyName1, "v3");
            assertTrue("expected result", result.hasNext());
            assertEquals(auth.getID(), result.next().getID());
            assertFalse("expected no more results", result.hasNext());

            // relPath = "relPath/prop1", v = "v1" -> should find the target group
            result = userMgr.findAuthorizables(relPath, "v1");
            assertTrue("expected result", result.hasNext());
            assertEquals(auth.getID(), result.next().getID());
            assertFalse("expected no more results", result.hasNext());

            // relPath : "./prop1", v = "v1" -> should not find the target group
            result = userMgr.findAuthorizables("./" + propertyName1, "v1");
            assertFalse("expected result", result.hasNext());

        } finally {
            // remove the create group again.
            if (auth != null) {
                auth.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testFindUser() throws RepositoryException, NotExecutableException {
        User u = null;
        try {
            Principal p = getTestPrincipal();
            String uid = "UID" + p.getName();
            u = userMgr.createUser(uid, "pw", p, null);
            superuser.save();

            boolean found = false;
            Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_USER);
            while (it.hasNext() && !found) {
                User nu = (User) it.next();
                found = nu.getID().equals(uid);
            }
            assertTrue("Searching for 'null' must find the created user.", found);

            it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, p.getName(), UserManager.SEARCH_TYPE_USER);
            found = false;
            while (it.hasNext() && !found) {
                User nu = (User) it.next();
                found = nu.getPrincipal().getName().equals(p.getName());
            }
            assertTrue("Searching for principal-name must find the created user.", found);

            // but search groups should not find anything
            it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, p.getName(), UserManager.SEARCH_TYPE_GROUP);
            assertFalse(it.hasNext());

            it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_GROUP);
            while (it.hasNext()) {
                if (it.next().getPrincipal().getName().equals(p.getName())) {
                    fail("Searching for Groups should never find a user");
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
    public void testFindGroup() throws RepositoryException, NotExecutableException {
        Group gr = null;
        try {
            Principal p = getTestPrincipal();
            gr = userMgr.createGroup(p);
            superuser.save();

            boolean found = false;
            Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_GROUP);
            while (it.hasNext() && !found) {
                Group ng = (Group) it.next();
                found = ng.getPrincipal().getName().equals(p.getName());
            }
            assertTrue("Searching for \"\" must find the created group.", found);

            it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, p.getName(), UserManager.SEARCH_TYPE_GROUP);
            assertTrue(it.hasNext());
            Group ng = (Group) it.next();
            assertEquals("Searching for principal-name must find the created group.", p.getName(), ng.getPrincipal().getName());
            assertFalse("Only a single group must be found for a given principal name.", it.hasNext());

            // but search users should not find anything
            it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, p.getName(), UserManager.SEARCH_TYPE_USER);
            assertFalse(it.hasNext());

            it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_USER);
            while (it.hasNext()) {
                if (it.next().getPrincipal().getName().equals(p.getName())) {
                    fail("Searching for Users should never find a group");
                }
            }
        } finally {
            if (gr != null) {
                gr.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testFindAllUsers() throws RepositoryException {
        Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_USER);
        while (it.hasNext()) {
            assertFalse(it.next().isGroup());
        }
    }

    @Test
    public void testFindAllGroups() throws RepositoryException {
        Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_GROUP);
        while (it.hasNext()) {
            assertTrue(it.next().isGroup());
        }
    }

    @Test
    public void testNewUserCanLogin() throws RepositoryException, NotExecutableException {
        String uid = getTestPrincipal().getName();
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
        String uid = getTestPrincipal().getName();
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
    public void testEnforceAuthorizableFolderHierarchy() throws RepositoryException, NotExecutableException {
        Authorizable auth = userMgr.getAuthorizable(user.getID());
        Node userNode = getNode(auth, superuser);

        Node folder = userNode.addNode("folder", UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
        String path = folder.getPath();
        try {
            // authNode - authFolder -> create User
            Authorizable a = null;
            try {
                Principal p = getTestPrincipal();
                a = userMgr.createUser(p.getName(), p.getName(), p, path);
                superuser.save();

                fail("Users may not be nested.");
            } catch (RepositoryException e) {
                // success
            } finally {
                if (a != null) {
                    a.remove();
                }
            }
        } finally {
            superuser.refresh(false);
            if (superuser.nodeExists(path)) {
                folder.remove();
                superuser.save();
            }
        }

        Node someContent = userNode.addNode("mystuff", "nt:unstructured");
        path = someContent.getPath();
        try {
            // authNode - anyNode -> create User
            Authorizable a = null;
            try {
                Principal p = getTestPrincipal();
                a = userMgr.createUser(p.getName(), p.getName(), p, someContent.getPath());
                superuser.save();

                fail("Users may not be nested.");
            } catch (RepositoryException e) {
                // success
            } finally {
                if (a != null) {
                    a.remove();
                    a = null;
                }
            }

            // authNode - anyNode - authFolder -> create User
            if (!superuser.nodeExists(path)) {
                someContent = userNode.addNode("mystuff", "nt:unstructured");
            }
            folder = someContent.addNode("folder", UserConstants.NT_REP_AUTHORIZABLE_FOLDER);
            superuser.save(); // this time save node structure
            try {
                Principal p = getTestPrincipal();
                a = userMgr.createUser(p.getName(), p.getName(), p, folder.getPath());
                superuser.save();

                fail("Users may not be nested.");
            } catch (RepositoryException e) {
                // success
            } finally {
                if (a != null) {
                    a.remove();
                }
            }
        } finally {
            if (superuser.nodeExists(path)) {
                someContent.remove();
                superuser.save();
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

                fail("intermediate path '"+ path +"' outside of the user tree.");
                user.remove();
                superuser.save();

            } catch (Exception e) {
                // success
                assertNull(userMgr.getAuthorizable(uid));
            } finally {
                superuser.refresh(false);
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
}
