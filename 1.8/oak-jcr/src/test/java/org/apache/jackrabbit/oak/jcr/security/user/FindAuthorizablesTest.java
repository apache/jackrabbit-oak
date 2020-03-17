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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.util.Text;
import org.junit.Test;

/**
 * Tests for the query API exposed by {@link UserManager}.
 */
public class FindAuthorizablesTest extends AbstractUserTest {

    @Test
    public void testFindAuthorizable() throws RepositoryException, NotExecutableException {
        Set<Principal> principals = new HashSet<Principal>();
        PrincipalManager pMgr = ((JackrabbitSession) superuser).getPrincipalManager();
        Principal p = pMgr.getPrincipal(superuser.getUserID());
        if (p != null) {
            principals.add(p);
            PrincipalIterator principalIterator = pMgr.getGroupMembership(p);
            while (principalIterator.hasNext()) {
                principals.add(principalIterator.nextPrincipal());
            }
        }

        Authorizable auth;
        for (Principal principal : principals) {
            auth = userMgr.getAuthorizable(principal);
            if (auth != null) {
                if (!auth.isGroup() && auth.hasProperty(UserConstants.REP_PRINCIPAL_NAME)) {
                    String val = auth.getProperty(UserConstants.REP_PRINCIPAL_NAME)[0].getString();
                    Iterator<Authorizable> users = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, val);

                    // the result must contain 1 authorizable
                    assertTrue(users.hasNext());
                    Authorizable first = users.next();
                    assertEquals(first.getID(), val);

                    // since id is unique -> there should be no more users in
                    // the iterator left
                    assertFalse(users.hasNext());
                }
            }
        }
    }

    @Test
    public void testFindAuthorizableByAddedProperty() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        Authorizable auth = null;

        try {
            auth = userMgr.createGroup(p);
            auth.setProperty("E-Mail", new Value[]{superuser.getValueFactory().createValue("anyVal")});
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
    public void testFindUserInAllUsers() throws RepositoryException, NotExecutableException {
        User u = null;
        try {
            Principal p = getTestPrincipal();
            String uid = createUserId();
            u = userMgr.createUser(uid, "pw", p, null);
            superuser.save();

            boolean found = false;
            Iterator<Authorizable> it = userMgr.findAuthorizables("./" + UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_USER);
            while (it.hasNext() && !found) {
                User nu = (User) it.next();
                found = nu.getID().equals(uid);
            }
            assertTrue("Searching for 'null' must find the created user.", found);
        } finally {
            if (u != null) {
                u.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testFindUserInAllUsers2() throws RepositoryException, NotExecutableException {
        User u = null;
        try {
            Principal p = getTestPrincipal();
            String uid = createUserId();
            u = userMgr.createUser(uid, "pw", p, null);
            superuser.save();

            boolean found = false;
            Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_USER);
            while (it.hasNext() && !found) {
                User nu = (User) it.next();
                found = nu.getID().equals(uid);
            }
            assertTrue("Searching for 'null' must find the created user.", found);
        } finally {
            if (u != null) {
                u.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testFindUserInAllGroups() throws RepositoryException, NotExecutableException {
        User u = null;
        try {
            Principal p = getTestPrincipal();
            String uid = createUserId();
            u = userMgr.createUser(uid, "pw", p, null);
            superuser.save();

            Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_GROUP);
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
    public void testFindUserByPrincipalName() throws RepositoryException, NotExecutableException {
        User u = null;
        try {
            Principal p = getTestPrincipal();
            String uid = createUserId();

            u = userMgr.createUser(uid, "pw", p, null);
            superuser.save();

            boolean found = false;

            Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, p.getName(), UserManager.SEARCH_TYPE_USER);
            while (it.hasNext() && !found) {
                User nu = (User) it.next();
                found = nu.getPrincipal().getName().equals(p.getName());
            }
            assertTrue("Searching for principal-name must find the created user.", found);

            // but search groups should not find anything
            it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, p.getName(), UserManager.SEARCH_TYPE_GROUP);
            assertFalse(it.hasNext());

        } finally {
            if (u != null) {
                u.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testFindUserWithGroupType() throws RepositoryException, NotExecutableException {
        User u = null;
        try {
            Principal p = getTestPrincipal();
            String uid = createUserId();

            u = userMgr.createUser(uid, "pw", p, null);
            superuser.save();

            // but search groups should not find anything
            Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, p.getName(), UserManager.SEARCH_TYPE_GROUP);
            assertFalse("Searching for Groups should not find the user", it.hasNext());

        } finally {
            if (u != null) {
                u.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testFindGroupInAllGroups() throws RepositoryException, NotExecutableException {
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
            assertTrue("Searching for 'null' must find the created group.", found);
        } finally {
            if (gr != null) {
                gr.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testFindGroupByPrinicpalName() throws RepositoryException, NotExecutableException {
        Group gr = null;
        try {
            Principal p = getTestPrincipal();
            gr = userMgr.createGroup(p);
            superuser.save();

            boolean found = false;
            Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, p.getName(), UserManager.SEARCH_TYPE_GROUP);
            assertTrue(it.hasNext());
            Group ng = (Group) it.next();
            assertEquals("Searching for principal-name must find the created group.", p.getName(), ng.getPrincipal().getName());
            assertFalse("Only a single group must be found for a given principal name.", it.hasNext());
        } finally {
            if (gr != null) {
                gr.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testFindGroupWithUserType() throws RepositoryException, NotExecutableException {
        Group gr = null;
        try {
            Principal p = getTestPrincipal();
            gr = userMgr.createGroup(p);
            superuser.save();

            Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, p.getName(), UserManager.SEARCH_TYPE_USER);
            assertFalse(it.hasNext());
        } finally {
            if (gr != null) {
                gr.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testFindGroupInAllUsers() throws RepositoryException, NotExecutableException {
        Group gr = null;
        try {
            Principal p = getTestPrincipal();
            gr = userMgr.createGroup(p);
            superuser.save();

            boolean found = false;
            Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_USER);
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
    public void testFindAllUsersDoesNotContainGroup() throws RepositoryException {
        Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_USER);
        while (it.hasNext()) {
            assertFalse(it.next().isGroup());
        }
    }

    @Test
    public void testFindAllGroupsDoesNotContainUser() throws RepositoryException {
        Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_GROUP);
        while (it.hasNext()) {
            assertTrue(it.next().isGroup());
        }
    }

    @Test
    public void testFindUserWithSpecialCharIdByPrincipalName() throws RepositoryException {
        List<String> ids = Lists.newArrayList("'", "]", "']", Text.escapeIllegalJcrChars("']"), Text.escape("']"));
        for (String id : ids) {
            User user = null;
            try {
                user = userMgr.createUser(id, "pw");
                superuser.save();

                boolean found = false;
                Iterator<Authorizable> it = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, id, UserManager.SEARCH_TYPE_USER);
                while (it.hasNext() && !found) {
                    Authorizable a = it.next();
                    found = id.equals(a.getID());
                }
                assertTrue(found);
            } finally {
                if (user != null) {
                    user.remove();
                    superuser.save();
                }
            }
        }
    }
}