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
import java.util.List;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CreateUserTest...
 */
public class CreateUserTest extends AbstractUserTest {

    private static Logger log = LoggerFactory.getLogger(CreateUserTest.class);

    private List<Authorizable> createdUsers = new ArrayList<Authorizable>();

    @After
    @Override
    protected void tearDown() throws Exception {
        superuser.refresh(false);
        // remove all created groups again
        for (Object createdUser : createdUsers) {
            Authorizable auth = (Authorizable) createdUser;
            try {
                auth.remove();
                superuser.save();
            } catch (RepositoryException e) {
                log.warn("Failed to remove User " + auth.getID() + " during tearDown.");
            }
        }
        super.tearDown();
    }

    private User createUser(String uid, String pw) throws RepositoryException, NotExecutableException {
        User u = userMgr.createUser(uid, pw);
        superuser.save();
        return u;
    }

    private User createUser(String uid, String pw, Principal p, String iPath) throws RepositoryException, NotExecutableException {
        User u = userMgr.createUser(uid, pw, p, iPath);
        superuser.save();
        return u;
    }

    @Test
    public void testCreateUser() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = p.getName();
        User user = createUser(uid, "pw");
        createdUsers.add(user);

        assertNotNull(user.getID());
        assertEquals(p.getName(), user.getPrincipal().getName());
    }

    // TODO: check again.
//    @Test
//    public void testCreateUserWithPath() throws RepositoryException, NotExecutableException {
//        Principal p = getTestPrincipal();
//        String uid = p.getName();
//        User user = createUser(uid, "pw", p, "/any/path/to/the/new/user");
//        createdUsers.add(user);
//
//        assertNotNull(user.getID());
//        assertEquals(p.getName(), user.getPrincipal().getName());
//    }

    @Test
    public void testCreateUserWithPath2() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = p.getName();
        User user = createUser(uid, "pw", p, "any/path");
        createdUsers.add(user);

        assertNotNull(user.getID());
        assertEquals(p.getName(), user.getPrincipal().getName());
    }

    @Test
    public void testCreateUserWithDifferentPrincipalName() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = getTestPrincipal().getName();
        User user = createUser(uid, "pw", p, "any/path");
        createdUsers.add(user);

        assertNotNull(user.getID());
        assertEquals(p.getName(), user.getPrincipal().getName());
    }

    @Test
    public void testCreateUserWithNullParamerters() throws RepositoryException {
        try {
            User user = createUser(null, null);
            createdUsers.add(user);

            fail("A User cannot be built from 'null' parameters");
        } catch (Exception e) {
            // ok
        }

        try {
            User user = createUser(null, null, null, null);
            createdUsers.add(user);

            fail("A User cannot be built from 'null' parameters");
        } catch (Exception e) {
            // ok
        }
    }

    @Test
    public void testCreateUserWithNullUserID() throws RepositoryException {
        try {
            User user = createUser(null, "anyPW");
            createdUsers.add(user);

            fail("A User cannot be built with 'null' userID");
        } catch (Exception e) {
            // ok
        }
    }

    @Test
    public void testCreateUserWithEmptyUserID() throws RepositoryException {
        try {
            User user = createUser("", "anyPW");
            createdUsers.add(user);

            fail("A User cannot be built with \"\" userID");
        } catch (Exception e) {
            // ok
        }
        try {
            User user = createUser("", "anyPW", getTestPrincipal(), null);
            createdUsers.add(user);

            fail("A User cannot be built with \"\" userID");
        } catch (Exception e) {
            // ok
        }
    }

    @Test
    public void testCreateUserWithEmptyPassword() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        User user = createUser(p.getName(), "");
        createdUsers.add(user);
    }

    @Test
    public void testCreateUserWithNullPrincipal() throws RepositoryException {
        try {
            Principal p = getTestPrincipal();
            String uid = p.getName();
            User user = createUser(uid, "pw", null, "/a/b/c");
            createdUsers.add(user);

            fail("A User cannot be built with 'null' Principal");
        } catch (Exception e) {
            // ok
        }
    }

    public void testCreateUserWithEmptyPrincipal() throws RepositoryException {
        try {
            Principal p = getTestPrincipal("");
            String uid = p.getName();
            User user = createUser(uid, "pw", p, "/a/b/c");
            createdUsers.add(user);

            fail("A User cannot be built with ''-named Principal");
        } catch (Exception e) {
            // ok
        }
        try {
            Principal p = getTestPrincipal(null);
            String uid = p.getName();
            User user = createUser(uid, "pw", p, "/a/b/c");
            createdUsers.add(user);

            fail("A User cannot be built with ''-named Principal");
        } catch (Exception e) {
            // ok
        }
    }

    public void testCreateTwiceWithSameUserID() throws RepositoryException, NotExecutableException {
        String uid = getTestPrincipal().getName();
        User user = createUser(uid, "pw");
        createdUsers.add(user);

        try {
            User user2 = createUser(uid, "anyPW");
            createdUsers.add(user2);

            fail("Creating 2 users with the same UserID should throw AuthorizableExistsException.");
        } catch (AuthorizableExistsException e) {
            // success.
        }
    }

    // TODO: RepositoryException is thrown instead of AuthorizableExistsException
    public void testCreateTwiceWithSamePrincipal() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = p.getName();
        User user = createUser(uid, "pw", p, "a/b/c");
        createdUsers.add(user);

        try {
            uid = getTestPrincipal().getName();
            User user2 = createUser(uid, "pw", p, null);
            createdUsers.add(user2);

            fail("Creating 2 users with the same Principal should throw AuthorizableExistsException.");
        } catch (RepositoryException e) {
            // success.
        }
    }

    public void testGetUserAfterCreation() throws RepositoryException, NotExecutableException {
        Principal p = getTestPrincipal();
        String uid = p.getName();

        User user = createUser(uid, "pw");
        createdUsers.add(user);

        assertNotNull(userMgr.getAuthorizable(user.getID()));
        assertNotNull(userMgr.getAuthorizable(p));
    }
}