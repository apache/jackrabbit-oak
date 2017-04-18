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

import java.util.Iterator;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for system {@code User} creation.
 *
 * @since Oak 1.1
 */
public class SystemUserTest extends AbstractUserTest {

    private static Logger log = LoggerFactory.getLogger(SystemUserTest.class);

    private String uid;
    private User user;

    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        uid = getTestPrincipal().getName();
    }

    @After
    @Override
    protected void tearDown() throws Exception {
        superuser.refresh(false);
        // remove all created groups again
        if (user != null) {
            try {
                user.remove();
                superuser.save();
            } catch (RepositoryException e) {
                log.warn("Failed to remove User " + user.getID() + " during tearDown.");
            }
        }
        super.tearDown();
    }

    private User createUser(String uid) throws RepositoryException {
        return createUser(uid, null);
    }

    private User createUser(String uid, String intermediatePath) throws RepositoryException {
        User u = userMgr.createSystemUser(uid, intermediatePath);
        superuser.save();
        return u;
    }

    @Test
    public void testCreateUser() throws RepositoryException, NotExecutableException {
        user = createUser(uid);
        assertNotNull(user.getID());

        assertTrue(user.isSystemUser());
        assertFalse(user.isAdmin());
        assertFalse(user.isGroup());
    }

    @Test
    public void testCreateUserWithNullUserID() throws RepositoryException {
        try {
            user = createUser(null);
            fail("A User cannot be built with 'null' userID");
        } catch (Exception e) {
            // ok
        }
    }

    @Test
    public void testCreateUserWithEmptyUserID() throws RepositoryException {
        try {
            user = createUser("");
            fail("A User cannot be built with \"\" userID");
        } catch (Exception e) {
            // ok
        }
    }

    @Test
    public void testCreateTwiceWithSameUserID() throws RepositoryException, NotExecutableException {
        user = createUser(uid);
        try {
            User user2 = createUser(uid);
            fail("Creating 2 users with the same UserID should throw AuthorizableExistsException.");
        } catch (AuthorizableExistsException e) {
            // success.
        }
    }

    @Test
    public void testGetUserByID() throws RepositoryException, NotExecutableException {
        user = createUser(uid);

        Authorizable authorizable = userMgr.getAuthorizable(user.getID());
        assertNotNull(authorizable);
        assertFalse(authorizable.isGroup());
        assertFalse(((User) authorizable).isAdmin());
        assertTrue(((User) authorizable).isSystemUser());
    }

    @Test
    public void testGetUserByPrincipal() throws Exception {
        user = createUser(uid);

        Authorizable authorizable = userMgr.getAuthorizable(user.getPrincipal());
        assertNotNull(authorizable);
        assertFalse(authorizable.isGroup());
        assertFalse(((User) authorizable).isAdmin());
        assertTrue(((User) authorizable).isSystemUser());
    }

    public void testGetUserByPath() throws Exception {
        user = createUser(uid);

        Authorizable authorizable = userMgr.getAuthorizableByPath(user.getPath());
        assertNotNull(authorizable);
        assertFalse(authorizable.isGroup());
        assertFalse(((User) authorizable).isAdmin());
        assertTrue(((User) authorizable).isSystemUser());
        assertEquals(user.getPath(), authorizable.getPath());
    }

    @Test
    public void testFindAuthorizable() throws Exception {
        user = createUser(uid);

        Iterator<Authorizable> iterator = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, user.getPrincipal().getName());
        assertTrue(iterator.hasNext());

        Authorizable authorizable = iterator.next();
        assertNotNull(authorizable);
        assertFalse(authorizable.isGroup());
        assertTrue(((User) authorizable).isSystemUser());

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testChangePassword() throws Exception {
        user = createUser(uid);
        try {
            user.changePassword("pw");
            superuser.save();
            fail();
        } catch (UnsupportedRepositoryOperationException e) {
            // success
        }
    }

    @Test
    public void testChangePassword2() throws Exception {
        user = createUser(uid);
        try {
            user.changePassword("old", "pw");
            superuser.save();
            fail();
        } catch (UnsupportedRepositoryOperationException e) {
            // success
        }
    }

    @Test
    public void testDisable() throws Exception {
        user = createUser(uid);
        user.disable("gone");
        superuser.save();

        assertTrue(user.isDisabled());
        assertEquals("gone", user.getDisabledReason());

        user.disable(null);
        assertFalse(user.isDisabled());
    }
}