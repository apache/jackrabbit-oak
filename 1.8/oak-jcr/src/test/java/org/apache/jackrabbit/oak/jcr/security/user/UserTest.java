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
import javax.jcr.Credentials;
import javax.jcr.LoginException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

/**
 * Tests for the {@link User} implementation.
 */
public class UserTest extends AbstractUserTest {

    @Test
    public void testIsUser() throws RepositoryException {
        Authorizable authorizable = userMgr.getAuthorizable(user.getID());
        assertTrue(authorizable instanceof User);
    }

    @Test
    public void testIsGroup() throws RepositoryException {
        assertFalse(user.isGroup());
    }

    @Test
    public void testGetId() throws NotExecutableException, RepositoryException {
        assertNotNull(user.getID());
        assertNotNull(userMgr.getAuthorizable(user.getID()).getID());
    }

    @Test
    public void testGetPrincipal() throws RepositoryException, NotExecutableException {
        assertNotNull(user.getPrincipal());
        assertNotNull(userMgr.getAuthorizable(user.getID()).getPrincipal());
    }

    @Test
    public void testGetPath() throws RepositoryException, NotExecutableException {
        assertNotNull(user.getPath());
        assertNotNull(userMgr.getAuthorizable(user.getID()).getPath());
        try {
            assertEquals(getNode(user, superuser).getPath(), user.getPath());
        } catch (UnsupportedRepositoryOperationException e) {
            // ok.
        }
    }

    @Test
    public void testIsAdmin() throws NotExecutableException, RepositoryException {
        assertFalse(user.isAdmin());
    }

    @Test
    public void testChangePasswordNull() throws RepositoryException, NotExecutableException {
        // invalid 'null' pw string
        try {
            user.changePassword(null);
            superuser.save();
            fail("invalid pw null");
        } catch (Exception e) {
            // success
        }
    }

    @Test
    public void testChangePassword() throws RepositoryException, NotExecutableException {
        try {
            String hash = getNode(user, superuser).getProperty(UserConstants.REP_PASSWORD).getString();

            user.changePassword("changed");
            superuser.save();

            assertFalse(hash.equals(getNode(user, superuser).getProperty(UserConstants.REP_PASSWORD).getString()));
        } catch (Exception e) {
            // success
        }
    }

    @Test
    public void testChangePasswordWithInvalidOldPassword() throws RepositoryException, NotExecutableException {
        try {
            user.changePassword("changed", "wrongOldPw");
            superuser.save();
            fail("old password didn't match -> changePassword(String,String) should fail.");
        } catch (RepositoryException e) {
            // success.
        }
    }

    @Test
    public void testChangePasswordWithOldPassword() throws RepositoryException, NotExecutableException {
        try {
            String hash = getNode(user, superuser).getProperty(UserConstants.REP_PASSWORD).getString();

            user.changePassword("changed", testPw);
            superuser.save();

            assertFalse(hash.equals(getNode(user, superuser).getProperty(UserConstants.REP_PASSWORD).getString()));
        } finally {
            user.changePassword(testPw);
            superuser.save();
        }
    }

    @Test
    public void testLoginAfterChangePassword() throws RepositoryException {
        user.changePassword("changed");
        superuser.save();

        // make sure the user can login with the new pw
        Session s = getHelper().getRepository().login(new SimpleCredentials(user.getID(), "changed".toCharArray()));
        s.logout();
    }

    @Test
    public void testLoginAfterChangePassword2() throws RepositoryException, NotExecutableException {
        try {

            user.changePassword("changed", testPw);
            superuser.save();

            // make sure the user can login with the new pw
            Session s = getHelper().getRepository().login(new SimpleCredentials(user.getID(), "changed".toCharArray()));
            s.logout();
        } finally {
            user.changePassword(testPw);
            superuser.save();
        }
    }

    @Test
    public void testLoginWithOldPassword() throws RepositoryException, NotExecutableException {
        try {
            user.changePassword("changed");
            superuser.save();

            Session s = getHelper().getRepository().login(new SimpleCredentials(user.getID(), testPw.toCharArray()));
            s.logout();
            fail("user pw has changed. login must fail.");
        } catch (LoginException e) {
            // success
        }
    }

    @Test
    public void testLoginWithOldPassword2() throws RepositoryException, NotExecutableException {
        try {
            user.changePassword("changed", testPw);
            superuser.save();

            Session s = getHelper().getRepository().login(new SimpleCredentials(user.getID(), testPw.toCharArray()));
            s.logout();
            fail("superuser pw has changed. login must fail.");
        } catch (LoginException e) {
            // success
        } finally {
            user.changePassword(testPw);
            superuser.save();
        }
    }

    @Test
    public void testEnabledByDefault() throws Exception {
        // by default a user isn't disabled
        assertFalse(user.isDisabled());
        assertNull(user.getDisabledReason());
    }

    @Test
    public void testDisable() throws Exception {
        String reason = "readonly user is disabled!";
        user.disable(reason);
        superuser.save();
        assertTrue(user.isDisabled());
        assertEquals(reason, user.getDisabledReason());
    }

    @Test
    public void testAccessDisabledUser() throws Exception {
        user.disable("readonly user is disabled!");
        superuser.save();

        // user must still be retrievable from user manager
        assertNotNull(getUserManager(superuser).getAuthorizable(user.getID()));
        // ... and from principal manager as well
        assertTrue(((JackrabbitSession) superuser).getPrincipalManager().hasPrincipal(user.getPrincipal().getName()));
    }

    @Test
    public void testAccessPrincipalOfDisabledUser()  throws Exception {
        user.disable("readonly user is disabled!");
        superuser.save();

        Principal principal = user.getPrincipal();
        assertTrue(((JackrabbitSession) superuser).getPrincipalManager().hasPrincipal(principal.getName()));
        assertEquals(principal, ((JackrabbitSession) superuser).getPrincipalManager().getPrincipal(principal.getName()));
    }

    @Test
    public void testEnableUser() throws Exception {
        user.disable("readonly user is disabled!");
        superuser.save();

        // enable user again
        user.disable(null);
        superuser.save();
        assertFalse(user.isDisabled());
        assertNull(user.getDisabledReason());

        // -> login must succeed again
        getHelper().getRepository().login(new SimpleCredentials(user.getID(), "pw".toCharArray())).logout();
    }

    @Test
    public void testLoginDisabledUser() throws Exception {
        user.disable("readonly user is disabled!");
        superuser.save();

        // -> login must fail
        try {
            Session ss = getHelper().getRepository().login(new SimpleCredentials(user.getID(), "pw".toCharArray()));
            ss.logout();
            fail("A disabled user must not be allowed to login any more");
        } catch (LoginException e) {
            // success
        }
    }

    @Test
    public void testImpersonateDisabledUser() throws Exception {
        user.disable("readonly user is disabled!");
        superuser.save();

        // -> impersonating this user must fail
        try {
            Session ss = superuser.impersonate(new SimpleCredentials(user.getID(), new char[0]));
            ss.logout();
            fail("A disabled user cannot be impersonated any more.");
        } catch (LoginException e) {
            // success
        }
    }

    @Test
    public void testLoginWithGetCredentials() throws RepositoryException, NotExecutableException {
        try {
            Credentials creds = user.getCredentials();
            Session s = getHelper().getRepository().login(creds);
            s.logout();
            fail("Login using credentials exposed on user must fail.");
        } catch (UnsupportedRepositoryOperationException e) {
            throw new NotExecutableException(e.getMessage());
        } catch (LoginException e) {
            // success
        }
    }
}