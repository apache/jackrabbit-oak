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

import javax.jcr.LoginException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Ignore;
import org.junit.Test;

/**
 * UserTest...
 */
@Ignore // FIXME: enable again
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
    public void testIsAdmin() throws NotExecutableException, RepositoryException {
        assertFalse(user.isAdmin());
    }

    @Test
    public void testAdminIsAdmin() throws NotExecutableException, RepositoryException {
        User admin = (User) userMgr.getAuthorizable(superuser.getUserID());
        if (admin == null) {
            throw new NotExecutableException("Admin user does not exist");
        }
        assertTrue(admin.isAdmin());
    }

    @Test
    public void testChangePassword() throws RepositoryException, NotExecutableException {
        String oldPw = getHelper().getProperty("javax.jcr.tck.superuser.pwd");
        if (oldPw == null) {
            // missing property
            throw new NotExecutableException();
        }

        User user = getTestUser(superuser);
        try {
            user.changePassword("pw");
            superuser.save();

            // make sure the user can login with the new pw
            Session s = getHelper().getRepository().login(new SimpleCredentials(user.getID(), "pw".toCharArray()));
            s.logout();
        } finally {
            user.changePassword(oldPw);
            superuser.save();
        }
    }

    @Test
    public void testChangePassword2() throws RepositoryException, NotExecutableException {
        String oldPw = getHelper().getProperty("javax.jcr.tck.superuser.pwd");
        if (oldPw == null) {
            // missing property
            throw new NotExecutableException();
        }

        User user = getTestUser(superuser);
        try {
            user.changePassword("pw");
            superuser.save();

            Session s = getHelper().getRepository().login(new SimpleCredentials(user.getID(), oldPw.toCharArray()));
            s.logout();
            fail("superuser pw has changed. login must fail.");
        } catch (LoginException e) {
            // success
        } finally {
            user.changePassword(oldPw);
            superuser.save();
        }
    }

    @Test
    public void testChangePasswordWithOldPassword() throws RepositoryException, NotExecutableException {
        String oldPw = getHelper().getProperty("javax.jcr.tck.superuser.pwd");
        if (oldPw == null) {
            // missing property
            throw new NotExecutableException();
        }

        User user = getTestUser(superuser);
        try {
            try {
                user.changePassword("pw", "wrongOldPw");
                superuser.save();
                fail("old password didn't match -> changePassword(String,String) should fail.");
            } catch (RepositoryException e) {
                // success.
            }

            user.changePassword("pw", oldPw);
            superuser.save();

            // make sure the user can login with the new pw
            Session s = getHelper().getRepository().login(new SimpleCredentials(user.getID(), "pw".toCharArray()));
            s.logout();
        } finally {
            user.changePassword(oldPw);
            superuser.save();
        }
    }

    @Test
    public void testChangePasswordWithOldPassword2() throws RepositoryException, NotExecutableException {
        String oldPw = getHelper().getProperty("javax.jcr.tck.superuser.pwd");
        if (oldPw == null) {
            // missing property
            throw new NotExecutableException();
        }

        User user = getTestUser(superuser);
        try {
            user.changePassword("pw", oldPw);
            superuser.save();

            Session s = getHelper().getRepository().login(new SimpleCredentials(user.getID(), oldPw.toCharArray()));
            s.logout();
            fail("superuser pw has changed. login must fail.");
        } catch (LoginException e) {
            // success
        } finally {
            user.changePassword(oldPw);
            superuser.save();
        }
    }

    @Test
    public void testDisable() throws Exception {
        // by default a user isn't disabled
        assertFalse(user.isDisabled());
        assertNull(user.getDisabledReason());

        // disable user
        String reason = "readonly user is disabled!";
        user.disable(reason);
        superuser.save();
        assertTrue(user.isDisabled());
        assertEquals(reason, user.getDisabledReason());

        // user must still be retrievable from user manager
        assertNotNull(getUserManager(superuser).getAuthorizable(user.getID()));
        // ... and from principal manager as well
        assertTrue(((JackrabbitSession) superuser).getPrincipalManager().hasPrincipal(user.getPrincipal().getName()));

        // -> login must fail
        try {
            Session ss = getHelper().getRepository().login(new SimpleCredentials(user.getID(), "pw".toCharArray()));
            ss.logout();
            fail("A disabled user must not be allowed to login any more");
        } catch (LoginException e) {
            // success
        }

        // -> impersonating this user must fail
        try {
            Session ss = superuser.impersonate(new SimpleCredentials(user.getID(), new char[0]));
            ss.logout();
            fail("A disabled user cannot be impersonated any more.");
        } catch (LoginException e) {
            // success
        }

        // enable user again
        user.disable(null);
        superuser.save();
        assertFalse(user.isDisabled());

        // -> login must succeed again
        getHelper().getRepository().login(new SimpleCredentials(user.getID(), "pw".toCharArray())).logout();
    }
}