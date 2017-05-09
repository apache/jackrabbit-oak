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
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AccountLockedException;
import javax.security.auth.login.AccountNotFoundException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * UserAuthenticationTest...
 */
public class UserAuthenticationTest extends AbstractSecurityTest {

    private String userId;
    private UserAuthentication authentication;

    @Before
    public void before() throws Exception {
        super.before();
        userId = getTestUser().getID();
        authentication = new UserAuthentication(getUserConfiguration(), root, userId);
    }

    @Test
    public void testAuthenticateWithoutUserId() throws Exception {
        authentication = new UserAuthentication(getUserConfiguration(), root, null);
        assertFalse(authentication.authenticate(new SimpleCredentials(userId, userId.toCharArray())));
    }

    @Test
    public void testAuthenticateInvalidCredentials() throws Exception {
        List<Credentials> invalid = new ArrayList<Credentials>();
        invalid.add(new TokenCredentials("token"));
        invalid.add(new Credentials() {});

        for (Credentials creds : invalid) {
            assertFalse(authentication.authenticate(creds));
        }
    }

    @Test
    public void testAuthenticateCannotResolveUser() throws Exception {
        SimpleCredentials sc = new SimpleCredentials("unknownUser", "pw".toCharArray());
        Authentication a = new UserAuthentication(getUserConfiguration(), root, sc.getUserID());

        assertFalse(a.authenticate(sc));
    }

    @Test
    public void testAuthenticateResolvesToGroup() throws Exception {
        Group g = getUserManager(root).createGroup("g1");
        SimpleCredentials sc = new SimpleCredentials(g.getID(), "pw".toCharArray());
        Authentication a = new UserAuthentication(getUserConfiguration(), root, sc.getUserID());

        try {
            a.authenticate(sc);
            fail("Authenticating Group should fail");
        } catch (LoginException e) {
            // success
            assertTrue(e instanceof AccountNotFoundException);
        } finally {
            g.remove();
            root.commit();
        }
    }

    @Test
    public void testAuthenticateResolvesToDisabledUser() throws Exception {
        User testUser = getTestUser();
        SimpleCredentials sc = new SimpleCredentials(testUser.getID(), testUser.getID().toCharArray());
        Authentication a = new UserAuthentication(getUserConfiguration(), root, sc.getUserID());

        try {
            getTestUser().disable("disabled");
            root.commit();

            a.authenticate(sc);
            fail("Authenticating disabled user should fail");
        } catch (LoginException e) {
            // success
            assertTrue(e instanceof AccountLockedException);
        } finally {
            getTestUser().disable(null);
            root.commit();
        }
    }

    @Test
    public void testAuthenticateInvalidSimpleCredentials() throws Exception {
        List<Credentials> invalid = new ArrayList<Credentials>();
        invalid.add(new SimpleCredentials(userId, "wrongPw".toCharArray()));
        invalid.add(new SimpleCredentials(userId, "".toCharArray()));
        invalid.add(new SimpleCredentials("unknownUser", "pw".toCharArray()));

        for (Credentials creds : invalid) {
            try {
                authentication.authenticate(creds);
                fail("LoginException expected");
            } catch (LoginException e) {
                // success
                assertTrue(e instanceof FailedLoginException);
            }
        }
    }

    @Test
    public void testAuthenticateIdMismatch() throws Exception {
        try {
            authentication.authenticate(new SimpleCredentials("unknownUser", "pw".toCharArray()));
            fail("LoginException expected");
        } catch (LoginException e) {
            // success
            assertTrue(e instanceof FailedLoginException);
        }
    }

    @Test
    public void testAuthenticateSimpleCredentials() throws Exception {
       assertTrue(authentication.authenticate(new SimpleCredentials(userId, userId.toCharArray())));
    }

    @Test
    public void testAuthenticateInvalidImpersonationCredentials() throws Exception {
       List<Credentials> invalid = new ArrayList<Credentials>();
        invalid.add(new ImpersonationCredentials(new GuestCredentials(), adminSession.getAuthInfo()));
        invalid.add(new ImpersonationCredentials(new SimpleCredentials(adminSession.getAuthInfo().getUserID(), new char[0]), new TestAuthInfo()));
        invalid.add(new ImpersonationCredentials(new SimpleCredentials("unknown", new char[0]), adminSession.getAuthInfo()));
        invalid.add(new ImpersonationCredentials(new SimpleCredentials("unknown", new char[0]), new TestAuthInfo()));

        for (Credentials creds : invalid) {
            try {
                authentication.authenticate(creds);
                fail("LoginException expected");
            } catch (LoginException e) {
                // success
                assertTrue(e instanceof FailedLoginException);
            }
        }
    }

    @Test
    public void testAuthenticateImpersonationCredentials() throws Exception {
        SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);
        assertTrue(authentication.authenticate(new ImpersonationCredentials(sc, adminSession.getAuthInfo())));
    }

    @Test
    public void testAuthenticateImpersonationCredentials2() throws Exception {
        SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);
        assertTrue(authentication.authenticate(new ImpersonationCredentials(sc, new TestAuthInfo())));
    }

    @Test(expected = IllegalStateException.class)
    public void testGetUserIdBeforeLogin() {
       authentication.getUserId();
    }

    @Test
    public void testGetUserId() throws LoginException {
        authentication.authenticate(new SimpleCredentials(userId, userId.toCharArray()));
        assertEquals(userId, authentication.getUserId());
    }

    @Test
    public void testGetUserIdCasedLoginId() throws LoginException {
        String loginId = userId.toLowerCase();

        UserAuthentication auth = new UserAuthentication(getUserConfiguration(), root, loginId);
        assertTrue(auth.authenticate(new SimpleCredentials(loginId, userId.toCharArray())));

        assertNotEquals(loginId, auth.getUserId());
        assertEquals(userId, auth.getUserId());
    }

    @Test(expected = IllegalStateException.class)
    public void testGetUserPrincipalBeforeLogin() {
        authentication.getUserPrincipal();
    }

    @Test
    public void testGetUserPrincipal() throws Exception {
        authentication.authenticate(new SimpleCredentials(userId, userId.toCharArray()));
        assertEquals(getTestUser().getPrincipal(), authentication.getUserPrincipal());
    }

    //--------------------------------------------------------------------------

    private final class TestAuthInfo implements AuthInfo {

        @Override
            public String getUserID() {
                return userId;
            }
            @Nonnull
            @Override
            public String[] getAttributeNames() {
                return new String[0];
            }
            @Override
            public Object getAttribute(String attributeName) {
                return null;
            }
            @Nonnull
            @Override
            public Set<Principal> getPrincipals() {
                return null;
            }
    }
}