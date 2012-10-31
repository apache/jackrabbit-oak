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
package org.apache.jackrabbit.oak.security.authentication.user;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * UserAuthenticationTest...
 */
public class UserAuthenticationTest extends AbstractSecurityTest {

    private Root root;

    private String userId;
    private UserManager userManager;

    private UserAuthentication authentication;

    @Before
    public void before() throws Exception {
        super.before();

        root = admin.getLatestRoot();

        userId = "testUser";
        userManager = getSecurityProvider().getUserConfiguration().getUserManager(root, NamePathMapper.DEFAULT);

        userManager.createUser(userId, "pw");
        root.commit();

        authentication = new UserAuthentication(userId, userManager);
    }

    @After
    public void after() throws Exception {
        try {
            Authorizable a = userManager.getAuthorizable(userId);
            if (a != null) {
                a.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testAuthenticateWithoutUserManager() throws Exception {
        UserAuthentication authentication = new UserAuthentication(userId, null);
        assertFalse(authentication.authenticate(new SimpleCredentials(userId, "pw".toCharArray())));
    }

    @Test
    public void testAuthenticateWithoutUserId() throws Exception {
        UserAuthentication authentication = new UserAuthentication(null, userManager);
        assertFalse(authentication.authenticate(new SimpleCredentials(userId, "pw".toCharArray())));
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
            }
        }
    }

    @Test
    public void testAuthenticateSimpleCredentials() throws Exception {
       assertTrue(authentication.authenticate(new SimpleCredentials(userId, "pw".toCharArray())));
    }

    @Test
    public void testAuthenticateInvalidImpersonationCredentials() throws Exception {
       List<Credentials> invalid = new ArrayList<Credentials>();
        invalid.add(new ImpersonationCredentials(new GuestCredentials(), admin.getAuthInfo()));
        invalid.add(new ImpersonationCredentials(new SimpleCredentials(admin.getAuthInfo().getUserID(), new char[0]), new TestAuthInfo()));
        invalid.add(new ImpersonationCredentials(new SimpleCredentials("unknown", new char[0]), admin.getAuthInfo()));
        invalid.add(new ImpersonationCredentials(new SimpleCredentials("unknown", new char[0]), new TestAuthInfo()));

        for (Credentials creds : invalid) {
            try {
                authentication.authenticate(creds);
                fail("LoginException expected");
            } catch (LoginException e) {
                // success
            }
        }
    }

    @Test
    public void testAuthenticateImpersonationCredentials() throws Exception {
        SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);
        assertTrue(authentication.authenticate(new ImpersonationCredentials(sc, admin.getAuthInfo())));
    }

    @Test
    public void testAuthenticateImpersonationCredentials2() throws Exception {
        SimpleCredentials sc = new SimpleCredentials(userId, new char[0]);
        assertTrue(authentication.authenticate(new ImpersonationCredentials(sc, new TestAuthInfo())));
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
            @Override
            public Set<Principal> getPrincipals() {
                return null;
            }
    }
}