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

import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.RepositoryCallback;
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LoginModuleImplTest extends AbstractSecurityTest {

    private static final String USER_ID = "test";
    private static final String USER_ID_CASED = "TeSt";
    private static final String USER_PW = "pw";
    private User user;

    @Override
    public void after() throws Exception {
        if (user != null) {
            user.remove();
            root.commit();
        }
    }

    @Override
    protected Configuration getConfiguration() {
        return ConfigurationUtil.getDefaultConfiguration(ConfigurationParameters.EMPTY);
    }

    private User createTestUser() throws RepositoryException, CommitFailedException {
        if (user == null) {
            UserManager userManager = getUserManager(root);
            user = userManager.createUser(USER_ID, USER_PW);
            root.commit();
        }
        return user;
    }

    @Test
    public void testNullLogin() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(null);
            fail("Null login should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testGuestLogin() throws Exception {
        try (ContentSession cs = login(new GuestCredentials())) {
            AuthInfo authInfo = cs.getAuthInfo();
            String anonymousID = UserUtil.getAnonymousId(getUserConfiguration().getParameters());
            assertEquals(anonymousID, authInfo.getUserID());
        }
    }

    @Test
    public void testAnonymousLogin() throws Exception {
        String anonymousID = UserUtil.getAnonymousId(getUserConfiguration().getParameters());

        UserManager userMgr = getUserManager(root);

        // verify initial user-content looks like expected
        Authorizable anonymous = userMgr.getAuthorizable(anonymousID);
        assertNotNull(anonymous);
        assertFalse(root.getTree(anonymous.getPath()).hasProperty(UserConstants.REP_PASSWORD));

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(anonymousID, new char[0]));
            fail("Login with anonymousID should fail since the initial setup doesn't provide a password.");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testUserLogin() throws Exception {
        ContentSession cs = null;
        try {
            createTestUser();

            cs = login(new SimpleCredentials(USER_ID, USER_PW.toCharArray()));
            AuthInfo authInfo = cs.getAuthInfo();
            assertEquals(USER_ID, authInfo.getUserID());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testAuthInfoContainsUserId() throws Exception {
        ContentSession cs = null;
        try {
            createTestUser();

            cs = login(new SimpleCredentials(USER_ID_CASED, USER_PW.toCharArray()));
            AuthInfo authInfo = cs.getAuthInfo();
            assertEquals(user.getID(), authInfo.getUserID());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testUserLoginIsCaseInsensitive() throws Exception {
        ContentSession cs = null;
        try {
            createTestUser();

            cs = login(new SimpleCredentials(USER_ID_CASED, USER_PW.toCharArray()));
            AuthInfo authInfo = cs.getAuthInfo();
            UserManager userMgr = getUserManager(root);
            Authorizable auth = userMgr.getAuthorizable(authInfo.getUserID());
            assertNotNull(auth);
            assertTrue(auth.getID().equalsIgnoreCase(USER_ID_CASED));
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testUserLoginIsCaseInsensitive2() throws Exception {
        ContentSession cs = null;
        try {
            createTestUser();
            cs = login(new SimpleCredentials(USER_ID_CASED, USER_PW.toCharArray()));
            AuthInfo authInfo = cs.getAuthInfo();
            assertEquals(user.getID(), authInfo.getUserID());
            assertTrue(USER_ID_CASED.equalsIgnoreCase(authInfo.getUserID()));
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testUnknownUserLogin() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials("unknown", "".toCharArray()));
            fail("Unknown user must not be able to login");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testSelfImpersonation() throws Exception {
        ContentSession cs = null;
        try {
            createTestUser();

            SimpleCredentials sc = new SimpleCredentials(USER_ID, USER_PW.toCharArray());
            cs = login(sc);

            AuthInfo authInfo = cs.getAuthInfo();
            assertEquals(USER_ID, authInfo.getUserID());

            cs.close();

            sc = new SimpleCredentials(USER_ID, new char[0]);
            ImpersonationCredentials ic = new ImpersonationCredentials(sc, authInfo);
            cs = login(ic);

            authInfo = cs.getAuthInfo();
            assertEquals(USER_ID, authInfo.getUserID());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testInvalidImpersonation() throws Exception {
        ContentSession cs = null;
        try {
            createTestUser();

            SimpleCredentials sc = new SimpleCredentials(USER_ID, USER_PW.toCharArray());
            cs = login(sc);

            AuthInfo authInfo = cs.getAuthInfo();
            assertEquals(USER_ID, authInfo.getUserID());

            cs.close();
            cs = null;

            ConfigurationParameters config = securityProvider.getConfiguration(UserConfiguration.class).getParameters();
            String adminId = UserUtil.getAdminId(config);
            sc = new SimpleCredentials(adminId, new char[0]);
            ImpersonationCredentials ic = new ImpersonationCredentials(sc, authInfo);

            try {
                cs = login(ic);
                fail("User 'test' should not be allowed to impersonate " + adminId);
            } catch (LoginException e) {
                // success
            }
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testLoginWithAttributes( ) throws Exception {
        ContentSession cs = null;
        try {
            createTestUser();

            SimpleCredentials sc = new SimpleCredentials(USER_ID, USER_PW.toCharArray());
            sc.setAttribute("attr", "value");

            cs = login(sc);

            AuthInfo authInfo = cs.getAuthInfo();
            assertTrue(Arrays.asList(authInfo.getAttributeNames()).contains("attr"));
            assertEquals("value", authInfo.getAttribute("attr"));

            cs.close();
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testImpersonationWithAttributes() throws Exception {
        ContentSession cs = null;
        try {
            createTestUser();

            SimpleCredentials sc = new SimpleCredentials(USER_ID, USER_PW.toCharArray());
            cs = login(sc);
            AuthInfo authInfo = cs.getAuthInfo();
            cs.close();
            cs = null;

            sc = new SimpleCredentials(USER_ID, new char[0]);
            sc.setAttribute("attr", "value");
            ImpersonationCredentials ic = new ImpersonationCredentials(sc, authInfo);
            cs = login(ic);

            authInfo = cs.getAuthInfo();
            assertTrue(Arrays.asList(authInfo.getAttributeNames()).contains("attr"));
            assertEquals("value", authInfo.getAttribute("attr"));
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testGetNullUserAuthentication() throws Exception {
        LoginModuleImpl loginModule = new LoginModuleImpl();
        CallbackHandler cbh = new TestCallbackHandler(Mockito.mock(UserAuthenticationFactory.class));
        loginModule.initialize(new Subject(), cbh, Maps.<String, Object>newHashMap(), Maps.<String, Object>newHashMap());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
    }

    @Test
    public void testCustomUserAuthentication() throws Exception {
        LoginModuleImpl loginModule = new LoginModuleImpl();

        UserAuthenticationFactory factory = new UserAuthenticationFactory() {
            @CheckForNull
            @Override
            public Authentication getAuthentication(@Nonnull UserConfiguration configuration, @Nonnull Root root, @Nullable String userId) {
                return new Authentication() {
                    @Override
                    public boolean authenticate(@Nullable Credentials credentials) throws LoginException {
                        return true;
                    }

                    @CheckForNull
                    @Override
                    public String getUserId() {
                        return null;
                    }

                    @CheckForNull
                    @Override
                    public Principal getUserPrincipal() {
                        return null;
                    }
                };
            }
        };

        CallbackHandler cbh = new TestCallbackHandler(factory);
        SimpleCredentials creds = new SimpleCredentials("loginId", new char[0]);
        Subject subject = new Subject(false, Sets.<Principal>newHashSet(), ImmutableSet.of(creds), Sets.newHashSet());

        loginModule.initialize(subject, cbh, Maps.<String, Object>newHashMap(), Maps.<String, Object>newHashMap());
        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        AuthInfo authInfo = subject.getPublicCredentials(AuthInfo.class).iterator().next();
        assertEquals("loginId", authInfo.getUserID());
    }


    private class TestCallbackHandler implements CallbackHandler {

        private final SecurityProvider sp;

        private TestCallbackHandler(@Nullable UserAuthenticationFactory authenticationFactory) {
            ConfigurationParameters params = ConfigurationParameters.of(
                    UserConfiguration.NAME,
                    ConfigurationParameters.of(
                            UserConstants.PARAM_USER_AUTHENTICATION_FACTORY, authenticationFactory));
            this.sp = new SecurityProviderBuilder().with(params).build();
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof RepositoryCallback) {
                    ((RepositoryCallback) callback).setSecurityProvider(sp);
                    ((RepositoryCallback) callback).setContentRepository(getContentRepository());
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }
    }

}
