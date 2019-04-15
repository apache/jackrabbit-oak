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
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.RepositoryCallback;
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

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
import java.security.Principal;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule.SHARED_KEY_PRE_AUTH_LOGIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    private void createTestUser() throws RepositoryException, CommitFailedException {
        if (user == null) {
            UserManager userManager = getUserManager(root);
            user = userManager.createUser(USER_ID, USER_PW);
            root.commit();
        }
    }

    @Test(expected = LoginException.class)
    public void testNullLogin() throws Exception {
        try (ContentSession cs = login(null)) {
            fail("Null login should fail");
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

    @Test(expected = LoginException.class)
    public void testAnonymousLogin() throws Exception {
        String anonymousID = UserUtil.getAnonymousId(getUserConfiguration().getParameters());

        UserManager userMgr = getUserManager(root);

        // verify initial user-content looks like expected
        Authorizable anonymous = userMgr.getAuthorizable(anonymousID);
        assertNotNull(anonymous);
        assertFalse(root.getTree(anonymous.getPath()).hasProperty(UserConstants.REP_PASSWORD));

        try (ContentSession cs = login(new SimpleCredentials(anonymousID, new char[0]))) {
            fail("Login with anonymousID should fail since the initial setup doesn't provide a password.");
        }
    }

    @Test
    public void testUserLogin() throws Exception {
        createTestUser();
        try (ContentSession cs = login(new SimpleCredentials(USER_ID, USER_PW.toCharArray()))) {
            AuthInfo authInfo = cs.getAuthInfo();
            assertEquals(USER_ID, authInfo.getUserID());
        }
    }

    @Test
    public void testAuthInfoContainsUserId() throws Exception {
        createTestUser();
        try (ContentSession cs = login(new SimpleCredentials(USER_ID_CASED, USER_PW.toCharArray()))) {
            AuthInfo authInfo = cs.getAuthInfo();
            assertEquals(user.getID(), authInfo.getUserID());
        }
    }

    @Test
    public void testUserLoginIsCaseInsensitive() throws Exception {
        createTestUser();
        try (ContentSession cs = login(new SimpleCredentials(USER_ID_CASED, USER_PW.toCharArray()))) {
            AuthInfo authInfo = cs.getAuthInfo();
            UserManager userMgr = getUserManager(root);
            Authorizable auth = userMgr.getAuthorizable(authInfo.getUserID());
            assertNotNull(auth);
            assertTrue(auth.getID().equalsIgnoreCase(USER_ID_CASED));
        }
    }

    @Test
    public void testUserLoginIsCaseInsensitive2() throws Exception {
        createTestUser();
        try (ContentSession cs = login(new SimpleCredentials(USER_ID_CASED, USER_PW.toCharArray()))) {
            AuthInfo authInfo = cs.getAuthInfo();
            assertEquals(user.getID(), authInfo.getUserID());
            assertTrue(USER_ID_CASED.equalsIgnoreCase(authInfo.getUserID()));
        }
    }

    @Test(expected = LoginException.class)
    public void testUnknownUserLogin() throws Exception {
        try (ContentSession cs = login(new SimpleCredentials("unknown", "".toCharArray()))) {
            fail("Unknown user must not be able to login");
        }
    }

    @Test
    public void testSelfImpersonation() throws Exception {
        createTestUser();
        AuthInfo authInfo;
        try (ContentSession cs = login(new SimpleCredentials(USER_ID, USER_PW.toCharArray()))) {
            authInfo = cs.getAuthInfo();
            assertEquals(USER_ID, authInfo.getUserID());
        }

        SimpleCredentials sc = new SimpleCredentials(USER_ID, new char[0]);
        ImpersonationCredentials ic = new ImpersonationCredentials(sc, authInfo);
        try (ContentSession cs = login(ic)) {
            authInfo = cs.getAuthInfo();
            assertEquals(USER_ID, authInfo.getUserID());
        }
    }

    @Test(expected = LoginException.class)
    public void testInvalidImpersonation() throws Exception {
        createTestUser();
        AuthInfo authInfo;
        try (ContentSession cs = login(new SimpleCredentials(USER_ID, USER_PW.toCharArray()))) {
            authInfo = cs.getAuthInfo();
            assertEquals(USER_ID, authInfo.getUserID());
        }

        ConfigurationParameters config = securityProvider.getConfiguration(UserConfiguration.class).getParameters();
        String adminId = UserUtil.getAdminId(config);
        SimpleCredentials sc = new SimpleCredentials(adminId, new char[0]);
        ImpersonationCredentials ic = new ImpersonationCredentials(sc, authInfo);
        // test-user should not be allowed to impersonate admin -> exception expected
        try (ContentSession cs = login(ic)) {
            fail("User 'test' should not be allowed to impersonate " + adminId);
        }
    }

    @Test
    public void testLoginWithAttributes( ) throws Exception {
        createTestUser();
        SimpleCredentials sc = new SimpleCredentials(USER_ID, USER_PW.toCharArray());
        sc.setAttribute("attr", "value");
        try (ContentSession cs = login(sc)){
            AuthInfo authInfo = cs.getAuthInfo();
            assertTrue(Arrays.asList(authInfo.getAttributeNames()).contains("attr"));
            assertEquals("value", authInfo.getAttribute("attr"));
        }
    }

    @Test
    public void testImpersonationWithAttributes() throws Exception {
        createTestUser();
        AuthInfo authInfo;
        try (ContentSession cs = login(new SimpleCredentials(USER_ID, USER_PW.toCharArray()))) {
            authInfo = cs.getAuthInfo();
        }

        SimpleCredentials sc = new SimpleCredentials(USER_ID, new char[0]);
        sc.setAttribute("attr", "value");
        ImpersonationCredentials ic = new ImpersonationCredentials(sc, authInfo);
        try (ContentSession cs = login(ic)) {
            authInfo = cs.getAuthInfo();
            assertTrue(Arrays.asList(authInfo.getAttributeNames()).contains("attr"));
            assertEquals("value", authInfo.getAttribute("attr"));
        }
    }

    @Test(expected = LoginException.class)
    public void testImpersonationWithUnsupportedBaseCredentials() throws Exception {
        Credentials baseCredentials = mock(Credentials.class);
        ImpersonationCredentials ic = new ImpersonationCredentials(baseCredentials, new AuthInfoImpl(USER_ID, null, null));
        try (ContentSession cs = login(ic)) {
            fail("Base credentials of ImpersonationCredentials can only be SimpleCredentials.");
        }
    }

    @Test
    public void LoginUnsupportedCredentials() throws Exception {
        Credentials unsupportedCredentials = mock(Credentials.class);

        CallbackHandler cbh = callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof RepositoryCallback) {
                    ((RepositoryCallback) callback).setSecurityProvider(getSecurityProvider());
                    ((RepositoryCallback) callback).setContentRepository(getContentRepository());
                } else if (callback instanceof CredentialsCallback) {
                    ((CredentialsCallback) callback).setCredentials(unsupportedCredentials);
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        };

        Subject subject = new Subject(false, ImmutableSet.of(), ImmutableSet.of(unsupportedCredentials), ImmutableSet.of());
        LoginModuleImpl lm = new LoginModuleImpl();
        lm.initialize(subject, cbh, Maps.newHashMap(), Maps.newHashMap());
        assertFalse(lm.login());
    }

    @Test
    public void testLoginPreAuthenticated() throws Exception {
        Authentication authentication = mock(Authentication.class);
        when(authentication.authenticate(any(Credentials.class))).thenReturn(true).getMock();
        when(authentication.getUserId()).thenReturn("uid"); // but getUserPrincipal returns null

        UserAuthenticationFactory uaf = when(mock(UserAuthenticationFactory.class).getAuthentication(any(UserConfiguration.class), any(Root.class), anyString())).thenReturn(authentication).getMock();
        Map<String, Object> sharedState = Maps.newHashMap();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin("uid"));

        Subject subject = new Subject();
        LoginModuleImpl lm = new LoginModuleImpl();
        lm.initialize(subject, new TestCallbackHandler(uaf), sharedState, Maps.newHashMap());
        assertTrue(lm.login());
        assertTrue(lm.commit());

        assertTrue(subject.getPrincipals().isEmpty());
        // no other public credentials than the AuthInfo
        assertEquals(1, subject.getPublicCredentials().size());
        // verify AuthInfo
        Set<AuthInfo> authInfos = subject.getPublicCredentials(AuthInfo.class);
        assertFalse(authInfos.isEmpty());
        assertEquals("uid", authInfos.iterator().next().getUserID());
    }

    @Test
    public void testLoginPreAuthenticatedWithReadOnlySubject() throws Exception {
        Authentication authentication = when(mock(Authentication.class).authenticate(any(Credentials.class))).thenReturn(true).getMock();
        UserAuthenticationFactory uaf = when(mock(UserAuthenticationFactory.class).getAuthentication(any(UserConfiguration.class), any(Root.class), anyString())).thenReturn(authentication).getMock();

        Map<String, Object> sharedState = Maps.newHashMap();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin("uid"));

        Subject subject = new Subject();
        subject.setReadOnly();
        LoginModuleImpl lm = new LoginModuleImpl();
        lm.initialize(subject, new TestCallbackHandler(uaf), sharedState, Maps.newHashMap());
        assertTrue(lm.login());
        assertTrue(lm.commit());

        assertTrue(subject.getPrincipals().isEmpty());
        assertTrue(subject.getPublicCredentials().isEmpty());
    }

    @Test
    public void testNullUserAuthentication() throws Exception {
        LoginModuleImpl loginModule = new LoginModuleImpl();
        CallbackHandler cbh = new TestCallbackHandler(mock(UserAuthenticationFactory.class));
        loginModule.initialize(new Subject(), cbh, Maps.newHashMap(), Maps.newHashMap());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
    }

    @Test
    public void testMissingUserAuthenticationFactory() throws Exception {
        CallbackHandler cbh = callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof RepositoryCallback) {
                    UserConfiguration uc = when(mock(UserConfiguration.class).getParameters()).thenReturn(ConfigurationParameters.EMPTY).getMock();
                    SecurityProvider sp = when(mock(SecurityProvider.class).getConfiguration(UserConfiguration.class)).thenReturn(uc).getMock();
                    ((RepositoryCallback) callback).setSecurityProvider(sp);
                    ((RepositoryCallback) callback).setContentRepository(getContentRepository());
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        };

        LoginModuleImpl loginModule = new LoginModuleImpl();
        loginModule.initialize(new Subject(), cbh, Maps.newHashMap(), Maps.newHashMap());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
    }

    @Test
    public void testMissingSecurityProviderGuestLogin() throws Exception {
        CallbackHandler cbh = callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof RepositoryCallback) {
                    ((RepositoryCallback) callback).setSecurityProvider(null);
                    ((RepositoryCallback) callback).setContentRepository(getContentRepository());
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        };

        LoginModuleImpl loginModule = new LoginModuleImpl();
        loginModule.initialize(new Subject(false, ImmutableSet.of(), ImmutableSet.of(new GuestCredentials()), ImmutableSet.of()), cbh, Maps.newHashMap(), Maps.newHashMap());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
    }

    @Test
    public void testMissingSecurityProvider() throws Exception {
        CallbackHandler cbh = callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof RepositoryCallback) {
                    ((RepositoryCallback) callback).setSecurityProvider(null);
                    ((RepositoryCallback) callback).setContentRepository(getContentRepository());
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        };

        LoginModuleImpl loginModule = new LoginModuleImpl();
        loginModule.initialize(new Subject(), cbh, Maps.newHashMap(), Maps.newHashMap());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
    }

    @Test
    public void testMissingRoot() throws Exception {
        CallbackHandler cbh = callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof RepositoryCallback) {
                    ((RepositoryCallback) callback).setSecurityProvider(getSecurityProvider());
                    ((RepositoryCallback) callback).setContentRepository(null);
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        };

        LoginModuleImpl loginModule = new LoginModuleImpl();
        loginModule.initialize(new Subject(), cbh, Maps.newHashMap(), Maps.newHashMap());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
    }

    @Test
    public void testMissingCallbackHandler() throws Exception {
        LoginModuleImpl loginModule = new LoginModuleImpl();
        loginModule.initialize(new Subject(), null, Maps.newHashMap(), Maps.newHashMap());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
    }

    @Test
    public void testLoginCustomUserAuthenticationFactory() throws Exception {
        UserAuthenticationFactory factory = (configuration, root, userId) -> new Authentication() {
            @Override
            public boolean authenticate(@Nullable Credentials credentials) {
                return true;
            }

            @Nullable
            @Override
            public String getUserId() {
                return null;
            }

            @Nullable
            @Override
            public Principal getUserPrincipal() {
                return null;
            }
        };

        CallbackHandler cbh = new TestCallbackHandler(factory);
        SimpleCredentials creds = new SimpleCredentials("loginId", new char[0]);
        Subject subject = new Subject(false, Sets.newHashSet(), ImmutableSet.of(creds), Sets.newHashSet());

        LoginModuleImpl loginModule = new LoginModuleImpl();
        loginModule.initialize(subject, cbh, Maps.newHashMap(), Maps.newHashMap());
        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        // authinfo falls back to loginId because Authentication.getUserId returned null
        AuthInfo authInfo = subject.getPublicCredentials(AuthInfo.class).iterator().next();
        assertEquals("loginId", authInfo.getUserID());
    }

    @Test
    public void testMissingUserId() throws Exception {
        UserAuthenticationFactory factory = (configuration, root, userId) -> new Authentication() {
            @Override
            public boolean authenticate(@Nullable Credentials credentials) {
                return true;
            }

            @Nullable
            @Override
            public String getUserId() {
                return null;
            }

            @Nullable
            @Override
            public Principal getUserPrincipal() {
                return null;
            }
        };

        CallbackHandler cbh = new TestCallbackHandler(factory);
        Subject subject = new Subject(false, Sets.newHashSet(), ImmutableSet.of(), Sets.newHashSet());

        LoginModuleImpl loginModule = new LoginModuleImpl();
        loginModule.initialize(subject, cbh, Maps.newHashMap(), Maps.newHashMap());
        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        AuthInfo authInfo = subject.getPublicCredentials(AuthInfo.class).iterator().next();
        assertNull(authInfo.getUserID());
        assertTrue(subject.getPrincipals().isEmpty());
    }


    private class TestCallbackHandler implements CallbackHandler {

        private final SecurityProvider sp;

        private TestCallbackHandler(@NotNull UserAuthenticationFactory authenticationFactory) {
            ConfigurationParameters params = ConfigurationParameters.of(
                    UserConfiguration.NAME,
                    ConfigurationParameters.of(
                            UserConstants.PARAM_USER_AUTHENTICATION_FACTORY, authenticationFactory));
            this.sp = SecurityProviderBuilder.newBuilder().with(params).build();
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
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
