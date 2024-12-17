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

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.commons.jdkcompat.Java23Subject;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.security.user.UserAuthenticationFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthInfoImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.Authentication;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMonitor;
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.RepositoryCallback;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserAuthenticationFactory;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.slf4j.event.Level;

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
import java.lang.reflect.Field;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule.SHARED_KEY_CREDENTIALS;
import static org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule.SHARED_KEY_PRE_AUTH_LOGIN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class LoginModuleImplTest extends AbstractSecurityTest {

    private static final String USER_ID = "test";
    private static final String USER_ID_CASED = "TeSt";
    private static final String USER_PW = "pw";

    private final LoginModuleMonitor monitor = mock(LoginModuleMonitor.class);
    private User user;

    @Override
    public void after() throws Exception {
        try {
            clearInvocations(monitor);
            if (user != null) {
                user.remove();
                root.commit();
            }
        } finally {
            super.after();
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

    @NotNull
    private CallbackHandler createCallbackHandler(@Nullable ContentRepository repository, @Nullable SecurityProvider securityProvider) {
        return callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof RepositoryCallback) {
                    ((RepositoryCallback) callback).setSecurityProvider(securityProvider);
                    ((RepositoryCallback) callback).setContentRepository(repository);
                    ((RepositoryCallback) callback).setLoginModuleMonitor(monitor);
                } else if (callback instanceof CredentialsCallback) {
                    // ignore
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        };
    }

    @NotNull
    private CallbackHandler createCallbackHandler(@NotNull UserAuthenticationFactory authenticationFactory) {
        ConfigurationParameters params = ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(
                        UserConstants.PARAM_USER_AUTHENTICATION_FACTORY, authenticationFactory));
        SecurityProvider sp = SecurityProviderBuilder.newBuilder().with(params).build();
        return createCallbackHandler(getContentRepository(), sp);
    }

    private LoginModuleImpl createLoginModule(@NotNull Subject subject, @Nullable CallbackHandler cbh, @NotNull Map<String, ?> sharedState) {
        LoginModuleImpl lm = new LoginModuleImpl();
        lm.initialize(subject, cbh, sharedState, new HashMap<>());
        return lm;
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

    @Test(expected = LoginException.class)
    public void testFailedLoginWithMonitor() throws Exception {
        createTestUser();

        Credentials credentials = new SimpleCredentials(USER_ID, "wrongPw".toCharArray());
        HashMap<String, Object> shared = new HashMap<>();
        shared.put(SHARED_KEY_CREDENTIALS, credentials);
        LoginModuleImpl lm = createLoginModule(new Subject(), createCallbackHandler(getContentRepository(), getSecurityProvider()), shared);
        try {
            lm.login();
        } finally {
            verify(monitor).loginFailed(any(LoginException.class), eq(credentials));
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

        Subject subject = new Subject(false, Set.of(), Set.of(unsupportedCredentials), Set.of());
        LoginModuleImpl lm = createLoginModule(subject, cbh, new HashMap<>());
        assertFalse(lm.login());

        verifyNoInteractions(monitor);
    }

    @Test
    public void testLoginPreAuthenticated() throws Exception {
        User testUser = getTestUser();
        Set<? extends Principal> principals = getConfig(PrincipalConfiguration.class).getPrincipalProvider(root, NamePathMapper.DEFAULT).getPrincipals(testUser.getID());

        Authentication authentication = mock(Authentication.class);
        when(authentication.authenticate(any(Credentials.class))).thenReturn(true).getMock();
        when(authentication.getUserId()).thenReturn(testUser.getID()); // but getUserPrincipal returns null

        Principal foreignPrincipal = new PrincipalImpl("foreign");

        UserAuthenticationFactory uaf = when(mock(UserAuthenticationFactory.class).getAuthentication(any(UserConfiguration.class), any(Root.class), anyString())).thenReturn(authentication).getMock();
        Map<String, Object> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin("uid"));

        Subject subject = new Subject(false, Set.of(foreignPrincipal), Set.of(), Set.of());
        LoginModuleImpl lm = createLoginModule(subject, createCallbackHandler(uaf), sharedState);
        assertTrue(lm.login());
        assertTrue(lm.commit());

        // verify subject has been updated with test-user principals
        Set<Principal> expected = new ImmutableSet.Builder().add(foreignPrincipal).addAll(principals).build();
        assertEquals(expected, subject.getPrincipals());
        // no other public credentials than the AuthInfo
        assertEquals(1, subject.getPublicCredentials().size());

        // verify AuthInfo
        Set<AuthInfo> authInfos = subject.getPublicCredentials(AuthInfo.class);
        assertFalse(authInfos.isEmpty());
        AuthInfo ai = authInfos.iterator().next();
        assertEquals(testUser.getID(), ai.getUserID());
        assertEquals(expected, ai.getPrincipals());

        // verify logout only removes credentials/principals associated with this very login module
        assertTrue(lm.logout());
        assertTrue(subject.getPublicCredentials().isEmpty());
        assertTrue(subject.getPrincipals().contains(foreignPrincipal));
        assertFalse(subject.getPrincipals().containsAll(principals));

        verify(monitor).principalsCollected(anyLong(), anyInt());
        verifyNoMoreInteractions(monitor);
    }

    @Test
    public void testLoginPreAuthenticatedWithReadOnlySubject() throws Exception {
        Authentication authentication = when(mock(Authentication.class).authenticate(any(Credentials.class))).thenReturn(true).getMock();
        UserAuthenticationFactory uaf = when(mock(UserAuthenticationFactory.class).getAuthentication(any(UserConfiguration.class), any(Root.class), anyString())).thenReturn(authentication).getMock();

        Map<String, Object> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin("uid"));

        Subject subject = new Subject();
        subject.setReadOnly();
        LoginModuleImpl lm = createLoginModule(subject, createCallbackHandler(uaf), sharedState);
        assertTrue(lm.login());
        assertTrue(lm.commit());

        assertTrue(subject.getPrincipals().isEmpty());
        assertTrue(subject.getPublicCredentials().isEmpty());

        assertTrue(lm.logout());

        verify(monitor).principalsCollected(anyLong(), anyInt());
        verifyNoMoreInteractions(monitor);
    }

    @Test(expected = LoginException.class)
    public void testLoginPreAuthenticatedFails() throws Exception {
        LoginException le = new LoginException();
        Authentication authentication = when(mock(Authentication.class).authenticate(PreAuthenticatedLogin.PRE_AUTHENTICATED)).thenThrow(le).getMock();
        UserAuthenticationFactory uaf = when(mock(UserAuthenticationFactory.class).getAuthentication(any(UserConfiguration.class), any(Root.class), anyString())).thenReturn(authentication).getMock();

        Map<String, Object> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin("uid"));

        LoginModuleImpl lm = createLoginModule(new Subject(), createCallbackHandler(uaf), sharedState);
        try {
            lm.login();
        } finally {
            verify(monitor).loginFailed(le, PreAuthenticatedLogin.PRE_AUTHENTICATED);
            verifyNoMoreInteractions(monitor);
        }
    }

    @Test
    public void testLoginWithReadOnlySubject() throws Exception {
        Map<String, Object> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_CREDENTIALS, getAdminCredentials());

        Principal unknownPrincipal = new PrincipalImpl("unknown");
        Subject subject = new Subject(true, Collections.singleton(unknownPrincipal), Collections.emptySet(), Collections.emptySet());

        LoginModuleImpl lm = createLoginModule(subject, createCallbackHandler(new UserAuthenticationFactoryImpl()), sharedState);

        assertTrue(lm.login());
        assertTrue(lm.commit());

        assertFalse(subject.getPrincipals().isEmpty());
        assertTrue(subject.getPublicCredentials().isEmpty());

        assertTrue(lm.logout());

        assertFalse(subject.getPrincipals().isEmpty());
        assertTrue(subject.getPublicCredentials().isEmpty());

        verify(monitor).principalsCollected(anyLong(), anyInt());
        verifyNoMoreInteractions(monitor);
    }

    @Test
    public void testNullUserAuthentication() throws Exception {
        CallbackHandler cbh = createCallbackHandler(mock(UserAuthenticationFactory.class));
        LoginModuleImpl loginModule = createLoginModule(new Subject(), cbh, new HashMap<>());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());

        verifyNoInteractions(monitor);
    }

    @Test
    public void testMissingUserAuthenticationFactory() throws Exception {
        UserConfiguration uc = when(mock(UserConfiguration.class).getParameters()).thenReturn(ConfigurationParameters.EMPTY).getMock();
        SecurityProvider sp = when(mock(SecurityProvider.class).getConfiguration(UserConfiguration.class)).thenReturn(uc).getMock();
        CallbackHandler cbh = createCallbackHandler(getContentRepository(), sp);

        LoginModuleImpl loginModule = createLoginModule(new Subject(), cbh, new HashMap<>());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());

        verifyNoInteractions(monitor);
    }

    @Test
    public void testMissingSecurityProviderGuestLogin() throws Exception {
        CallbackHandler cbh = createCallbackHandler(getContentRepository(), null);

        LoginModuleImpl loginModule = createLoginModule(new Subject(false, Set.of(), Set.of(new GuestCredentials()), Set.of()), cbh, new HashMap<>());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());

        verifyNoInteractions(monitor);
    }

    @Test
    public void testMissingSecurityProvider() throws Exception {
        CallbackHandler cbh = createCallbackHandler(getContentRepository(), null);

        LoginModuleImpl loginModule = createLoginModule(new Subject(), cbh, new HashMap<>());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());

        verifyNoInteractions(monitor);
    }

    @Test
    public void testMissingRoot() throws Exception {
        CallbackHandler cbh = createCallbackHandler(null, getSecurityProvider());

        LoginModuleImpl loginModule = createLoginModule(new Subject(), cbh, new HashMap<>());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());

        verifyNoInteractions(monitor);
    }

    @Test
    public void testMissingCallbackHandler() throws Exception {
        LoginModuleImpl loginModule = createLoginModule(new Subject(), null, new HashMap<>());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());

        verifyNoInteractions(monitor);
    }

    @Test
    public void testUnsupportedCredentialsCallback() throws Exception {
        CallbackHandler cbh = callbacks -> {
            for (Callback cb : callbacks) {
                if (cb instanceof RepositoryCallback) {
                    RepositoryCallback rcb = (RepositoryCallback) cb;
                    rcb.setLoginModuleMonitor(monitor);
                    rcb.setContentRepository(getContentRepository());
                    rcb.setSecurityProvider(getSecurityProvider());
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }
        };
        LoginModuleImpl loginModule = createLoginModule(new Subject(), cbh, new HashMap<>());

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());

        verify(monitor).loginError();
        verifyNoMoreInteractions(monitor);
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

        CallbackHandler cbh = createCallbackHandler(factory);
        SimpleCredentials creds = new SimpleCredentials("loginId", new char[0]);
        Subject subject = new Subject(false, new HashSet<>(), Set.of(creds), new HashSet<>());

        LoginModuleImpl loginModule = createLoginModule(subject, cbh, new HashMap<>());
        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        // authinfo falls back to loginId because Authentication.getUserId returned null
        AuthInfo authInfo = subject.getPublicCredentials(AuthInfo.class).iterator().next();
        assertEquals("loginId", authInfo.getUserID());

        assertTrue(loginModule.logout());
        assertTrue(subject.getPrincipals().isEmpty());
        assertTrue(subject.getPublicCredentials().isEmpty());

        verify(monitor).principalsCollected(anyLong(), anyInt());
        verifyNoMoreInteractions(monitor);
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

        CallbackHandler cbh = createCallbackHandler(factory);
        Subject subject = new Subject(false, new HashSet<>(), Set.of(), new HashSet<>());

        LoginModuleImpl loginModule = createLoginModule(subject, cbh, new HashMap<>());
        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        AuthInfo authInfo = subject.getPublicCredentials(AuthInfo.class).iterator().next();
        assertNull(authInfo.getUserID());
        assertTrue(subject.getPrincipals().isEmpty());

        assertTrue(loginModule.logout());
        assertTrue(subject.getPublicCredentials().isEmpty());
        assertTrue(subject.getPrincipals().isEmpty());

        verify(monitor, never()).principalsCollected(anyLong(), anyInt());
        verifyNoInteractions(monitor);
    }

    @Test
    public void testCommitReadOnlySubject() throws Exception {
        Principal principal = new PrincipalImpl("subjetPrincipal");
        Subject subject = new Subject(true, Set.of(principal), Set.of(), Set.of());

        Map<String, Object> shared = new HashMap<>();
        shared.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, new SimpleCredentials(getTestUser().getID(), getTestUser().getID().toCharArray()));

        LoginModuleImpl loginModule = createLoginModule(subject, createCallbackHandler(new UserAuthenticationFactoryImpl()), shared);

        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        // auth-Info field must not be cleared by successful commit
        Field f = LoginModuleImpl.class.getDeclaredField("authInfo");
        f.setAccessible(true);
        AuthInfo ai = (AuthInfo) f.get(loginModule);
        assertNotNull(ai);
        assertTrue(ai.getPrincipals().contains(principal));
        assertTrue(ai.getPrincipals().contains(getTestUser().getPrincipal()));

        verify(monitor).principalsCollected(anyLong(), anyInt());
        verifyNoMoreInteractions(monitor);
    }

    @Test
    public void testLoginLogoutPreexistingReadonlySubject() throws Exception {
        createTestUser();
        Subject subject = new Subject(true, Collections.singleton(() -> "JMXPrincipal: foo"), Collections.EMPTY_SET, Collections.EMPTY_SET);
        Java23Subject.doAs(subject, (PrivilegedExceptionAction<Void>) () -> {
            LogCustomizer logCustomizer = LogCustomizer
                    .forLogger("org.apache.jackrabbit.oak.core.ContentSessionImpl")
                    .enable(Level.ERROR)
                    .create();

            ContentSession cs = login(new SimpleCredentials(USER_ID, USER_PW.toCharArray()));
            try {
                logCustomizer.starting();
                cs.close();
            } finally {
                //verify that ContentSessionImpl.close() did not log anything
                assertEquals(0, logCustomizer.getLogs().size());
                logCustomizer.finished();
            }
            return null;
        });
    }
}
