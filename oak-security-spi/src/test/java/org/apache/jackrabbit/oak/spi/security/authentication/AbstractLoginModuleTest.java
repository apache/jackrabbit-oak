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
package org.apache.jackrabbit.oak.spi.security.authentication;

import java.io.IOException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.PrincipalProviderCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.RepositoryCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.SecurityProviderCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.UserManagerCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.WhiteboardCallback;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.TestPrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractLoginModuleTest {

    private static AbstractLoginModule initLoginModule(Class<?> supportedCredentials, Map<String, ?> sharedState) {
        AbstractLoginModule lm = new TestLoginModule(supportedCredentials);
        lm.initialize(new Subject(), null, sharedState, null);
        return lm;
    }

    private static AbstractLoginModule initLoginModule(Class<?> supportedCredentials, CallbackHandler cbh) {
        AbstractLoginModule lm = new TestLoginModule(supportedCredentials);
        lm.initialize(new Subject(), cbh, Collections.<String, Object>emptyMap(), null);
        return lm;
    }

    private static AbstractLoginModule initLoginModule(Class<?> supportedCredentials, CallbackHandler cbh, LoginModuleMonitor monitor) {
        AbstractLoginModule lm = new TestLoginModule(supportedCredentials, monitor);
        lm.initialize(new Subject(), cbh, Collections.emptyMap(), null);
        return lm;
    }

    private static AbstractLoginModule initLoginModule(Subject subject, CallbackHandler cbh, LoginModuleMonitor monitor) {
        AbstractLoginModule lm = new TestLoginModule(TestCredentials.class, monitor);
        lm.initialize(subject, cbh, Collections.emptyMap(), null);
        return lm;
    }

    @Test
    public void testInitializeWithOptions() {
        AbstractLoginModule lm = new TestLoginModule(TestCredentials.class);
        Map options = ImmutableMap.of("key", "value");
        lm.initialize(new Subject(), null, Collections.emptyMap(), options);

        assertNotSame(options, lm.options);
        assertEquals(options, lm.options);

        ConfigurationParameters options2 = ConfigurationParameters.of(options);
        lm.initialize(new Subject(), null, Collections.emptyMap(), options2);

        assertSame(options2, lm.options);
    }

    @Test
    public void testLogout() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, ImmutableMap.of());

        assertFalse(loginModule.logout());
    }

    @Test
    public void testLogoutSuccessClearsSubject() {
        Subject subject = new Subject(false, ImmutableSet.<Principal>of(new PrincipalImpl("pName")), ImmutableSet.of(new TestCredentials()), ImmutableSet.of());
        AbstractLoginModule loginModule = initLoginModule(subject, null, null);

        assertTrue(loginModule.logout());

        assertTrue(subject.getPublicCredentials().isEmpty());
        assertTrue(subject.getPrincipals().isEmpty());
    }

    @Test
    public void testLogoutSuccessReadOnlySubject() {
        Subject subject = new Subject(true, ImmutableSet.<Principal>of(new PrincipalImpl("pName")), ImmutableSet.of(new TestCredentials()), ImmutableSet.of());
        AbstractLoginModule loginModule = initLoginModule(subject, null, null);

        assertTrue(loginModule.logout());

        assertFalse(subject.getPublicCredentials().isEmpty());
        assertFalse(subject.getPrincipals().isEmpty());
    }

    @Test
    public void testLogoutSubjectWithoutCredentials() {
        Subject subject = new Subject(false, ImmutableSet.<Principal>of(new PrincipalImpl("pName")), ImmutableSet.of("stringNotCredentials"), ImmutableSet.of());
        AbstractLoginModule loginModule = initLoginModule(subject, null, null);
        loginModule.logout();

        assertFalse(subject.getPublicCredentials().isEmpty());
        assertFalse(subject.getPrincipals().isEmpty());

        subject = new Subject(false, ImmutableSet.<Principal>of(new PrincipalImpl("pName")), ImmutableSet.of(), ImmutableSet.of());
        loginModule = initLoginModule(subject, null, null);
        loginModule.logout();

        assertTrue(subject.getPublicCredentials().isEmpty());
        assertFalse(subject.getPrincipals().isEmpty());
    }

    @Test
    public void testLogoutSubjectWithoutPrincipals() {
        Subject subject = new Subject(false, ImmutableSet.<Principal>of(), ImmutableSet.of(new TestCredentials()), ImmutableSet.of());
        AbstractLoginModule loginModule = initLoginModule(subject, null, null);
        loginModule.logout();

        assertFalse(subject.getPublicCredentials().isEmpty());
        assertTrue(subject.getPrincipals().isEmpty());
    }

    @Test
    public void testAbort() throws LoginException {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, ImmutableMap.of());

        assertTrue(loginModule.abort());

        loginModule.login();
        assertTrue(loginModule.abort());
    }

    @Test
    public void testAbortWithFailedSystemLogout() throws LoginException {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler(new TestContentRepository(), null, null));

        // trigger creation of system-session
        loginModule.getRoot();
        assertTrue(loginModule.abort());
    }

    @Test
    public void testClearStateWithSessionCloseFailing() throws Exception {
        TestContentRepository cr = new TestContentRepository();
        doThrow(IOException.class).when(cr.cs).close();

        LoginModuleStats stats = newLoginModuleStats();
        CallbackHandler cbh = new TestCallbackHandler(cr, mock(SecurityProvider.class), null);

        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, cbh, stats);
        loginModule.getRoot();
        loginModule.clearState();
        assertEquals(1, stats.getLoginErrors());
        verify(cr.cs, times(1)).close();
    }

    @Test
    public void testGetSharedLoginName() {
        Map<String, String> sharedState = new HashMap<>();

        sharedState.put(AbstractLoginModule.SHARED_KEY_LOGIN_NAME, "test");
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, sharedState);
        assertEquals("test", lm.getSharedLoginName());

        sharedState.clear();
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getSharedLoginName());
    }

    @Test
    public void testGetSharedCredentials() {
        Map<String, Object> sharedState = new HashMap<>();

        sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, new TestCredentials());
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, sharedState);
        assertTrue(lm.getSharedCredentials() instanceof TestCredentials);

        sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, new SimpleCredentials("test", "test".toCharArray()));
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertTrue(lm.getSharedCredentials() instanceof SimpleCredentials);

        lm = initLoginModule(SimpleCredentials.class, sharedState);
        assertTrue(lm.getSharedCredentials() instanceof SimpleCredentials);

        sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, "no credentials object");
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getSharedCredentials());

        sharedState.clear();
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getSharedCredentials());
    }

    @Test
    public void testGetCredentialsFromSharedState() {
        Map<String, Credentials> sharedState = new HashMap<>();

        sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, new TestCredentials());
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, sharedState);
        assertTrue(lm.getCredentials() instanceof TestCredentials);

        SimpleCredentials sc = new SimpleCredentials("test", "test".toCharArray());
        sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, sc);
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getCredentials());

        sharedState.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, sc);
        lm = initLoginModule(SimpleCredentials.class, sharedState);
        assertTrue(lm.getCredentials() instanceof SimpleCredentials);

        sharedState.clear();
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getCredentials());
    }

    @Test
    public void testGetCredentialsFromSubject() {
        Subject subject = new Subject();


        subject.getPublicCredentials().add(new TestCredentials());

        AbstractLoginModule lm = new TestLoginModule(TestCredentials.class);
        lm.initialize(subject, null, ImmutableMap.<String, Object>of(), null);

        assertTrue(lm.getCredentials() instanceof TestCredentials);
    }

    @Test
    public void testGetCredentialsFromSubjectWrongClass() {
        Subject subject = new Subject();
        subject.getPublicCredentials().add(new SimpleCredentials("userid", new char[0]));

        AbstractLoginModule lm = new TestLoginModule(TestCredentials.class);
        lm.initialize(subject, null, ImmutableMap.<String, Object>of(), null);

        assertNull(lm.getCredentials());
    }

    @Test
    public void testGetCredentialsFromCallbackHandler() {
        CallbackHandler cbh = callbacks -> {
            for (Callback cb : callbacks) {
                if (cb instanceof CredentialsCallback) {
                    ((CredentialsCallback) cb).setCredentials(new TestCredentials());
                }
            }
        };

        AbstractLoginModule lm = initLoginModule(TestCredentials.class, cbh);
        assertTrue(lm.getCredentials() instanceof TestCredentials);

        lm = initLoginModule(SimpleCredentials.class, cbh);
        assertNull(lm.getCredentials());
    }

    @Test
    public void testGetCredentialsIOException() {
        LoginModuleMonitor monitor = mock(LoginModuleMonitor.class);
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(true), monitor);
        assertNull(lm.getCredentials());
        verify(monitor, times(1)).loginError();
    }

    @Test
    public void testGetCredentialsUnsupportedCallbackException() {
        LoginModuleMonitor monitor = mock(LoginModuleMonitor.class);
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(false), monitor);
        assertNull(lm.getCredentials());
        verify(monitor, times(1)).loginError();
    }

    @Test
    public void testGetCredentialsCallbackReturnsNull() {
        CallbackHandler cbh = callbacks -> {
            for (Callback cb : callbacks) {
                if (cb instanceof CredentialsCallback) {
                    ((CredentialsCallback) cb).setCredentials(null);
                }
            }
        };
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, cbh);
        assertNull(lm.getCredentials());
    }

    @Test
    public void testGetSharedPreAuthLoginEmptySharedState() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, ImmutableMap.of());
        assertNull(loginModule.getSharedPreAuthLogin());
    }

    @Test
    public void testGetSharedPreAuthLogin() {
        Map<String, PreAuthenticatedLogin> sharedState = new HashMap<>();
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, sharedState);

        PreAuthenticatedLogin preAuthenticatedLogin = new PreAuthenticatedLogin("userId");
        sharedState.put(AbstractLoginModule.SHARED_KEY_PRE_AUTH_LOGIN, preAuthenticatedLogin);

        assertSame(preAuthenticatedLogin, loginModule.getSharedPreAuthLogin());
    }

    @Test
    public void testGetSharedPreAuthLoginWrongEntry() {
        Map<String, String> sharedState = new HashMap<>();
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, sharedState);

        sharedState.put(AbstractLoginModule.SHARED_KEY_PRE_AUTH_LOGIN, "wrongType");

        assertNull(loginModule.getSharedPreAuthLogin());
    }

    @Test
    public void testIncompleteRepositoryCallback() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler());

        assertNull(loginModule.getSecurityProvider());
        assertNull(loginModule.getRoot());
    }

    @Test
    public void testGetRoot() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler(new TestContentRepository(), null, null));

        Root root = loginModule.getRoot();
        assertNotNull(root);
        // root is stored as field -> second access returns the same object
        assertSame(root, loginModule.getRoot());
    }

    private static LoginModuleStats newLoginModuleStats() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        StatisticsProvider sp = new DefaultStatisticsProvider(executor);
        return new LoginModuleStats(sp);
    }

    @Test
    public void testGetRootIOException() {
        LoginModuleStats stats = newLoginModuleStats();
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(true), stats);
        assertNull(lm.getRoot());
        assertEquals(1, stats.getLoginErrors());
    }

    @Test
    public void testGetRootUnsupportedCallbackException() {
        LoginModuleStats stats = newLoginModuleStats();
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(false), stats);
        assertNull(lm.getRoot());
        assertEquals(1, stats.getLoginErrors());
    }

    @Test
    public void testGetRootMissingCallbackHandler() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, null, null);
        assertNull(loginModule.getRoot());
    }

    @Test
    public void testGetSecurityProvider() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler(null, new OpenSecurityProvider(), null));

        SecurityProvider securityProvider = loginModule.getSecurityProvider();
        assertNotNull(securityProvider);
        // securityProvider is stored as field -> second access returns the same object
        assertSame(securityProvider, loginModule.getSecurityProvider());
    }

    @Test
    public void testGetSecurityProviderIOException() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(true));

        assertNull(loginModule.getSecurityProvider());
    }

    @Test
    public void testGetSecurityProviderUnsupportedCallbackException() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(false));

        assertNull(loginModule.getRoot());
    }

    @Test
    public void testGetSecurityProviderMissingCallbackHandler() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, null, null);
        assertNull(loginModule.getSecurityProvider());
    }

    @Test
    public void testGetWhiteboardFromCallback() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler(new DefaultWhiteboard()));

        Whiteboard wb = loginModule.getWhiteboard();
        assertNotNull(wb);
        // whiteboard is stored as field -> second access returns the same object
        assertSame(wb, loginModule.getWhiteboard());

    }

    @Test
    public void testGetWhiteboardFromIncompleteCallback() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler());

        assertNull(loginModule.getWhiteboard());
    }

    @Test
    public void testGetWhiteboardIOException() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(true));

        assertNull(loginModule.getWhiteboard());
    }

    @Test
    public void testGetWhiteboardUnsupportedCallbackException() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(false));

        assertNull(loginModule.getWhiteboard());
    }

    @Test
    public void testGetWhiteBoardMissingCallbackHandler() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, null, null);
        assertNull(loginModule.getWhiteboard());
    }

    @Test
    public void testGetUserManagerFromCallback() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler(mock(UserManager.class)));

        UserManager userManager = loginModule.getUserManager();
        assertNotNull(userManager);
        // usermanager is stored as field -> second access returns the same object
        assertSame(userManager, loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerFromIncompleteCallback() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler());

        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerIOException() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(true));

        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerUnsupportedCallbackException() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(false));

        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerWithRepositoryCallbackHandler() throws Exception {
        Root r = mock(Root.class);
        ContentSession cs = when(mock(ContentSession.class).getLatestRoot()).thenReturn(r).getMock();
        ContentRepository cp = when(mock(ContentRepository.class).login(null, null)).thenReturn(cs).getMock();
        UserManager um = mock(UserManager.class);
        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(r, NamePathMapper.DEFAULT)).thenReturn(um).getMock();
        SecurityProvider sp = when(mock(SecurityProvider.class).getConfiguration(UserConfiguration.class)).thenReturn(uc).getMock();

        CallbackHandler cbh = new TestCallbackHandler(cp, sp, null);
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, cbh, null);
        assertEquals(um, loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerMissingCallbackHandler() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, null, null);
        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerMissingRoot() throws Exception {
        ContentRepository cp = when(mock(ContentRepository.class).login(null, null)).thenReturn(mock(ContentSession.class)).getMock();

        CallbackHandler cbh = new TestCallbackHandler(cp, mock(SecurityProvider.class), null);
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, cbh, null);
        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerMissingSecurityProvider() throws Exception {
        ContentSession cs = when(mock(ContentSession.class).getLatestRoot()).thenReturn(mock(Root.class)).getMock();
        ContentRepository cp = when(mock(ContentRepository.class).login(null, null)).thenReturn(cs).getMock();

        CallbackHandler cbh = new TestCallbackHandler(cp, null, null);
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, cbh, null);
        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetPrincipalProviderFromCallback() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler(new TestPrincipalProvider()));

        assertNotNull(loginModule.getPrincipalProvider());

        PrincipalProvider principalProvider = loginModule.getPrincipalProvider();
        assertNotNull(principalProvider);
        // principalProvider is stored as field -> second access returns the same object
        assertSame(principalProvider, loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderFromIncompleteCallback() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler());

        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderIOException() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(true));

        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderUnsupportedCallbackException() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(false));

        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderWithRepositoryCallbackHandler() throws Exception {
        Root r = mock(Root.class);
        ContentSession cs = when(mock(ContentSession.class).getLatestRoot()).thenReturn(r).getMock();
        ContentRepository cp = when(mock(ContentRepository.class).login(null, null)).thenReturn(cs).getMock();
        PrincipalProvider pp = mock(PrincipalProvider.class);
        PrincipalConfiguration pc = when(mock(PrincipalConfiguration.class).getPrincipalProvider(r, NamePathMapper.DEFAULT)).thenReturn(pp).getMock();
        SecurityProvider sp = when(mock(SecurityProvider.class).getConfiguration(PrincipalConfiguration.class)).thenReturn(pc).getMock();

        CallbackHandler cbh = new TestCallbackHandler(cp, sp, null);
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, cbh, null);
        assertEquals(pp, loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderMissingCallbackHandler() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, null, null);
        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderMissingRoot() throws Exception {
        ContentRepository cp = when(mock(ContentRepository.class).login(null, null)).thenReturn(mock(ContentSession.class)).getMock();

        CallbackHandler cbh = new TestCallbackHandler(cp, mock(SecurityProvider.class), null);
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, cbh, null);
        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderMissingSecurityProvider() throws Exception {
        ContentSession cs = when(mock(ContentSession.class).getLatestRoot()).thenReturn(mock(Root.class)).getMock();
        ContentRepository cp = when(mock(ContentRepository.class).login(null, null)).thenReturn(cs).getMock();

        CallbackHandler cbh = new TestCallbackHandler(cp, null, null);
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, cbh, null);
        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipals() {
        PrincipalProvider principalProvider = new TestPrincipalProvider();

        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler(principalProvider));

        Principal principal = principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP).next();
        String userId = TestPrincipalProvider.getIDFromPrincipal(principal);
        Set<? extends Principal> principals = loginModule.getPrincipals(userId);

        assertFalse(principals.isEmpty());
        assertEquals(principalProvider.getPrincipals(userId), principals);
    }


    @Test
    public void testGetPrincipalsMissingProvider() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler());

        Set<? extends Principal> principals = loginModule.getPrincipals("userId");
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetPrincipalsFromPrincipal() {
        PrincipalProvider principalProvider = new TestPrincipalProvider();

        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler(principalProvider));

        Principal principal = principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP).next();
        Set<Principal> expected = new HashSet<>();
        expected.add(principal);
        expected.addAll(principalProvider.getMembershipPrincipals(principal));

        Set<? extends Principal> principals = loginModule.getPrincipals(principal);

        assertFalse(principals.isEmpty());
        assertEquals(expected, principals);
    }

    @Test
    public void testGetPrincipalsFromPrincipalMissingProvider() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, new TestCallbackHandler());

        Set<? extends Principal> principals = loginModule.getPrincipals(new PrincipalImpl("principalName"));
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testSetAuthInfo() {
        Subject subject = new Subject();
        AuthInfo authInfo = new AuthInfoImpl("userid", null, null);

        AbstractLoginModule.setAuthInfo(authInfo, subject);
        Set<AuthInfo> fromSubject = subject.getPublicCredentials(AuthInfo.class);
        assertEquals(1, fromSubject.size());
        assertSame(authInfo, fromSubject.iterator().next());
    }

    @Test
    public void testSetAuthInfoPreExisting() {
        Subject subject = new Subject();
        subject.getPublicCredentials().add(new AuthInfoImpl(null, null, null));

        AuthInfo authInfo = new AuthInfoImpl("userid", null, null);

        AbstractLoginModule.setAuthInfo(authInfo, subject);
        Set<AuthInfo> fromSubject = subject.getPublicCredentials(AuthInfo.class);
        assertEquals(1, fromSubject.size());
        assertSame(authInfo, fromSubject.iterator().next());
    }

    @Test
    public void testOnError() {
        LoginModuleStats stats = newLoginModuleStats();
        CallbackHandler cbh = callbacks -> {
            for (Callback cb : callbacks) {
                if (cb instanceof RepositoryCallback) {
                    ((RepositoryCallback) cb).setLoginModuleMonitor(stats);
                }
            }
        };

        AbstractLoginModule lm = initLoginModule(TestCredentials.class, cbh, null);
        assertTrue(lm.getLoginModuleMonitor() instanceof LoginModuleStats);
        lm.onError();
        assertEquals(1, stats.getLoginErrors());
    }

    @Test
    public void testNullLoginModuleMonitor() {
        LoginModuleStats stats = newLoginModuleStats();
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, null, null);
        assertNull(lm.getLoginModuleMonitor());
        lm.onError();
        assertEquals(0, stats.getLoginErrors());
    }

    @Test
    public void testErrorOnGetLoginModuleMonitor() {
        LoginModuleStats stats = newLoginModuleStats();
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, new ThrowingCallbackHandler(true), null);

        assertNull(lm.getLoginModuleMonitor());
        lm.onError();
        assertEquals(0, stats.getLoginErrors());
    }

    //--------------------------------------------------------------------------

    private final class TestCredentials implements Credentials {}

    private static final class TestLoginModule extends AbstractLoginModule {

        private final Class supportedCredentialsClass;

        private LoginModuleMonitor mon;

        private TestLoginModule(Class supportedCredentialsClass) {
            this.supportedCredentialsClass = supportedCredentialsClass;
        }

        private TestLoginModule(Class supportedCredentialsClass, LoginModuleMonitor mon) {
            this.supportedCredentialsClass = supportedCredentialsClass;
            this.mon = mon;
        }

        @NotNull
        @Override
        protected Set<Class> getSupportedCredentials() {
            return Collections.singleton(supportedCredentialsClass);
        }

        @Override
        public boolean login() {
            return true;
        }

        @Override
        public boolean commit() {
            return true;
        }

        @Override
        protected LoginModuleMonitor getLoginModuleMonitor() {
            if (mon != null) {
                return mon;
            } else {
                return super.getLoginModuleMonitor();
            }
        }
    }

    private final class TestCallbackHandler implements CallbackHandler {

        private Whiteboard whiteboard = null;
        private UserManager userManager = null;
        private PrincipalProvider principalProvider = null;
        private ContentRepository contentRepository = null;
        private SecurityProvider securityProvider = null;
        private String workspaceName = null;

        private TestCallbackHandler() {
        }

        private TestCallbackHandler(@NotNull Whiteboard whiteboard) {
            this.whiteboard = whiteboard;
        }

        private TestCallbackHandler(@NotNull UserManager userManager) {
            this.userManager = userManager;
        }

        private TestCallbackHandler(@NotNull PrincipalProvider principalProvider) {
            this.principalProvider = principalProvider;
        }

        private TestCallbackHandler(@Nullable ContentRepository contentRepository, @Nullable SecurityProvider securityProvider, @Nullable String workspaceName) {
            this.contentRepository = contentRepository;
            this.securityProvider = securityProvider;
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback cb : callbacks) {
                if (cb instanceof WhiteboardCallback) {
                    ((WhiteboardCallback) cb).setWhiteboard(whiteboard);
                } else if (cb instanceof PrincipalProviderCallback) {
                    ((PrincipalProviderCallback) cb).setPrincipalProvider(principalProvider);
                } else if (cb instanceof UserManagerCallback) {
                    ((UserManagerCallback) cb).setUserManager(userManager);
                } else if (cb instanceof SecurityProviderCallback) {
                    ((SecurityProviderCallback) cb).setSecurityProvider(securityProvider);
                } else if (cb instanceof RepositoryCallback) {
                    RepositoryCallback rcb = (RepositoryCallback) cb;
                    rcb.setContentRepository(contentRepository);
                    rcb.setSecurityProvider(securityProvider);
                    rcb.setWorkspaceName(workspaceName);
                    rcb.setLoginModuleMonitor(LoginModuleMonitor.NOOP);
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }

        }

    }

    private final class TestContentRepository implements ContentRepository {

        private ContentSession cs = Mockito.mock(ContentSession.class);

        @NotNull
        @Override
        public ContentSession login(@Nullable Credentials credentials, @Nullable String workspaceName) {
            Mockito.when(cs.getLatestRoot()).thenReturn(Mockito.mock(Root.class));
            return cs;

        }

        @NotNull
        @Override
        public Descriptors getDescriptors() {
            throw new UnsupportedOperationException();
        }
    }
}
