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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
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
import org.mockito.Answers;

import static org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule.SHARED_KEY_CREDENTIALS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class AbstractLoginModuleTest {

    private static AbstractLoginModule initLoginModule(@NotNull Class<?> supportedCredentials, @NotNull  Map<String, ?> sharedState) {
        AbstractLoginModule lm = new TestLoginModule(supportedCredentials);
        initialize(lm, new Subject(), null, sharedState);
        return lm;
    }

    private static AbstractLoginModule initLoginModule(@Nullable CallbackHandler cbh) {
        AbstractLoginModule lm = new TestLoginModule(TestCredentials.class);
        initialize(lm, new Subject(), cbh, Collections.emptyMap());
        return lm;
    }

    private static AbstractLoginModule initLoginModule(@NotNull CallbackHandler cbh, @NotNull LoginModuleMonitor monitor) {
        AbstractLoginModule lm = new TestLoginModule(TestCredentials.class, monitor);
        initialize(lm, new Subject(), cbh, Collections.emptyMap());
        return lm;
    }

    private static AbstractLoginModule initLoginModule(@NotNull Subject subject) {
        AbstractLoginModule lm = new TestLoginModule(TestCredentials.class, null);
        initialize(lm, subject, mock(CallbackHandler.class), Collections.emptyMap());
        return lm;
    }

    private static void initialize(@NotNull AbstractLoginModule loginModule, @NotNull Subject subject, @Nullable CallbackHandler cbh, @NotNull Map<String, ?> sharedState) {
        loginModule.initialize(subject, cbh, sharedState, null);
    }

    private static ContentRepository mockContentRepository(@Nullable ContentSession contentSession) throws Exception {
        ContentSession cs = (contentSession == null) ? mock(ContentSession.class) : contentSession;
        Root r = when(mock(Root.class).getContentSession()).thenReturn(cs).getMock();
        when(cs.getLatestRoot()).thenReturn(r);
        return when(mock(ContentRepository.class).login(null, null)).thenReturn(cs).getMock();
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
    public void testLogoutIgnored() throws LoginException {
        AbstractLoginModule loginModule = initLoginModule(new Subject());
        assertFalse(loginModule.logout());
    }

    @Test
    public void testLogoutSuccess() throws LoginException {
        PrincipalProvider pp = new TestPrincipalProvider("user");
        Principal p = pp.getPrincipal("user");
        Principal foreignPrincipal = TestPrincipalProvider.UNKNOWN;

        String userId = TestPrincipalProvider.getIDFromPrincipal(p);

        Credentials creds = new SimpleCredentials(userId, new char[0]);
        Credentials foreign1 = new GuestCredentials();
        Credentials foreign2 = new TokenCredentials("token");

        Subject subject = new Subject(false,
                ImmutableSet.of(foreignPrincipal),
                ImmutableSet.of(creds, foreign1, foreign2), ImmutableSet.of());

        TestLoginModule loginModule = new TestLoginModule(SimpleCredentials.class);
        loginModule.initialize(subject, new TestCallbackHandler(pp), Collections.emptyMap(), null);

        assertFalse(loginModule.logout());

        loginModule.loginAndCommit(userId);
        assertTrue(subject.getPrincipals().contains(p));

        assertTrue(loginModule.logout());

        Set publicCreds = subject.getPublicCredentials();
        assertFalse(publicCreds.contains(creds));
        assertTrue(publicCreds.contains(foreign1));
        assertTrue(publicCreds.contains(foreign2));

        Set<Principal> principals = subject.getPrincipals();
        assertFalse(principals.contains(p));
        assertTrue(principals.contains(foreignPrincipal));
    }

    @Test
    public void testLogoutDestroyable() throws Exception {
        Credentials creds = mock(TestCredentials.class, withSettings().extraInterfaces(Destroyable.class));
        Credentials foreign1 = new GuestCredentials();
        Credentials foreign2 = new TokenCredentials("token");

        Subject subject = new Subject(true,
                ImmutableSet.<Principal>of(new PrincipalImpl("pName")),
                ImmutableSet.of(creds, foreign1, foreign2), ImmutableSet.of());
        TestLoginModule loginModule = new TestLoginModule(TestCredentials.class);
        loginModule.initialize(subject, null, Collections.emptyMap(), null);

        assertFalse(loginModule.logout());

        loginModule.loginAndCommit("userId");
        assertTrue(loginModule.logout());

        verify(((Destroyable) creds), times(1)).destroy();
    }

    @Test(expected = LoginException.class)
    public void testLogoutDestroyFails() throws Exception {
        Credentials creds = mock(TestCredentials.class, withSettings().extraInterfaces(Destroyable.class));
        doThrow(new DestroyFailedException()).when((Destroyable)creds).destroy();

        Subject subject = new Subject(true, ImmutableSet.of(), ImmutableSet.of(creds), ImmutableSet.of());
        TestLoginModule loginModule = new TestLoginModule(TestCredentials.class);
        loginModule.initialize(subject, null, Collections.emptyMap(), null);

        loginModule.loginAndCommit("userId");
        assertTrue(loginModule.logout());
    }

    @Test(expected = LoginException.class)
    public void testLogoutNotDestroyable() throws LoginException {
        Credentials creds = new TestCredentials();
        Subject subject = new Subject(true,
                ImmutableSet.of(),
                ImmutableSet.of(creds), ImmutableSet.of());

        TestLoginModule loginModule = (TestLoginModule) initLoginModule(subject);
        loginModule.loginAndCommit("userId");

        // logout must fail
        loginModule.logout();
    }

    @Test
    public void testLogoutInvalidCredentials() throws LoginException {
        // subject with invalid credentials
        Subject subject = new Subject(false, ImmutableSet.of(new PrincipalImpl("pName")), ImmutableSet.of("stringNotCredentials"), ImmutableSet.of());
        AbstractLoginModule loginModule = initLoginModule(subject);

        assertFalse(loginModule.logout());
    }

    @Test
    public void testLogoutMissingCredentials() throws LoginException {
        Credentials creds = new TestCredentials();
        Principal principal = TestPrincipalProvider.UNKNOWN;
        TestPrincipalProvider pp = new TestPrincipalProvider("principal");

        Subject subject = new Subject(false, ImmutableSet.of(principal), ImmutableSet.of(creds), ImmutableSet.of());

        TestLoginModule loginModule = new TestLoginModule(SimpleCredentials.class);
        loginModule.initialize(subject, new TestCallbackHandler(pp), Collections.emptyMap(), null);
        loginModule.getPrincipals(pp.getTestPrincipals().iterator().next());

        assertFalse(loginModule.logout());
        // subject must not be altered by logout
        assertTrue(subject.getPublicCredentials().contains(creds));
        assertTrue(subject.getPrincipals().contains(principal));
    }

    @Test
    public void testLogoutMissingPrincipals() throws LoginException {
        Credentials creds = new TestCredentials();
        Principal preExisting = new PrincipalImpl("pName");

        Subject subject = new Subject(false, ImmutableSet.of(preExisting), ImmutableSet.of(creds), ImmutableSet.of());
        TestLoginModule loginModule = (TestLoginModule) initLoginModule(subject);
        loginModule.credentials = creds;

        assertFalse(loginModule.logout());

        assertTrue(subject.getPublicCredentials().contains(creds));
        assertTrue(subject.getPrincipals().contains(preExisting));
    }

    @Test
    public void testAbort() throws LoginException {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, ImmutableMap.of());

        assertTrue(loginModule.abort());
    }

    @Test
    public void testAbortWithFailedSystemLogout() throws Exception {
        ContentRepository cr = mockContentRepository(null);
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler(cr, null));

        // trigger creation of system-session
        loginModule.getRoot();
        assertTrue(loginModule.abort());
    }

    @Test
    public void testClearStateWithSessionCloseFailing() throws Exception {
        ContentSession cs = mock(ContentSession.class);
        ContentRepository cr = mockContentRepository(cs);
        doThrow(IOException.class).when(cs).close();

        LoginModuleStats stats = newLoginModuleStats();
        CallbackHandler cbh = new TestCallbackHandler(cr, mock(SecurityProvider.class));

        AbstractLoginModule loginModule = initLoginModule(cbh, stats);
        loginModule.getRoot();
        loginModule.clearState();
        assertEquals(1, stats.getLoginErrors());
        verify(cs, times(1)).close();
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

        sharedState.put(SHARED_KEY_CREDENTIALS, new TestCredentials());
        AbstractLoginModule lm = initLoginModule(TestCredentials.class, sharedState);
        assertTrue(lm.getSharedCredentials() instanceof TestCredentials);

        sharedState.put(SHARED_KEY_CREDENTIALS, new SimpleCredentials("test", "test".toCharArray()));
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertTrue(lm.getSharedCredentials() instanceof SimpleCredentials);

        lm = initLoginModule(SimpleCredentials.class, sharedState);
        assertTrue(lm.getSharedCredentials() instanceof SimpleCredentials);

        sharedState.put(SHARED_KEY_CREDENTIALS, "no credentials object");
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getSharedCredentials());

        sharedState.clear();
        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getSharedCredentials());
    }

    @Test
    public void testGetCredentialsFromSharedState() {
        Map<String, Credentials> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_CREDENTIALS, new TestCredentials());

        AbstractLoginModule lm = initLoginModule(TestCredentials.class, sharedState);
        assertTrue(lm.getCredentials() instanceof TestCredentials);

        SimpleCredentials sc = new SimpleCredentials("test", "test".toCharArray());
        sharedState.put(SHARED_KEY_CREDENTIALS, sc);

        lm = initLoginModule(TestCredentials.class, sharedState);
        assertNull(lm.getCredentials());

        sharedState.put(SHARED_KEY_CREDENTIALS, sc);

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
        lm.initialize(subject, null, Collections.emptyMap(), null);

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

        AbstractLoginModule lm = initLoginModule(cbh);
        assertTrue(lm.getCredentials() instanceof TestCredentials);

        lm = new TestLoginModule(SimpleCredentials.class);
        lm.initialize(new Subject(), cbh, Collections.emptyMap(), null);
        assertNull(lm.getCredentials());
    }

    @Test
    public void testGetCredentialsIOException() {
        LoginModuleMonitor monitor = mock(LoginModuleMonitor.class);
        AbstractLoginModule lm = initLoginModule(new ThrowingCallbackHandler(true), monitor);
        assertNull(lm.getCredentials());
        verify(monitor, times(1)).loginError();
    }

    @Test
    public void testGetCredentialsUnsupportedCallbackException() {
        LoginModuleMonitor monitor = mock(LoginModuleMonitor.class);
        AbstractLoginModule lm = initLoginModule(new ThrowingCallbackHandler(false), monitor);
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
        AbstractLoginModule lm = initLoginModule(cbh);
        assertNull(lm.getCredentials());
    }

    @Test
    public void testGetCommitedCredentials() {
        AbstractLoginModule lm = mock(AbstractLoginModule.class);
        when(lm.getCommittedCredentials()).thenAnswer(Answers.CALLS_REAL_METHODS);
        assertNull(lm.getCommittedCredentials());
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
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler());

        assertNull(loginModule.getSecurityProvider());
        assertNull(loginModule.getRoot());
    }

    @Test
    public void testGetRoot() throws Exception {
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler(mockContentRepository(null), null));

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
        AbstractLoginModule lm = initLoginModule(new ThrowingCallbackHandler(true), stats);
        assertNull(lm.getRoot());
        assertEquals(1, stats.getLoginErrors());
    }

    @Test
    public void testGetRootUnsupportedCallbackException() {
        LoginModuleStats stats = newLoginModuleStats();
        AbstractLoginModule lm = initLoginModule(new ThrowingCallbackHandler(false), stats);
        assertNull(lm.getRoot());
        assertEquals(1, stats.getLoginErrors());
    }

    @Test
    public void testGetRootMissingCallbackHandler() {
        AbstractLoginModule loginModule = initLoginModule((CallbackHandler) null);
        assertNull(loginModule.getRoot());
    }

    @Test
    public void testGetSecurityProvider() {
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler(null, new OpenSecurityProvider()));

        SecurityProvider securityProvider = loginModule.getSecurityProvider();
        assertNotNull(securityProvider);
        // securityProvider is stored as field -> second access returns the same object
        assertSame(securityProvider, loginModule.getSecurityProvider());
    }

    @Test
    public void testGetSecurityProviderIOException() {
        AbstractLoginModule loginModule = initLoginModule(new ThrowingCallbackHandler(true));

        assertNull(loginModule.getSecurityProvider());
    }

    @Test
    public void testGetSecurityProviderUnsupportedCallbackException() {
        AbstractLoginModule loginModule = initLoginModule(new ThrowingCallbackHandler(false));

        assertNull(loginModule.getRoot());
    }

    @Test
    public void testGetSecurityProviderMissingCallbackHandler() {
        AbstractLoginModule loginModule = initLoginModule((CallbackHandler) null);
        assertNull(loginModule.getSecurityProvider());
    }

    @Test
    public void testGetWhiteboardFromCallback() {
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler(new DefaultWhiteboard()));

        Whiteboard wb = loginModule.getWhiteboard();
        assertNotNull(wb);
        // whiteboard is stored as field -> second access returns the same object
        assertSame(wb, loginModule.getWhiteboard());

    }

    @Test
    public void testGetWhiteboardFromIncompleteCallback() {
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler());

        assertNull(loginModule.getWhiteboard());
    }

    @Test
    public void testGetWhiteboardIOException() {
        AbstractLoginModule loginModule = initLoginModule(new ThrowingCallbackHandler(true));

        assertNull(loginModule.getWhiteboard());
    }

    @Test
    public void testGetWhiteboardUnsupportedCallbackException() {
        AbstractLoginModule loginModule = initLoginModule(new ThrowingCallbackHandler(false));

        assertNull(loginModule.getWhiteboard());
    }

    @Test
    public void testGetWhiteBoardMissingCallbackHandler() {
        AbstractLoginModule loginModule = initLoginModule((CallbackHandler) null);
        assertNull(loginModule.getWhiteboard());
    }

    @Test
    public void testGetUserManagerFromCallback() {
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler(mock(UserManager.class)));

        UserManager userManager = loginModule.getUserManager();
        assertNotNull(userManager);
        // usermanager is stored as field -> second access returns the same object
        assertSame(userManager, loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerFromIncompleteCallback() {
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler());

        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerIOException() {
        AbstractLoginModule loginModule = initLoginModule(new ThrowingCallbackHandler(true));

        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerUnsupportedCallbackException() {
        AbstractLoginModule loginModule = initLoginModule(new ThrowingCallbackHandler(false));

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

        CallbackHandler cbh = new TestCallbackHandler(cp, sp);
        AbstractLoginModule loginModule = initLoginModule(cbh);
        assertEquals(um, loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerMissingCallbackHandler() {
        AbstractLoginModule loginModule = initLoginModule((CallbackHandler) null);
        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerMissingRoot() throws Exception {
        ContentRepository cp = when(mock(ContentRepository.class).login(null, null)).thenReturn(mock(ContentSession.class)).getMock();

        CallbackHandler cbh = new TestCallbackHandler(cp, mock(SecurityProvider.class));
        AbstractLoginModule loginModule = initLoginModule(cbh);
        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetUserManagerMissingSecurityProvider() throws Exception {
        ContentSession cs = when(mock(ContentSession.class).getLatestRoot()).thenReturn(mock(Root.class)).getMock();
        ContentRepository cp = when(mock(ContentRepository.class).login(null, null)).thenReturn(cs).getMock();

        CallbackHandler cbh = new TestCallbackHandler(cp, null);
        AbstractLoginModule loginModule = initLoginModule(cbh);
        assertNull(loginModule.getUserManager());
    }

    @Test
    public void testGetPrincipalProviderFromCallback() {
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler(new TestPrincipalProvider()));

        assertNotNull(loginModule.getPrincipalProvider());

        PrincipalProvider principalProvider = loginModule.getPrincipalProvider();
        assertNotNull(principalProvider);
        // principalProvider is stored as field -> second access returns the same object
        assertSame(principalProvider, loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderFromIncompleteCallback() {
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler());

        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderIOException() {
        AbstractLoginModule loginModule = initLoginModule(new ThrowingCallbackHandler(true));

        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderUnsupportedCallbackException() {
        AbstractLoginModule loginModule = initLoginModule(new ThrowingCallbackHandler(false));

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

        CallbackHandler cbh = new TestCallbackHandler(cp, sp);
        AbstractLoginModule loginModule = initLoginModule(cbh);
        assertEquals(pp, loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderMissingCallbackHandler() {
        AbstractLoginModule loginModule = initLoginModule((CallbackHandler) null);
        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderMissingRoot() throws Exception {
        ContentRepository cp = when(mock(ContentRepository.class).login(null, null)).thenReturn(mock(ContentSession.class)).getMock();

        CallbackHandler cbh = new TestCallbackHandler(cp, mock(SecurityProvider.class));
        AbstractLoginModule loginModule = initLoginModule(cbh);
        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipalProviderMissingSecurityProvider() throws Exception {
        ContentSession cs = when(mock(ContentSession.class).getLatestRoot()).thenReturn(mock(Root.class)).getMock();
        ContentRepository cp = when(mock(ContentRepository.class).login(null, null)).thenReturn(cs).getMock();

        CallbackHandler cbh = new TestCallbackHandler(cp, null);
        AbstractLoginModule loginModule = initLoginModule(cbh);
        assertNull(loginModule.getPrincipalProvider());
    }

    @Test
    public void testGetPrincipals() {
        PrincipalProvider principalProvider = new TestPrincipalProvider();

        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler(principalProvider));

        Principal principal = principalProvider.findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP).next();
        String userId = TestPrincipalProvider.getIDFromPrincipal(principal);
        Set<? extends Principal> principals = loginModule.getPrincipals(userId);

        assertFalse(principals.isEmpty());
        assertEquals(principalProvider.getPrincipals(userId), principals);
    }


    @Test
    public void testGetPrincipalsMissingProvider() {
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler());

        Set<? extends Principal> principals = loginModule.getPrincipals("userId");
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetPrincipalsFromPrincipal() {
        PrincipalProvider principalProvider = new TestPrincipalProvider();

        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler(principalProvider));

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
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler());

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

        AbstractLoginModule lm = initLoginModule(cbh);
        assertTrue(lm.getLoginModuleMonitor() instanceof LoginModuleStats);
        lm.onError();
        assertEquals(1, stats.getLoginErrors());
    }

    @Test
    public void testNullLoginModuleMonitor() {
        LoginModuleStats stats = newLoginModuleStats();
        AbstractLoginModule lm = initLoginModule((CallbackHandler) null);
        assertNull(lm.getLoginModuleMonitor());
        lm.onError();
        assertEquals(0, stats.getLoginErrors());
    }

    @Test
    public void testErrorOnGetLoginModuleMonitor() {
        LoginModuleStats stats = newLoginModuleStats();
        AbstractLoginModule lm = initLoginModule(new ThrowingCallbackHandler(true));

        assertNull(lm.getLoginModuleMonitor());
        lm.onError();
        assertEquals(0, stats.getLoginErrors());
    }

    //--------------------------------------------------------------------------

    private class TestCredentials implements Credentials {}

    private static final class TestLoginModule extends AbstractLoginModule {

        private final Class supportedCredentialsClass;
        private final LoginModuleMonitor mon;

        private Credentials credentials;

        private TestLoginModule(@NotNull Class supportedCredentialsClass) {
            this(supportedCredentialsClass, null);
        }

        private TestLoginModule(@NotNull Class supportedCredentialsClass, @Nullable LoginModuleMonitor mon) {
            this.supportedCredentialsClass = supportedCredentialsClass;
            this.mon = mon;
        }

        private void loginAndCommit(@NotNull String userId) {
            credentials = getCredentials();
            Set<? extends Principal> principals = getPrincipals(userId);

            if (!subject.isReadOnly()) {
                if (credentials != null) {
                    subject.getPublicCredentials().add(credentials);
                }
                subject.getPrincipals().addAll(principals);
            }
        }

        @NotNull
        @Override
        protected Set<Class> getSupportedCredentials() {
            return Collections.singleton(supportedCredentialsClass);
        }

        @Override
        protected @Nullable Credentials getCommittedCredentials() {
            return credentials;
        }

        @Override
        public boolean login() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean commit() {
            throw new UnsupportedOperationException();
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

        private TestCallbackHandler(@Nullable ContentRepository contentRepository, @Nullable SecurityProvider securityProvider) {
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
                    rcb.setLoginModuleMonitor(LoginModuleMonitor.NOOP);
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }
        }
    }
}
