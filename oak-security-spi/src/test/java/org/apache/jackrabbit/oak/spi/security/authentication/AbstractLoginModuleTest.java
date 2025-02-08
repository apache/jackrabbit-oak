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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

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
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule.SHARED_KEY_CREDENTIALS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class AbstractLoginModuleTest {

    private final LoginModuleMonitor monitor = mock(LoginModuleMonitor.class);

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

    private static AbstractLoginModule initLoginModule(@NotNull Subject subject) {
        AbstractLoginModule lm = new TestLoginModule(TestCredentials.class);
        initialize(lm, subject, mock(CallbackHandler.class), Collections.emptyMap());
        return lm;
    }

    private static void initialize(@NotNull AbstractLoginModule loginModule, @NotNull Subject subject, @Nullable CallbackHandler cbh, @NotNull Map<String, ?> sharedState) {
        loginModule.initialize(subject, cbh, sharedState, null);
    }

    private static void setMonitor(@NotNull AbstractLoginModule loginModule, @NotNull LoginModuleMonitor monitor) throws Exception {
        Field f = AbstractLoginModule.class.getDeclaredField("loginModuleMonitor");
        f.setAccessible(true);
        f.set(loginModule, monitor);
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
        Map<String, String> options = Map.of("key", "value");
        lm.initialize(new Subject(), null, Collections.emptyMap(), options);

        assertNotSame(options, lm.options);
        assertEquals(options, lm.options);

        ConfigurationParameters options2 = ConfigurationParameters.of(options);
        lm.initialize(new Subject(), null, Collections.emptyMap(), options2);

        assertSame(options2, lm.options);
    }

    @Test
    public void testLogout() throws Exception {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, Map.of());

        assertFalse(loginModule.logout());
    }

    @Test
    public void testLogoutSuccessClearsSubject() throws Exception {
        Subject subject = new Subject(false, Set.of(new PrincipalImpl("pName")), Set.of(new TestCredentials()), Set.of());
        AbstractLoginModule loginModule = initLoginModule(subject);

        assertTrue(loginModule.logout());

        assertTrue(subject.getPublicCredentials().isEmpty());
        assertTrue(subject.getPrincipals().isEmpty());
    }

    @Test
    public void testLogoutSuccessReadOnlySubject() throws Exception {
        Subject subject = new Subject(true, Set.of(new PrincipalImpl("pName")), Set.of(new TestCredentials()), Set.of());
        AbstractLoginModule loginModule = initLoginModule(subject);

        assertTrue(loginModule.logout());

        assertFalse(subject.getPublicCredentials().isEmpty());
        assertFalse(subject.getPrincipals().isEmpty());
    }

    @Test
    public void testLogoutSubjectWithoutCredentials() throws Exception {
        Subject subject = new Subject(false, Set.of(new PrincipalImpl("pName")), Set.of("stringNotCredentials"), Set.of());
        AbstractLoginModule loginModule = initLoginModule(subject);
        loginModule.logout();

        assertFalse(subject.getPublicCredentials().isEmpty());
        assertFalse(subject.getPrincipals().isEmpty());

        subject = new Subject(false, Set.of(new PrincipalImpl("pName")), Set.of(), Set.of());
        loginModule = initLoginModule(subject);
        loginModule.logout();

        assertTrue(subject.getPublicCredentials().isEmpty());
        assertFalse(subject.getPrincipals().isEmpty());
    }

    @Test
    public void testLogoutSubjectWithoutPrincipals() throws Exception {
        Subject subject = new Subject(false, Set.of(), Set.of(new TestCredentials()), Set.of());
        AbstractLoginModule loginModule = initLoginModule(subject);
        loginModule.logout();

        assertFalse(subject.getPublicCredentials().isEmpty());
        assertTrue(subject.getPrincipals().isEmpty());
    }

    @Test
    public void testLogoutCPIgnored() throws LoginException {
        AbstractLoginModule loginModule = initLoginModule(new Subject());
        assertFalse(loginModule.logout(null, null));
    }

    @Test
    public void testLogoutCPSuccess() throws LoginException {
        PrincipalProvider pp = new TestPrincipalProvider("user");
        Principal p = pp.getPrincipal("user");
        assertNotNull(p);
        Principal foreignPrincipal = TestPrincipalProvider.UNKNOWN;

        String userId = TestPrincipalProvider.getIDFromPrincipal(p);
        Set<? extends Principal> principals = pp.getPrincipals(userId);
        Set<Principal> all = Stream.concat(Stream.of(p, foreignPrincipal), principals.stream()).collect(Collectors.toUnmodifiableSet());


        AuthInfo authInfo = new AuthInfoImpl(userId, null, all);
        Credentials foreign1 = new GuestCredentials();
        Credentials foreign2 = new TokenCredentials("token");

        Subject subject = new Subject(false,
                Set.of(foreignPrincipal, p),
                Set.of(authInfo, foreign1, foreign2), Set.of());

        TestLoginModule loginModule = new TestLoginModule(SimpleCredentials.class);
        loginModule.initialize(subject, new TestCallbackHandler(pp), Collections.emptyMap(), null);

        assertTrue(loginModule.logout(Set.of(authInfo), principals));

        Set<Object> publicCreds = subject.getPublicCredentials();
        assertFalse(publicCreds.contains(authInfo));
        assertTrue(publicCreds.contains(foreign1));
        assertTrue(publicCreds.contains(foreign2));

        assertFalse(subject.getPrincipals().contains(p));
        assertTrue(Collections.disjoint(subject.getPrincipals(), principals));
        assertTrue(subject.getPrincipals().contains(foreignPrincipal));
        verifyNoInteractions(monitor);
    }

    @Test
    public void testLogoutCPDestroyable() throws Exception {
        Credentials creds = mock(TestCredentials.class, withSettings().extraInterfaces(Destroyable.class));
        Credentials foreign1 = new GuestCredentials();
        Credentials foreign2 = new TokenCredentials("token");

        Subject subject = new Subject(true,
                Set.of(new PrincipalImpl("pName")),
                Set.of(creds, foreign1, foreign2), Set.of());

        AbstractLoginModule loginModule = initLoginModule(subject);
        assertTrue(loginModule.logout(Set.of(creds), Collections.emptySet()));

        verify(((Destroyable) creds), times(1)).destroy();
    }

    @Test(expected = LoginException.class)
    public void testLogoutCPDestroyFails() throws Exception {
        Credentials creds = mock(TestCredentials.class, withSettings().extraInterfaces(Destroyable.class));
        doThrow(new DestroyFailedException()).when((Destroyable)creds).destroy();

        Subject subject = new Subject(true, Set.of(), Set.of(creds), Set.of());
        AbstractLoginModule loginModule = initLoginModule(subject);
        loginModule.logout(Set.of(creds), null);
    }

    @Test
    public void testLogoutCPNotDestroyable() throws LoginException {
        Credentials creds = new TestCredentials();
        Subject subject = new Subject(true,
                Set.of(),
                Set.of(creds), Set.of());

        TestLoginModule loginModule = (TestLoginModule) initLoginModule(subject);
        assertTrue(loginModule.logout(null, Collections.emptySet()));
    }

    @Test
    public void testLogoutCPNullParams() throws LoginException {
        Credentials creds = new TestCredentials();
        Principal unknownPrincipal = TestPrincipalProvider.UNKNOWN;
        TestPrincipalProvider pp = new TestPrincipalProvider("principal");

        Subject subject = new Subject(false, Set.of(unknownPrincipal), Set.of(creds), Set.of());

        TestLoginModule loginModule = new TestLoginModule(SimpleCredentials.class);
        loginModule.initialize(subject, new TestCallbackHandler(pp), Collections.emptyMap(), null);

        assertFalse(loginModule.logout(null, null));
        // subject must not be altered by logout
        assertTrue(subject.getPublicCredentials().contains(creds));
        assertTrue(subject.getPrincipals().contains(unknownPrincipal));
        verifyNoInteractions(monitor);
    }

    @Test
    public void testLogoutCPMissingCredentials() throws LoginException {
        Credentials creds = new TestCredentials();
        Principal unknownPrincipal = TestPrincipalProvider.UNKNOWN;
        TestPrincipalProvider pp = new TestPrincipalProvider("principal");
        Principal p = pp.getPrincipal("principal");
        assertNotNull(p);

        Subject subject = new Subject(false, Set.of(unknownPrincipal, p), Set.of(creds), Set.of());

        TestLoginModule loginModule = new TestLoginModule(SimpleCredentials.class);
        loginModule.initialize(subject, new TestCallbackHandler(pp), Collections.emptyMap(), null);

        assertTrue(loginModule.logout(null, Set.of(p)));

        // only credentials/principals passed to logout must be removed
        assertTrue(subject.getPublicCredentials().contains(creds));
        assertTrue(subject.getPrincipals().contains(unknownPrincipal));
        assertFalse(subject.getPrincipals().contains(p));
        verifyNoInteractions(monitor);
    }

    @Test
    public void testLogoutCPMissingCredentialsReadOnly() throws LoginException {
        Credentials creds = new TestCredentials();
        Principal unknownPrincipal = TestPrincipalProvider.UNKNOWN;
        TestPrincipalProvider pp = new TestPrincipalProvider("principal");
        Principal p = pp.getPrincipal("principal");

        Set<? extends Principal> principals = Set.of(unknownPrincipal, p);
        AuthInfo authInfo = new AuthInfoImpl(null, null, principals);
        Subject subject = new Subject(true, principals, Set.of(creds, authInfo), Set.of());

        TestLoginModule loginModule = new TestLoginModule(SimpleCredentials.class);
        loginModule.initialize(subject, new TestCallbackHandler(pp), Collections.emptyMap(), null);

        assertTrue(loginModule.logout(Set.of(creds, authInfo), Collections.singleton(p)));
        // read-only subject must not be altered by logout
        assertTrue(subject.getPublicCredentials().contains(creds));
        assertTrue(subject.getPrincipals().contains(unknownPrincipal));
        assertTrue(subject.getPrincipals().contains(p));
        verifyNoInteractions(monitor);
    }

    @Test
    public void testLogoutCPMissingPrincipals() throws LoginException {
        Credentials creds = new TestCredentials();
        Principal preExisting = new PrincipalImpl("pName");

        Subject subject = new Subject(false, Set.of(preExisting), Set.of(creds), Set.of());
        TestLoginModule loginModule = (TestLoginModule) initLoginModule(subject);
        assertTrue(loginModule.logout(Set.of(creds), null));

        assertFalse(subject.getPublicCredentials().contains(creds));
        assertTrue(subject.getPrincipals().contains(preExisting));
    }

    @Test
    public void testAbort() throws LoginException {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, Map.of());

        assertTrue(loginModule.abort());
    }

    @Test
    public void testAbortWithFailedSystemLogout() throws Exception {
        ContentRepository cr = mockContentRepository(null);
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler(cr, null));

        // trigger creation of system-session
        loginModule.getRoot();
        assertTrue(loginModule.abort());
        verifyNoInteractions(monitor);
    }

    @Test
    public void testClearStateWithSessionCloseFailing() throws Exception {
        ContentSession cs = mock(ContentSession.class);
        ContentRepository cr = mockContentRepository(cs);
        doThrow(IOException.class).when(cs).close();

        CallbackHandler cbh = new TestCallbackHandler(cr, mock(SecurityProvider.class));

        AbstractLoginModule loginModule = initLoginModule(cbh);
        loginModule.getRoot();
        loginModule.clearState();
        verify(monitor).loginError();
        verifyNoMoreInteractions(monitor);
        verify(cs, times(1)).close();
    }

    @Test
    public void testCloseSystemSession() throws Exception {
        ContentSession cs = mock(ContentSession.class);
        ContentRepository cr = mockContentRepository(cs);

        CallbackHandler cbh = new TestCallbackHandler(cr, mock(SecurityProvider.class));

        AbstractLoginModule loginModule = initLoginModule(cbh);
        loginModule.getRoot();
        loginModule.closeSystemSession();
        verify(cs, times(1)).close();
        verifyNoInteractions(monitor);
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
        lm.initialize(subject, null, Map.of(), null);

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
    public void testGetCredentialsIOException() throws Exception {
        AbstractLoginModule lm = initLoginModule(new ThrowingCallbackHandler(true));
        setMonitor(lm, monitor);

        assertNull(lm.getCredentials());
        verify(monitor, times(1)).loginError();
        verifyNoMoreInteractions(monitor);
    }

    @Test
    public void testGetCredentialsUnsupportedCallbackException() throws Exception {
        AbstractLoginModule lm = initLoginModule(new ThrowingCallbackHandler(false));
        setMonitor(lm, monitor);

        assertNull(lm.getCredentials());
        verify(monitor, times(1)).loginError();
        verifyNoMoreInteractions(monitor);
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
    public void testGetSharedPreAuthLoginEmptySharedState() {
        AbstractLoginModule loginModule = initLoginModule(TestCredentials.class, Map.of());
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
        verifyNoInteractions(monitor);
    }

    @Test
    public void testGetRootIOException() throws Exception {
        AbstractLoginModule lm = initLoginModule(new ThrowingCallbackHandler(true));
        setMonitor(lm, monitor);

        assertNull(lm.getRoot());
        verify(monitor).loginError();
        verifyNoMoreInteractions(monitor);
    }

    @Test
    public void testGetRootUnsupportedCallbackException() throws Exception {
        AbstractLoginModule lm = initLoginModule(new ThrowingCallbackHandler(false));
        setMonitor(lm, monitor);

        assertNull(lm.getRoot());
        verify(monitor).loginError();
        verifyNoMoreInteractions(monitor);
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
        verifyNoInteractions(monitor);
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
        verifyNoInteractions(monitor);
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
        verifyNoInteractions(monitor);
    }

    @Test
    public void testGetUserManagerMissingSecurityProvider() throws Exception {
        ContentSession cs = when(mock(ContentSession.class).getLatestRoot()).thenReturn(mock(Root.class)).getMock();
        ContentRepository cp = when(mock(ContentRepository.class).login(null, null)).thenReturn(cs).getMock();

        CallbackHandler cbh = new TestCallbackHandler(cp, null);
        AbstractLoginModule loginModule = initLoginModule(cbh);
        assertNull(loginModule.getUserManager());
        verifyNoInteractions(monitor);
    }

    @Test
    public void testGetPrincipalProviderFromCallback() {
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler(new TestPrincipalProvider()));

        assertNotNull(loginModule.getPrincipalProvider());

        PrincipalProvider principalProvider = loginModule.getPrincipalProvider();
        assertNotNull(principalProvider);
        // principalProvider is stored as field -> second access returns the same object
        assertSame(principalProvider, loginModule.getPrincipalProvider());
        verifyNoInteractions(monitor);
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
        verifyNoInteractions(monitor);
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
        verifyNoInteractions(monitor);
    }

    @Test
    public void testGetPrincipalProviderMissingSecurityProvider() throws Exception {
        ContentSession cs = when(mock(ContentSession.class).getLatestRoot()).thenReturn(mock(Root.class)).getMock();
        ContentRepository cp = when(mock(ContentRepository.class).login(null, null)).thenReturn(cs).getMock();

        CallbackHandler cbh = new TestCallbackHandler(cp, null);
        AbstractLoginModule loginModule = initLoginModule(cbh);
        assertNull(loginModule.getPrincipalProvider());
        verifyNoInteractions(monitor);
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
        verify(monitor).principalsCollected(anyLong(), anyInt());
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
        verify(monitor).principalsCollected(anyLong(), anyInt());
    }

    @Test
    public void testGetPrincipalsFromPrincipalMissingProvider() {
        AbstractLoginModule loginModule = initLoginModule(new TestCallbackHandler());

        Set<? extends Principal> principals = loginModule.getPrincipals(new PrincipalImpl("principalName"));
        assertTrue(principals.isEmpty());
        verifyNoInteractions(monitor);
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
        CallbackHandler cbh = callbacks -> {
            for (Callback cb : callbacks) {
                if (cb instanceof RepositoryCallback) {
                    ((RepositoryCallback) cb).setLoginModuleMonitor(monitor);
                }
            }
        };

        AbstractLoginModule lm = initLoginModule(cbh);
        assertSame(monitor, lm.getLoginModuleMonitor());
        lm.onError();
        verify(monitor).loginError();
        verifyNoMoreInteractions(monitor);
    }

    @Test
    public void testLoginModuleMonitorMissingCallback() {
        AbstractLoginModule lm = initLoginModule((CallbackHandler) null);
        assertSame(LoginModuleMonitor.NOOP, lm.getLoginModuleMonitor());
        lm.onError();
        verifyNoInteractions(monitor);
    }

    @Test
    public void testErrorOnGetLoginModuleMonitor() {
        AbstractLoginModule lm = initLoginModule(new ThrowingCallbackHandler(true));

        assertSame(LoginModuleMonitor.NOOP, lm.getLoginModuleMonitor());
        lm.onError();
        verifyNoInteractions(monitor);
    }

    //--------------------------------------------------------------------------

    private static class TestCredentials implements Credentials {}

    private static final class TestLoginModule extends AbstractLoginModule {

        private final Class<?> supportedCredentialsClass;

        private TestLoginModule(@NotNull Class<?> supportedCredentialsClass) {
            this.supportedCredentialsClass = supportedCredentialsClass;
        }

        @NotNull
        @Override
        protected Set<Class> getSupportedCredentials() {
            return Collections.singleton(supportedCredentialsClass);
        }

        @Override
        public boolean login() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean commit() {
            throw new UnsupportedOperationException();
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
                    rcb.setLoginModuleMonitor(monitor);
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }
        }
    }
}
