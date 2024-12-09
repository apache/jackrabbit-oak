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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMonitor;
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.RepositoryCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.WhiteboardCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.monitor.ExternalIdentityMonitor;
import org.apache.jackrabbit.oak.spi.security.principal.EmptyPrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.api.CommitFailedException.OAK;
import static org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule.SHARED_KEY_ATTRIBUTES;
import static org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule.SHARED_KEY_PRE_AUTH_LOGIN;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.DEFAULT_IDP_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.ID_EXCEPTION;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.ID_TEST_USER;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_IDP_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME;
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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class ExternalLoginModuleTest extends AbstractSecurityTest {

    private final ExternalLoginModule loginModule = new ExternalLoginModule();

    private final Whiteboard wb = spy(new DefaultWhiteboard());
    private final ExternalIdentityProviderManager extIPMgr = mock(ExternalIdentityProviderManager.class);
    private final SyncManager syncManager = mock(SyncManager.class);
    private final LoginModuleMonitor monitor = mock(LoginModuleMonitor.class);
    private final ExternalIdentityMonitor externalIdentityMonitor = mock(ExternalIdentityMonitor.class);

    @Before
    public void before() throws Exception {
        super.before();
        wb.register(ExternalIdentityMonitor.class, externalIdentityMonitor, Collections.emptyMap());
    }

    @After
    public void after() throws Exception {
        try {
            clearInvocations(externalIdentityMonitor, monitor, syncManager, extIPMgr);
        } finally {
            super.after();
        }
    }

    private CallbackHandler createCallbackHandler(@Nullable Whiteboard wb, @Nullable ContentRepository contentRepository, @Nullable SecurityProvider securityProvider, @Nullable Credentials creds) {
        return callbacks -> {
            for (Callback cb : callbacks) {
                if (cb instanceof WhiteboardCallback) {
                    ((WhiteboardCallback) cb).setWhiteboard(wb);
                } else if (cb instanceof RepositoryCallback) {
                    ((RepositoryCallback) cb).setContentRepository(contentRepository);
                    ((RepositoryCallback) cb).setSecurityProvider(securityProvider);
                    ((RepositoryCallback) cb).setLoginModuleMonitor(monitor);
                } else if (cb instanceof CredentialsCallback) {
                    ((CredentialsCallback) cb).setCredentials(creds);
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }
        };
    }

    private void verifySyncException(@NotNull Exception e) {
        assertTrue(e.getCause() instanceof SyncException);
        verify(externalIdentityMonitor).syncFailed((SyncException) e.getCause());
        verifyNoMoreInteractions(externalIdentityMonitor);
    }

    @Test
    public void testInitializeMissingWhiteboard() throws LoginException {
        loginModule.setIdpManager(extIPMgr);
        loginModule.setSyncManager(syncManager);

        CallbackHandler cbh = mock(CallbackHandler.class);
        loginModule.initialize(new Subject(), cbh, Collections.emptyMap(), Map.of(PARAM_IDP_NAME, "idp", PARAM_SYNC_HANDLER_NAME, "syncHandler"));

        verify(extIPMgr, never()).getProvider("idp");
        verify(syncManager, never()).getSyncHandler("syncHandler");
        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());
        verifyNoInteractions(monitor);
    }

    @Test
    public void testInitializeMissingIdpSyncHandler() throws LoginException {
        loginModule.initialize(new Subject(), createCallbackHandler(wb, null, null, null), Collections.emptyMap(), Map.of(PARAM_IDP_NAME, "idp", PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());
        verifyNoInteractions(monitor);
    }

    @Test
    public void testInitializedUnknownIdpSyncHandlerNames() throws LoginException {
        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        loginModule.initialize(new Subject(), createCallbackHandler(wb, null, null, null), Collections.emptyMap(), Map.of(PARAM_IDP_NAME, "idp", PARAM_SYNC_HANDLER_NAME, "syncHandler"));

        verify(extIPMgr, times(1)).getProvider("idp");
        verify(syncManager, times(1)).getSyncHandler("syncHandler");

        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());
        verifyNoInteractions(monitor);
    }

    @Test
    public void testInitializEmptyIdpName() {
        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());

        loginModule.initialize(new Subject(), createCallbackHandler(wb, null, null, null), Collections.emptyMap(), Collections.singletonMap(PARAM_IDP_NAME, ""));
        verify(extIPMgr, never()).getProvider("");
        verifyNoInteractions(monitor);
    }

    @Test
    public void testInitializEmptySyncHanderName() {
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        loginModule.initialize(new Subject(), createCallbackHandler(wb, null, null, null), Collections.emptyMap(), Collections.singletonMap(PARAM_SYNC_HANDLER_NAME, ""));
        verify(syncManager, never()).getSyncHandler("");
        verifyNoInteractions(monitor);
    }


    @Test
    public void testLoginWithoutInit() throws Exception {
        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());
        verifyNoInteractions(monitor);
    }

    @Test
    public void testLoginMissingSyncHandler() throws Exception {
        when(extIPMgr.getProvider("idpName")).thenReturn(new TestIdentityProvider());
        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());

        loginModule.initialize(new Subject(), createCallbackHandler(wb, null, null, null), Collections.emptyMap(), Collections.singletonMap(PARAM_IDP_NAME, "idpName"));
        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());
        verifyNoInteractions(monitor);
    }

    @Test
    public void testLoginMissingUserId() throws Exception {
        ExternalIdentityProvider idp = mock(ExternalIdentityProvider.class, withSettings().extraInterfaces(CredentialsSupport.class));
        when(idp.getName()).thenReturn(DEFAULT_IDP_NAME);
        when(((CredentialsSupport) idp).getUserId(any(Credentials.class))).thenReturn(null);
        when(((CredentialsSupport) idp).getCredentialClasses()).thenReturn(Set.of(GuestCredentials.class));

        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(idp);
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), new GuestCredentials());

        loginModule.initialize(new Subject(), cbh, new HashMap<>(), Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertFalse(loginModule.login());
        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());
        verifyNoInteractions(monitor);
    }

    @Test
    public void testLoginCommitUpdatesSubject() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Map<String,Object> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(ID_TEST_USER));
        sharedState.put(SHARED_KEY_ATTRIBUTES, Collections.singletonMap("att", "value"));

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), null);

        Subject subject = new Subject();
        loginModule.initialize(subject, cbh, sharedState, Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        AuthInfo authInfo = subject.getPublicCredentials(AuthInfo.class).iterator().next();
        assertEquals("value", authInfo.getAttribute("att"));

        root.refresh();
        assertNotNull(getUserManager(root).getAuthorizable(ID_TEST_USER));

        assertTrue(loginModule.logout());
        assertTrue(subject.getPublicCredentials().isEmpty());

        verify(monitor).principalsCollected(anyLong(), anyInt());
        verifyNoMoreInteractions(monitor);
    }

    @Test
    public void testLoginCommitEmptyPrincipalSet() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Map<String,Object> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(ID_TEST_USER));
        sharedState.put(SHARED_KEY_ATTRIBUTES, Collections.singletonMap("att", "value"));

        PrincipalConfiguration pc = when(mock(PrincipalConfiguration.class).getPrincipalProvider(any(Root.class), any(NamePathMapper.class))).thenReturn(EmptyPrincipalProvider.INSTANCE).getMock();
        SecurityProvider sp = spy(getSecurityProvider());
        when(sp.getConfiguration(PrincipalConfiguration.class)).thenReturn(pc);

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), sp, null);

        Principal principal = new PrincipalImpl("preset");
        Subject subject = new Subject();
        subject.getPrincipals().add(principal);

        loginModule.initialize(subject, cbh, sharedState, Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        assertTrue(subject.getPrincipals().contains(principal));
        assertFalse(subject.getPublicCredentials(AuthInfo.class).isEmpty());
        AuthInfo info = subject.getPublicCredentials(AuthInfo.class).iterator().next();
        assertTrue(info.getPrincipals().contains(principal));

        assertTrue(loginModule.logout());

        // authinfo must be removed upon logout
        assertTrue(subject.getPublicCredentials(AuthInfo.class).isEmpty());
        // predefined principal must _not_ be removed
        assertTrue(subject.getPrincipals().contains(principal));

        verify(monitor).principalsCollected(anyLong(), anyInt());
        verifyNoMoreInteractions(monitor);
    }

    @Test
    public void testLoginCommitImpersonationCredentials() throws Exception {
        SimpleCredentials sc = new SimpleCredentials(ID_TEST_USER, new char[0]);
        ImpersonationCredentials creds = new ImpersonationCredentials(sc, AuthInfo.EMPTY);
        ExternalIdentityProvider idp = mock(ExternalIdentityProvider.class, withSettings().extraInterfaces(CredentialsSupport.class));
        when(idp.getName()).thenReturn(DEFAULT_IDP_NAME);
        when(idp.authenticate(creds)).thenReturn(new TestIdentityProvider.TestUser(ID_TEST_USER, DEFAULT_IDP_NAME));
        when(((CredentialsSupport) idp).getUserId(any(Credentials.class))).thenReturn(ID_TEST_USER);
        when(((CredentialsSupport) idp).getCredentialClasses()).thenReturn(Set.of(ImpersonationCredentials.class));
        Map attr = Map.of("attr","value");
        when(((CredentialsSupport) idp).getAttributes(creds)).thenReturn(attr);
        when(((CredentialsSupport) idp).getAttributes(sc)).thenReturn(Collections.emptyMap());

        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(idp);
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), creds);

        Subject subject = new Subject();
        loginModule.initialize(subject, cbh, new HashMap<>(), Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        AuthInfo authInfo = subject.getPublicCredentials(AuthInfo.class).iterator().next();
        assertNull(authInfo.getAttribute("attr"));

        assertTrue(loginModule.logout());
        assertTrue(subject.getPublicCredentials().isEmpty());
        assertTrue(subject.getPrincipals().isEmpty());

        verify(monitor).principalsCollected(anyLong(), anyInt());
        verifyNoMoreInteractions(monitor);
    }

    @Test
    public void testLoginCommitReadonlySubject() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Map<String,Object> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(ID_TEST_USER));
        sharedState.put(SHARED_KEY_ATTRIBUTES, Collections.singletonMap("att", "value"));

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), null);

        Subject readOnly = new Subject(true, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        loginModule.initialize(readOnly, cbh, sharedState, Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        root.refresh();
        assertNotNull(getUserManager(root).getAuthorizable(ID_TEST_USER));

        assertTrue(loginModule.logout());

        verify(monitor).principalsCollected(anyLong(), anyInt());
        verifyNoMoreInteractions(monitor);
    }

    @Test(expected = LoginException.class)
    public void testSyncUserMissingRoot() throws LoginException {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Map<String,Object> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(ID_TEST_USER));

        loginModule.initialize(new Subject(), createCallbackHandler(wb, null, null, null), sharedState, Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        try {
            loginModule.login();
        } catch (LoginException e) {
            verifySyncException(e);
            throw e;
        } finally {
            verify(monitor, times(2)).loginError();
            verifyNoMoreInteractions(monitor);
        }
    }

    @Test(expected = LoginException.class)
    public void testSyncUserMissingUserManager() throws LoginException {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        UserConfiguration uc = mock(UserConfiguration.class);
        SecurityProvider sp = when(mock(SecurityProvider.class).getConfiguration(UserConfiguration.class)).thenReturn(uc).getMock();

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), sp, null);

        Map<String,Object> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(ID_TEST_USER));

        loginModule.initialize(new Subject(), cbh, sharedState, Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        try {
            loginModule.login();
        } catch (LoginException e) {
            verifySyncException(e);
            throw e;
        } finally {
            verify(monitor, times(2)).loginError();
            verifyNoMoreInteractions(monitor);
        }
    }

    @Test(expected = LoginException.class)
    public void testSyncUserFailsCommit() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        ContentRepository repository = spy(getContentRepository());
        ContentSession s = spy(adminSession);
        Root r = spy(root);
        doThrow(new CommitFailedException(OAK, -1, "error")).when(r).commit();
        when(r.hasPendingChanges()).thenReturn(true);
        doReturn(s).when(repository).login(null, null);
        when(s.getLatestRoot()).thenReturn(r);

        CallbackHandler cbh = createCallbackHandler(wb, repository, getSecurityProvider(), null);

        Map<String,Object> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(ID_TEST_USER));

        loginModule.initialize(new Subject(), cbh, sharedState, Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        try {
            loginModule.login();
        } catch (LoginException e) {
            verifySyncException(e);
            throw e;
        } finally {
            verify(monitor).loginError();
            verifyNoMoreInteractions(monitor);
        }
    }

    @Test
    public void testValidateUser() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Map<String,Object> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin("local"));

        User u = getUserManager(root).createUser("local", null);
        u.setProperty(REP_EXTERNAL_ID, getValueFactory().createValue("local;"+TestIdentityProvider.DEFAULT_IDP_NAME));
        root.commit();

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), null);

        loginModule.initialize(new Subject(), cbh, sharedState, Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertFalse(loginModule.login());
        root.refresh();
        assertNull(getUserManager(root).getAuthorizable("local"));

        assertFalse(loginModule.commit());
        assertFalse(loginModule.logout());

        verify(externalIdentityMonitor).doneSyncId(anyLong(), any(SyncResult.class));
        verifyNoInteractions(monitor);
    }

    @Test(expected = LoginException.class)
    public void testValidateUserFailsCommit() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Map<String,Object> sharedState = new HashMap<>();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin("local"));

        User u = getUserManager(root).createUser("local", null);
        u.setProperty(REP_EXTERNAL_ID, getValueFactory().createValue("local;"+TestIdentityProvider.DEFAULT_IDP_NAME));

        ContentRepository repository = spy(getContentRepository());
        ContentSession s = spy(adminSession);
        Root r = spy(root);
        doThrow(new CommitFailedException(OAK, -1, "error")).when(r).commit();
        when(r.hasPendingChanges()).thenReturn(true);
        doReturn(s).when(repository).login(null, null);
        when(s.getLatestRoot()).thenReturn(r);

        CallbackHandler cbh = createCallbackHandler(wb, repository, getSecurityProvider(), null);

        loginModule.initialize(new Subject(), cbh, sharedState, Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        try {
            loginModule.login();
        } catch (LoginException e) {
            verifySyncException(e);
            throw e;
        } finally {
            verify(monitor).loginError();
            verifyNoMoreInteractions(monitor);
        }
    }

    @Test(expected = LoginException.class)
    public void testLoginFailed() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Credentials credentials = new SimpleCredentials(ID_TEST_USER, "wrongpassword".toCharArray());
        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), credentials);

        loginModule.initialize(new Subject(), cbh, new HashMap<>(), Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        try {
            loginModule.login();
        } finally {
            verify(monitor).loginFailed(any(LoginException.class), eq(credentials));
            verifyNoMoreInteractions(monitor);
        }
    }

    @Test
    public void testSyncUser() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Credentials crds = new SimpleCredentials("testUser", new char[0]);
        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), crds);

        loginModule.initialize(new Subject(), cbh, new HashMap<>(), Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertTrue(loginModule.login());
        root.refresh();
        Authorizable a = getUserManager(root).getAuthorizable("testUser");
        assertNotNull(a);
        assertTrue(a.hasProperty(REP_EXTERNAL_ID));

        assertTrue(loginModule.commit());
        assertTrue(loginModule.logout());

        verify(externalIdentityMonitor).doneSyncExternalIdentity(anyLong(), any(SyncResult.class), anyInt());
        verify(monitor).principalsCollected(anyLong(), anyInt());
        verifyNoMoreInteractions(externalIdentityMonitor, monitor);
    }
    
    @Test
    public void testGetSyncedIdentityFails() throws Exception {
        SyncHandler sh = when(mock(SyncHandler.class).findIdentity(any(UserManager.class), anyString())).thenThrow(new RepositoryException()).getMock();
        SyncManager syncManager = when(mock(SyncManager.class).getSyncHandler(anyString())).thenReturn(sh).getMock();
        when(extIPMgr.getProvider(anyString())).thenReturn(new TestIdentityProvider());
        
        ExternalLoginModule lm = new ExternalLoginModule();
        lm.setIdpManager(extIPMgr);
        lm.setSyncManager(syncManager);

        Credentials crds = new SimpleCredentials("testUser", new char[0]);
        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), crds);
        lm.initialize(new Subject(), cbh, new HashMap<>(), 
                Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        
        try {
            lm.login();
            fail();
        } catch (LoginException e) {
            assertEquals("Error while obtaining synced identity.", e.getMessage());
            verify(monitor).loginError();
        }
    }

    @Test
    public void testGetExternalIdentityFails() throws Exception {
        SyncManager syncManager = when(mock(SyncManager.class).getSyncHandler(anyString())).thenReturn(new DefaultSyncHandler()).getMock();
        when(extIPMgr.getProvider(anyString())).thenReturn(new TestIdentityProvider());

        ExternalLoginModule lm = new ExternalLoginModule();
        lm.setIdpManager(extIPMgr);
        lm.setSyncManager(syncManager);

        Credentials crds = new SimpleCredentials(ID_EXCEPTION, new char[0]);
        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), crds);
        lm.initialize(new Subject(), cbh, new HashMap<>(),
                Map.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));

        try {
            assertFalse(lm.login());
        } finally {
            verify(monitor).loginError();
        }
    }
}