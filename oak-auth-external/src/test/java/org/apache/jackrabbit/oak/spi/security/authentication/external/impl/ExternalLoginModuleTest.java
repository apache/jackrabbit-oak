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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.CredentialsCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.RepositoryCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.WhiteboardCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProviderManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EmptyPrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.jackrabbit.oak.api.CommitFailedException.OAK;
import static org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule.SHARED_KEY_ATTRIBUTES;
import static org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule.SHARED_KEY_PRE_AUTH_LOGIN;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.DEFAULT_IDP_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.ID_TEST_USER;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_IDP_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class ExternalLoginModuleTest extends AbstractSecurityTest {

    private final ExternalLoginModule loginModule = new ExternalLoginModule();

    private final Whiteboard wb = spy(new DefaultWhiteboard());
    private final ExternalIdentityProviderManager extIPMgr = mock(ExternalIdentityProviderManager.class);
    private final SyncManager syncManager = mock(SyncManager.class);

    private CallbackHandler createCallbackHandler(@Nullable Whiteboard wb, @Nullable ContentRepository contentRepository, @Nullable SecurityProvider securityProvider, @Nullable Credentials creds) {
        return callbacks -> {
            for (Callback cb : callbacks) {
                if (cb instanceof WhiteboardCallback) {
                    ((WhiteboardCallback) cb).setWhiteboard(wb);
                } else if (cb instanceof RepositoryCallback) {
                    ((RepositoryCallback) cb).setContentRepository(contentRepository);
                    ((RepositoryCallback) cb).setSecurityProvider(securityProvider);
                } else if (cb instanceof CredentialsCallback) {
                    ((CredentialsCallback) cb).setCredentials(creds);
                }else {
                    throw new UnsupportedCallbackException(cb);
                }
            }
        };
    }

    @Test
    public void testInitializeMissingWhiteboard() throws LoginException {
        loginModule.setIdpManager(extIPMgr);
        loginModule.setSyncManager(syncManager);

        CallbackHandler cbh = mock(CallbackHandler.class);
        loginModule.initialize(new Subject(), cbh, Collections.emptyMap(), ImmutableMap.of(PARAM_IDP_NAME, "idp", PARAM_SYNC_HANDLER_NAME, "syncHandler"));

        verify(extIPMgr, never()).getProvider("idp");
        verify(syncManager, never()).getSyncHandler("syncHandler");
        assertFalse(loginModule.login());
    }

    @Test
    public void testInitializeMissingIdpSyncHandler() throws LoginException {
        loginModule.initialize(new Subject(), createCallbackHandler(wb, null, null, null), Collections.emptyMap(), ImmutableMap.of(PARAM_IDP_NAME, "idp", PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertFalse(loginModule.login());
    }

    @Test
    public void testInitializedUnknownIdpSyncHandlerNames() throws LoginException {
        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        loginModule.initialize(new Subject(), createCallbackHandler(wb, null, null, null), Collections.emptyMap(), ImmutableMap.of(PARAM_IDP_NAME, "idp", PARAM_SYNC_HANDLER_NAME, "syncHandler"));

        verify(extIPMgr, times(1)).getProvider("idp");
        verify(syncManager, times(1)).getSyncHandler("syncHandler");
        assertFalse(loginModule.login());
    }

    @Test
    public void testInitializEmptyIdpName() {
        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());

        loginModule.initialize(new Subject(), createCallbackHandler(wb, null, null, null), Collections.emptyMap(), Collections.singletonMap(PARAM_IDP_NAME, ""));
        verify(extIPMgr, never()).getProvider("");
    }

    @Test
    public void testInitializEmptySyncHanderName() {
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        loginModule.initialize(new Subject(), createCallbackHandler(wb, null, null, null), Collections.emptyMap(), Collections.singletonMap(PARAM_SYNC_HANDLER_NAME, ""));
        verify(syncManager, never()).getSyncHandler("");
    }


    @Test
    public void testLoginWithoutInit() throws Exception {
        assertFalse(loginModule.login());
    }

    @Test
    public void testLoginMissingSyncHandler() throws Exception {
        when(extIPMgr.getProvider("idpName")).thenReturn(new TestIdentityProvider());
        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());

        loginModule.initialize(new Subject(), createCallbackHandler(wb, null, null, null), Collections.emptyMap(), Collections.singletonMap(PARAM_IDP_NAME, "idpName"));
        assertFalse(loginModule.login());
    }

    @Test
    public void testLoginMissingUserId() throws Exception {
        ExternalIdentityProvider idp = mock(ExternalIdentityProvider.class, withSettings().extraInterfaces(CredentialsSupport.class));
        when(idp.getName()).thenReturn(DEFAULT_IDP_NAME);
        when(((CredentialsSupport) idp).getUserId(any(Credentials.class))).thenReturn(null);
        when(((CredentialsSupport) idp).getCredentialClasses()).thenReturn(ImmutableSet.of(GuestCredentials.class));

        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(idp);
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), new GuestCredentials());

        loginModule.initialize(new Subject(), cbh, new HashMap<>(), ImmutableMap.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertFalse(loginModule.login());
    }

    @Test
    public void testLoginCommitUpdatesSubject() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Map<String,Object> sharedState = Maps.newHashMap();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(ID_TEST_USER));
        sharedState.put(SHARED_KEY_ATTRIBUTES, Collections.singletonMap("att", "value"));

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), null);

        Subject subject = new Subject();
        loginModule.initialize(subject, cbh, sharedState, ImmutableMap.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        AuthInfo authInfo = subject.getPublicCredentials(AuthInfo.class).iterator().next();
        assertEquals("value", authInfo.getAttribute("att"));

        root.refresh();
        assertNotNull(getUserManager(root).getAuthorizable(ID_TEST_USER));
    }

    @Test
    public void testLoginCommitEmptyPrincipalSet() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Map<String,Object> sharedState = Maps.newHashMap();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(ID_TEST_USER));
        sharedState.put(SHARED_KEY_ATTRIBUTES, Collections.singletonMap("att", "value"));

        PrincipalConfiguration pc = when(mock(PrincipalConfiguration.class).getPrincipalProvider(any(Root.class), any(NamePathMapper.class))).thenReturn(EmptyPrincipalProvider.INSTANCE).getMock();
        SecurityProvider sp = spy(getSecurityProvider());
        when(sp.getConfiguration(PrincipalConfiguration.class)).thenReturn(pc);

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), sp, null);

        Subject subject = new Subject();
        loginModule.initialize(subject, cbh, sharedState, ImmutableMap.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertTrue(loginModule.login());
        assertFalse(loginModule.commit());

        assertFalse(subject.getPublicCredentials(AuthInfo.class).iterator().hasNext());
        assertTrue(subject.getPrincipals().isEmpty());
    }

    @Test
    public void testLoginCommitImpersonationCredentials() throws Exception {
        SimpleCredentials sc = new SimpleCredentials(ID_TEST_USER, new char[0]);
        ImpersonationCredentials creds = new ImpersonationCredentials(sc, AuthInfo.EMPTY);
        ExternalIdentityProvider idp = mock(ExternalIdentityProvider.class, withSettings().extraInterfaces(CredentialsSupport.class));
        when(idp.getName()).thenReturn(DEFAULT_IDP_NAME);
        when(idp.authenticate(creds)).thenReturn(new TestIdentityProvider.TestUser(ID_TEST_USER, DEFAULT_IDP_NAME));
        when(((CredentialsSupport) idp).getUserId(any(Credentials.class))).thenReturn(ID_TEST_USER);
        when(((CredentialsSupport) idp).getCredentialClasses()).thenReturn(ImmutableSet.of(ImpersonationCredentials.class));
        Map attr = ImmutableMap.of("attr","value");
        when(((CredentialsSupport) idp).getAttributes(creds)).thenReturn(attr);
        when(((CredentialsSupport) idp).getAttributes(sc)).thenReturn(Collections.emptyMap());

        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(idp);
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), creds);

        Subject subject = new Subject();
        loginModule.initialize(subject, cbh, Maps.newHashMap(), ImmutableMap.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        AuthInfo authInfo = subject.getPublicCredentials(AuthInfo.class).iterator().next();
        assertNull(authInfo.getAttribute("attr"));
    }

    @Test
    public void testLoginCommitReadonlySubject() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Map<String,Object> sharedState = Maps.newHashMap();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(ID_TEST_USER));
        sharedState.put(SHARED_KEY_ATTRIBUTES, Collections.singletonMap("att", "value"));

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), null);

        Subject readOnly = new Subject(true, Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
        loginModule.initialize(readOnly, cbh, sharedState, ImmutableMap.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertTrue(loginModule.login());
        assertTrue(loginModule.commit());

        root.refresh();
        assertNotNull(getUserManager(root).getAuthorizable(ID_TEST_USER));
    }

    @Test(expected = LoginException.class)
    public void testSyncUserMissingRoot() throws LoginException {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Map<String,Object> sharedState = Maps.newHashMap();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(ID_TEST_USER));

        loginModule.initialize(new Subject(), createCallbackHandler(wb, null, null, null), sharedState, ImmutableMap.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        try {
            loginModule.login();
        } catch (LoginException e) {
            assertTrue(e.getCause() instanceof SyncException);
            throw e;
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

        Map<String,Object> sharedState = Maps.newHashMap();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(ID_TEST_USER));

        loginModule.initialize(new Subject(), cbh, sharedState, ImmutableMap.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        try {
            loginModule.login();
        } catch (LoginException e) {
            assertTrue(e.getCause() instanceof SyncException);
            throw e;
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

        Map<String,Object> sharedState = Maps.newHashMap();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin(ID_TEST_USER));

        loginModule.initialize(new Subject(), cbh, sharedState, ImmutableMap.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        try {
            loginModule.login();
        } catch (LoginException e) {
            assertTrue(e.getCause() instanceof SyncException);
            throw e;
        }
    }

    @Test
    public void testValidateUser() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Map<String,Object> sharedState = Maps.newHashMap();
        sharedState.put(SHARED_KEY_PRE_AUTH_LOGIN, new PreAuthenticatedLogin("local"));

        User u = getUserManager(root).createUser("local", null);
        u.setProperty(REP_EXTERNAL_ID, getValueFactory().createValue("local;"+TestIdentityProvider.DEFAULT_IDP_NAME));
        root.commit();

        CallbackHandler cbh = createCallbackHandler(wb, getContentRepository(), getSecurityProvider(), null);

        loginModule.initialize(new Subject(), cbh, sharedState, ImmutableMap.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        assertFalse(loginModule.login());
        root.refresh();
        assertNull(getUserManager(root).getAuthorizable("local"));
    }

    @Test(expected = LoginException.class)
    public void testValidateUserFailsCommit() throws Exception {
        when(extIPMgr.getProvider(DEFAULT_IDP_NAME)).thenReturn(new TestIdentityProvider());
        when(syncManager.getSyncHandler("syncHandler")).thenReturn(new DefaultSyncHandler(new DefaultSyncConfigImpl().setName("syncHandler")));

        wb.register(ExternalIdentityProviderManager.class, extIPMgr, Collections.emptyMap());
        wb.register(SyncManager.class, syncManager, Collections.emptyMap());

        Map<String,Object> sharedState = Maps.newHashMap();
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

        loginModule.initialize(new Subject(), cbh, sharedState, ImmutableMap.of(PARAM_IDP_NAME, DEFAULT_IDP_NAME, PARAM_SYNC_HANDLER_NAME, "syncHandler"));
        try {
            loginModule.login();
        } catch (LoginException e) {
            assertTrue(e.getCause() instanceof SyncException);
            throw e;
        }
    }
}