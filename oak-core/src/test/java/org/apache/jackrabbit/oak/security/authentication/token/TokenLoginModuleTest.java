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
package org.apache.jackrabbit.oak.security.authentication.token;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.PrincipalProviderCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.RepositoryCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.TokenProviderCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants.TOKEN_ATTRIBUTE;
import static org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants.TOKEN_ATTRIBUTE_DO_CREATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TokenLoginModuleTest extends AbstractSecurityTest {

    private static TokenProvider mockTokenProvider(@NotNull String userId) {
        TokenInfo info = mock(TokenInfo.class);
        when(info.isExpired(anyLong())).thenReturn(false);
        when(info.matches(any(TokenCredentials.class))).thenReturn(true);
        when(info.getUserId()).thenReturn(userId);

        return when(mock(TokenProvider.class).getTokenInfo(anyString())).thenReturn(info).getMock();
    }

    @Override
    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                AppConfigurationEntry defaultEntry = new AppConfigurationEntry(
                        TokenLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        Collections.<String, Object>emptyMap());

                return new AppConfigurationEntry[] {defaultEntry};
            }
        };
    }

    @Test
    public void testNullLogin() throws Exception {
        try (ContentSession cs = login(null)) {
            fail("Null login should fail");
        } catch (LoginException e) {
            // success
        }
    }

    @Test
    public void testGuestLogin() throws Exception {
        try (ContentSession cs = login(new GuestCredentials())) {
            fail("GuestCredentials login should fail");
        } catch (LoginException e) {
            // success
        }
    }

    @Test
    public void testSimpleCredentials() throws Exception {
        ContentSession cs = null;
        try {
            SimpleCredentials sc = new SimpleCredentials("admin", "admin".toCharArray());
            cs = login(sc);
            fail("Unsupported credentials login should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testSimpleCredentialsWithAttribute() throws Exception {
        ContentSession cs = null;
        try {
            SimpleCredentials sc = new SimpleCredentials("test", new char[0]);
            sc.setAttribute(TOKEN_ATTRIBUTE, TOKEN_ATTRIBUTE_DO_CREATE);

            cs = login(sc);
            fail("Unsupported credentials login should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testInvalidTokenCredentials() throws Exception {
        try (ContentSession cs = login(new TokenCredentials("invalid"))) {
            fail("Invalid token credentials login should fail");
        } catch (LoginException e) {
            // success
        }
    }

    @Test
    public void testTokenCredentials() throws Exception {
        Root root = adminSession.getLatestRoot();
        TokenConfiguration tokenConfig = getSecurityProvider().getConfiguration(TokenConfiguration.class);
        TokenProvider tp = tokenConfig.getTokenProvider(root);

        SimpleCredentials sc = (SimpleCredentials) getAdminCredentials();
        TokenInfo info = tp.createToken(sc.getUserID(), Collections.<String, Object>emptyMap());

        try (ContentSession cs = login(new TokenCredentials(info.getToken()))) {
            assertEquals(sc.getUserID(), cs.getAuthInfo().getUserID());
        }
    }

    @Test
    public void testTokenCredentialsWithPublicAttributes() throws Exception {
        Root root = adminSession.getLatestRoot();
        TokenConfiguration tokenConfig = getSecurityProvider().getConfiguration(TokenConfiguration.class);
        TokenProvider tp = tokenConfig.getTokenProvider(root);

        SimpleCredentials sc = (SimpleCredentials) getAdminCredentials();
        TokenInfo info = tp.createToken(sc.getUserID(), Map.of("public", "value"));

        TokenCredentials tc = new TokenCredentials(info.getToken());
        try (ContentSession cs = login(tc)) {
            assertEquals(sc.getUserID(), cs.getAuthInfo().getUserID());
            assertEquals("value", cs.getAuthInfo().getAttribute("public"));
        }
    }

    @Test
    public void testMissingTokenProvider() throws Exception {
        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), null, Map.of(), Map.of());

        assertFalse(lm.login());
        assertFalse(lm.commit());
        assertFalse(lm.logout());
    }

    @Test
    public void testMissingTokenProvider2() throws Exception {
        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new TestCallbackHandler(null), Map.of(), Map.of());

        assertFalse(lm.login());
        assertFalse(lm.commit());
        assertFalse(lm.logout());
    }


    @Test
    public void testMissingTokenProvider3() throws Exception {
        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new TestCallbackHandler(null),
                Map.of(AbstractLoginModule.SHARED_KEY_CREDENTIALS, mock(Credentials.class)),
                Map.of());

        assertFalse(lm.login());
        assertFalse(lm.commit());
        assertFalse(lm.logout());
    }

    @Test
    public void testTokenProviderCallback() throws Exception {
        TokenProvider tp = new TokenProviderImpl(root, ConfigurationParameters.EMPTY, getUserConfiguration());

        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new TestCallbackHandler(tp), Map.of(), Map.of());

        assertFalse(lm.login());
        assertFalse(lm.commit());
        assertFalse(lm.logout());
    }

    @Test
    public void testUnsupportedCallbackException() throws Exception {
        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new ThrowingCallbackHandler(UnsupportedCallbackException.class), Map.of(), Map.of());

        assertFalse(lm.login());
        assertFalse(lm.commit());
        assertFalse(lm.logout());
    }

    @Test
    public void testIOException() throws Exception {
        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new ThrowingCallbackHandler(IOException.class), Map.of(), Map.of());

        assertFalse(lm.login());
        assertFalse(lm.commit());
        assertFalse(lm.logout());
    }

    @Test(expected = LoginException.class)
    public void testCreateTokenFailure() throws Exception {
        TokenProvider tp = mock(TokenProvider.class);
        when(tp.doCreateToken(any(Credentials.class))).thenReturn(true);
        when(tp.createToken(any(Credentials.class))).thenReturn(null);
        when(tp.createToken(anyString(), any(Map.class))).thenReturn(null);

        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new TestCallbackHandler(tp),
                Map.of(AbstractLoginModule.SHARED_KEY_CREDENTIALS, new Credentials() {}),
                Map.of());

        assertFalse(lm.login());
        try {
            lm.commit();
        } finally {
            verify(tp, times(1)).doCreateToken(any(Credentials.class));
            assertFalse(lm.logout());
        }
    }

    @Test
    public void testMissingUserPrincipal() throws Exception {
        TokenProvider tp = mockTokenProvider(getTestUser().getID());
        TokenCredentials tc = new TokenCredentials("token");

        TokenLoginModule lm = new TokenLoginModule();
        Map sharedState = new HashMap<>(Map.of(AbstractLoginModule.SHARED_KEY_CREDENTIALS, tc));

        Subject subject = new Subject();
        CallbackHandler cbh = callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof PrincipalProviderCallback) {
                    ((PrincipalProviderCallback) callback).setPrincipalProvider(getConfig(PrincipalConfiguration.class).getPrincipalProvider(root, getNamePathMapper()));
                }
            }
        };
        lm.initialize(subject, new TestCallbackHandler(tp, cbh),
                sharedState,
                Map.of());

        assertTrue(lm.login());
        assertTrue(lm.commit());

        assertEquals(Set.of(getTestUser().getPrincipal(), EveryonePrincipal.getInstance()), subject.getPrincipals());
        assertFalse(subject.getPublicCredentials(AuthInfo.class).isEmpty());
        assertFalse(subject.getPublicCredentials(TokenCredentials.class).isEmpty());

        assertTrue(lm.logout());
        assertTrue(subject.getPublicCredentials().isEmpty());
        assertTrue(subject.getPrincipals().isEmpty());
    }

    @Test
    public void testReadOnlySubject() throws Exception {
        User u = getTestUser();
        Subject subject = new Subject();
        subject.setReadOnly();

        TokenProvider tp = spy(new TokenProviderImpl(root, ConfigurationParameters.EMPTY, getUserConfiguration()));

        SimpleCredentials sc = new SimpleCredentials(u.getID(), u.getID().toCharArray());
        sc.setAttribute(TOKEN_ATTRIBUTE, TOKEN_ATTRIBUTE_DO_CREATE);

        TokenLoginModule lm = new TokenLoginModule();
        Map sharedState = new HashMap<>(Map.of(AbstractLoginModule.SHARED_KEY_CREDENTIALS, sc));
        lm.initialize(subject, new TestCallbackHandler(tp),
                sharedState,
                Map.of());

        assertFalse(lm.login());
        assertFalse(lm.commit());
        verify(tp, times(1)).createToken(sc);
        assertTrue(subject.getPublicCredentials(TokenCredentials.class).isEmpty());

        assertFalse(lm.logout());
    }

    @Test
    public void testInvalidShareCredentialsObject() throws Exception {
        TokenProvider tp = spy(new TokenProviderImpl(root, ConfigurationParameters.EMPTY, getUserConfiguration()));

        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new TestCallbackHandler(tp),
                Map.of(AbstractLoginModule.SHARED_KEY_CREDENTIALS, "notCredentialsObject"),
                Map.of());

        assertFalse(lm.login());
        assertFalse(lm.commit());
        verify(tp, never()).createToken(any(Credentials.class));
        assertFalse(lm.logout());
    }

    @Test
    public void testMissingShareCredentials() throws Exception {
        TokenProvider tp = spy(new TokenProviderImpl(root, ConfigurationParameters.EMPTY, getUserConfiguration()));

        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new TestCallbackHandler(tp),
                Map.of(),
                Map.of());

        assertFalse(lm.login());
        assertFalse(lm.commit());
        verify(tp, never()).doCreateToken(any(Credentials.class));
        assertFalse(lm.logout());
    }

    @Test
    public void testMissingSecurityProvider() throws Exception {
        CallbackHandler cbh = callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof RepositoryCallback) {
                    ((RepositoryCallback) callback).setSecurityProvider(null);
                    ((RepositoryCallback) callback).setContentRepository(getContentRepository());
                }
            }
        };

        SimpleCredentials sc = new SimpleCredentials(getTestUser().getID(), getTestUser().getID().toCharArray());
        sc.setAttribute(TOKEN_ATTRIBUTE, TOKEN_ATTRIBUTE_DO_CREATE);
        Map sharedState = new HashMap<>(Map.of(AbstractLoginModule.SHARED_KEY_CREDENTIALS, sc));

        TokenProvider tp = spy(new TokenProviderImpl(root, ConfigurationParameters.EMPTY, getUserConfiguration()));
        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new TestCallbackHandler(tp, cbh),
                sharedState,
                Map.of());

        assertFalse(lm.login());
        assertFalse(lm.commit());
        verify(tp, times(1)).createToken(sc);
        assertFalse(lm.logout());
    }

    @Test
    public void testMissingRoot() throws Exception {
        CallbackHandler cbh = callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof RepositoryCallback) {
                    ((RepositoryCallback) callback).setSecurityProvider(getSecurityProvider());
                    ((RepositoryCallback) callback).setContentRepository(null);
                }
            }
        };

        SimpleCredentials sc = new SimpleCredentials(getTestUser().getID(), getTestUser().getID().toCharArray());
        sc.setAttribute(TOKEN_ATTRIBUTE, TOKEN_ATTRIBUTE_DO_CREATE);
        Map sharedState = new HashMap<>(Map.of(AbstractLoginModule.SHARED_KEY_CREDENTIALS, sc));

        TokenProvider tp = spy(new TokenProviderImpl(root, ConfigurationParameters.EMPTY, getUserConfiguration()));
        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new TestCallbackHandler(tp, cbh),
                sharedState,
                Map.of());

        assertFalse(lm.login());
        assertFalse(lm.commit());
        verify(tp, times(1)).createToken(sc);
        assertFalse(lm.logout());
    }

    @Test
    public void testSuccessCommitRespectsSubjectPrincipals() throws Exception {
        Principal principal = new PrincipalImpl("subjetPrincipal");
        Subject subject = new Subject();
        subject.getPrincipals().add(principal);

        Map<String, Object> shared = new HashMap<>();
        shared.put(AbstractLoginModule.SHARED_KEY_CREDENTIALS, new TokenCredentials("token"));

        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(subject, new TestCallbackHandler(mockTokenProvider("userId")), shared, new HashMap<>());

        assertTrue(lm.login());
        assertTrue(lm.commit());

        Set<AuthInfo> authInfoSet = subject.getPublicCredentials(AuthInfo.class);
        assertFalse(authInfoSet.isEmpty());
        AuthInfo info = authInfoSet.iterator().next();
        assertTrue(info.getPrincipals().contains(principal));
    }

    private final class TestCallbackHandler implements CallbackHandler {

        private final TokenProvider tokenProvider;
        private final CallbackHandler base;
        private final Class<? extends Exception> e;

        private TestCallbackHandler(@Nullable TokenProvider tokenProvider) {
            this.tokenProvider = tokenProvider;
            this.base = null;
            this.e = null;
        }

        private TestCallbackHandler(@Nullable TokenProvider tokenProvider, @NotNull CallbackHandler base) {
            this.tokenProvider = tokenProvider;
            this.base = base;
            this.e = null;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback cb : callbacks) {
                if (cb instanceof TokenProviderCallback) {
                    ((TokenProviderCallback) cb).setTokenProvider(tokenProvider);
                } else if (base != null) {
                    base.handle(callbacks);
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }
        }
    }

    private final class ThrowingCallbackHandler implements CallbackHandler {

        private final Class<? extends Exception> e;

        private ThrowingCallbackHandler(@NotNull Class<? extends Exception> e) {
            this.e = e;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            if (e.getName().equals(UnsupportedCallbackException.class.getName())) {
                throw new UnsupportedCallbackException(new TokenProviderCallback());
            } else if (e.getName().equals(IOException.class.getName())) {
                throw new IOException();
            }
        }
    }
}
