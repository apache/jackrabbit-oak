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
import java.util.Collections;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.AbstractLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.callback.TokenProviderCallback;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TokenLoginModuleTest extends AbstractSecurityTest {

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
        ContentSession cs = null;
        try {
            cs = login(new GuestCredentials());
            fail("GuestCredentials login should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
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
            sc.setAttribute(".token", "");

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
        ContentSession cs = null;
        try {
            cs = login(new TokenCredentials("invalid"));
            fail("Invalid token credentials login should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testValidTokenCredentials() throws Exception {
        Root root = adminSession.getLatestRoot();
        TokenConfiguration tokenConfig = getSecurityProvider().getConfiguration(TokenConfiguration.class);
        TokenProvider tp = tokenConfig.getTokenProvider(root);

        SimpleCredentials sc = (SimpleCredentials) getAdminCredentials();
        TokenInfo info = tp.createToken(sc.getUserID(), Collections.<String, Object>emptyMap());

        ContentSession cs = login(new TokenCredentials(info.getToken()));
        try {
            assertEquals(sc.getUserID(), cs.getAuthInfo().getUserID());
        } finally {
            cs.close();
        }
    }

    @Test
    public void testMissingTokenProvider() throws Exception {
        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), null, ImmutableMap.<String, Object>of(), ImmutableMap.<String, Object>of());

        assertFalse(lm.login());
    }

    @Test
    public void testMissingTokenProvider2() throws Exception {
        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new TestCallbackHandler(null), ImmutableMap.<String, Object>of(), ImmutableMap.<String, Object>of());

        assertFalse(lm.login());
    }

    @Test
    public void testTokenProviderCallback() throws Exception {
        TokenProvider tp = new TokenProviderImpl(root, ConfigurationParameters.EMPTY, getUserConfiguration());

        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new TestCallbackHandler(tp), ImmutableMap.<String, Object>of(), ImmutableMap.<String, Object>of());

        assertFalse(lm.login());
    }

    @Test
    public void testUnsupportedCallbackException() throws Exception {
        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new ThrowingCallbackHandler(UnsupportedCallbackException.class), ImmutableMap.<String, Object>of(), ImmutableMap.<String, Object>of());

        assertFalse(lm.login());
    }

    @Test
    public void testIOException() throws Exception {
        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new ThrowingCallbackHandler(IOException.class), ImmutableMap.<String, Object>of(), ImmutableMap.<String, Object>of());

        assertFalse(lm.login());
    }

    @Test
    public void testCreateTokenFailed() throws Exception {
        TokenProvider tp = new TokenProvider() {

            @Override
            public boolean doCreateToken(@Nonnull Credentials credentials) {
                return true;
            }

            @CheckForNull
            @Override
            public TokenInfo createToken(@Nonnull Credentials credentials) {
                return null;
            }

            @CheckForNull
            @Override
            public TokenInfo createToken(@Nonnull String userId, @Nonnull Map<String, ?> attributes) {
                return null;
            }

            @CheckForNull
            @Override
            public TokenInfo getTokenInfo(@Nonnull String token) {
                return null;
            }
        };

        TokenLoginModule lm = new TokenLoginModule();
        lm.initialize(new Subject(), new TestCallbackHandler(tp),
                ImmutableMap.<String, Object>of(AbstractLoginModule.SHARED_KEY_CREDENTIALS, new Credentials() {}),
                ImmutableMap.<String, Object>of());

        lm.login();
        try {
            lm.commit();
            fail("LoginException expected");
        } catch (LoginException e) {
            // success
        }
    }

    private final class TestCallbackHandler implements CallbackHandler {

        private final TokenProvider tokenProvider;
        private final Class<? extends Exception> e;

        private TestCallbackHandler(@Nullable TokenProvider tokenProvider) {
            this.tokenProvider = tokenProvider;
            this.e = null;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback cb : callbacks) {
                if (cb instanceof TokenProviderCallback) {
                    ((TokenProviderCallback) cb).setTokenProvider(tokenProvider);
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }
        }
    }

    private final class ThrowingCallbackHandler implements CallbackHandler {

        private final Class<? extends Exception> e;

        private ThrowingCallbackHandler(@Nonnull Class<? extends Exception> e) {
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