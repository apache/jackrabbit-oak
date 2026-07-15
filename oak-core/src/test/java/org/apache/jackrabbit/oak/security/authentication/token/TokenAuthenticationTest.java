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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMonitor;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenCredentialsExpiredException;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TokenAuthenticationTest extends AbstractTokenTest {

    private LoginModuleMonitor monitor;
    private TokenAuthentication authentication;
    private TokenProviderImpl tokenProvider;
    private String userId;

    @Before
    public void before() throws Exception {
        super.before();
        tokenProvider = createTokenProvider(root, getUserConfiguration());

        root.commit();
        monitor = mock(LoginModuleMonitor.class);
        authentication = new TokenAuthentication(tokenProvider, monitor);
        userId = getTestUser().getID();
    }

    @After
    public void after() throws Exception {
        try {
            clearInvocations(monitor);
        } finally {
            super.after();
        }
    }

    @Test
    public void testAuthenticateWithInvalidCredentials() throws Exception {
        List<Credentials> invalid = new ArrayList<>();
        invalid.add(new GuestCredentials());
        invalid.add(new SimpleCredentials(userId, new char[0]));

        for (Credentials creds : invalid) {
            assertFalse(authentication.authenticate(creds));
        }
    }

    @Test
    public void testAuthenticateWithInvalidTokenCredentials() {
        try {
            authentication.authenticate(new TokenCredentials(UUID.randomUUID().toString()));
            fail("LoginException expected");
        } catch (LoginException e) {
            // success
            verify(monitor).loginFailed(any(LoginException.class), any(Credentials.class));
        }
    }

    @Test
    public void testAuthenticate() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        assertTrue(authentication.authenticate(new TokenCredentials(info.getToken())));
        verifyNoInteractions(monitor);
    }

    @Test
    public void testGetTokenInfoBeforeAuthenticate() {
        try {
            authentication.getTokenInfo();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            // success
            verifyNoInteractions(monitor);
        }
    }

    @Test
    public void testGetTokenInfoAfterAuthenticate() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        authentication.authenticate(new TokenCredentials(info.getToken()));

        TokenInfo info2 = authentication.getTokenInfo();
        assertNotNull(info2);
        assertEquals(info.getUserId(), info2.getUserId());
        verifyNoInteractions(monitor);
    }

    @Test
    public void testAuthenticateNotMatchingToken() {
        TokenInfo info = tokenProvider.createToken(userId, Map.of(TokenConstants.TOKEN_ATTRIBUTE + "_mandatory", "val"));
        assertNotNull(info);
        try {
            authentication.authenticate(new TokenCredentials(info.getToken()));
            fail("LoginException expected");
        } catch (LoginException e) {
            // success
            verify(monitor).loginFailed(any(LoginException.class), any(Credentials.class));
        }
    }

    @Test
    public void testAuthenticateExpiredToken() {
        TokenProvider tp = new TokenProviderImpl(root,
                ConfigurationParameters.of(TokenProvider.PARAM_TOKEN_EXPIRATION, 1),
                getUserConfiguration());

        TokenInfo info = createTokenInfo(tp, userId);
        waitUntilExpired(info);

        try {
            new TokenAuthentication(tp, monitor).authenticate(new TokenCredentials(info.getToken()));
            fail("LoginException expected");
        } catch (LoginException e) {
            // success
            assertTrue(e instanceof TokenCredentialsExpiredException);
            verify(monitor).loginFailed(any(TokenCredentialsExpiredException.class), any(Credentials.class));
        }

        // expired token must have been removed
        assertNull(tp.getTokenInfo(info.getToken()));
    }

    @Test(expected = IllegalStateException.class)
    public void testGetUserIdBeforeLogin() {
        authentication.getUserId();
    }

    @Test
    public void testGetUserId() throws LoginException {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        assertTrue(authentication.authenticate(new TokenCredentials(info.getToken())));
        assertEquals(userId, authentication.getUserId());
        verifyNoInteractions(monitor);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetUserPrincipalBeforeLogin() {
        authentication.getUserPrincipal();
    }

    @Test
    public void testGetUserPrincipal() throws Exception {
        TokenInfo info = createTokenInfo(tokenProvider, userId);
        assertTrue(authentication.authenticate(new TokenCredentials(info.getToken())));
        assertEquals(getTestUser().getPrincipal(), authentication.getUserPrincipal());
        verifyNoInteractions(monitor);
    }

    @Test
    public void testGetUserPrincipalNonDefaultProvider() throws Exception {
        TokenInfo info = mock(TokenInfo.class);
        when(info.isExpired(anyLong())).thenReturn(false);
        when(info.matches(any(TokenCredentials.class))).thenReturn(true);

        TokenProvider tp = when(mock(TokenProvider.class).getTokenInfo(anyString())).thenReturn(info).getMock();
        TokenAuthentication ta = new TokenAuthentication(tp, monitor);

        assertTrue(ta.authenticate(new TokenCredentials("token")));
        assertNull(ta.getUserPrincipal());
        verifyNoInteractions(monitor);
    }

    @Test
    public void testAuthenticateRefreshToken() {
        TokenCredentials tc = new TokenCredentials("token");
        TokenProvider tp = mock(TokenProvider.class);
        TokenInfo ti = mock(TokenInfo.class);

        when(tp.getTokenInfo(anyString())).thenReturn(ti);
        when(ti.isExpired(anyLong())).thenReturn(false);
        when(ti.matches(tc)).thenReturn(true);

        TokenAuthentication auth = new TokenAuthentication(tp, monitor);
        try {
            assertTrue(auth.authenticate(tc));
            verify(ti).resetExpiration(anyLong());
        } catch (LoginException e) {
            fail(e.getMessage());
            verify(monitor).loginFailed(e, tc);
        }
    }

    @Test
    public void testAuthenticateSkipRefreshToken() {
        TokenCredentials tc = new TokenCredentials("token");
        tc.setAttribute(TokenConstants.TOKEN_SKIP_REFRESH, "");

        TokenProvider tp = mock(TokenProvider.class);
        TokenInfo ti = mock(TokenInfo.class);

        when(tp.getTokenInfo(anyString())).thenReturn(ti);
        when(ti.isExpired(anyLong())).thenReturn(false);
        when(ti.matches(tc)).thenReturn(true);

        TokenAuthentication auth = new TokenAuthentication(tp, monitor);
        try {
            assertTrue(auth.authenticate(tc));
            verify(ti, Mockito.never()).resetExpiration(anyLong());
        } catch (LoginException e) {
            fail(e.getMessage());
            verify(monitor).loginFailed(e, tc);
        }
    }

    @Test
    public void testAuthenticateExpiredTokenMock() {
        TokenCredentials tc = new TokenCredentials("token");
        TokenProvider tp = mock(TokenProvider.class);
        TokenInfo ti = mock(TokenInfo.class);

        when(tp.getTokenInfo(anyString())).thenReturn(ti);
        when(ti.isExpired(anyLong())).thenReturn(true);

        TokenAuthentication auth = new TokenAuthentication(tp, monitor);
        try {
            auth.authenticate(tc);
            fail("LoginException expected");
        } catch (LoginException e) {
            // success
            assertTrue(e instanceof TokenCredentialsExpiredException);
            verify(monitor).loginFailed((TokenCredentialsExpiredException) e, tc);
        }

        verify(ti, Mockito.never()).matches(any());
        verify(ti, Mockito.never()).resetExpiration(anyLong());
    }
}
