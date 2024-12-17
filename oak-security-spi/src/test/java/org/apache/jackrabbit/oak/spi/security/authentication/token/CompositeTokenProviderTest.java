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
package org.apache.jackrabbit.oak.spi.security.authentication.token;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompositeTokenProviderTest {

    private static final String TOKEN = "t";
    private static final String USERID = "userId";
    private TokenInfo info;

    private TokenProvider composite;

    @Before
    public void before() {
        info = mock(TokenInfo.class);
        when(info.getToken()).thenReturn(TOKEN);

        TokenProvider tp1 = mock(TokenProvider.class);
        TokenProvider tp2 = new TestTokenProvider();

        composite = CompositeTokenProvider.newInstance(tp1, tp2);
    }

    @Test
    public void testNullProvider() {
        TokenProvider tp = CompositeTokenProvider.newInstance();

        assertSame(tp, CompositeTokenProvider.newInstance(ImmutableList.<TokenProvider>of()));

        Credentials creds = new Credentials() {};

        assertFalse(tp.doCreateToken(null));
        assertFalse(tp.doCreateToken(creds));

        assertNull(tp.createToken(null, null));
        assertNull(tp.createToken(USERID, Map.of()));

        assertNull(tp.createToken(null));
        assertNull(tp.createToken(creds));

        assertNull(tp.getTokenInfo(null));
        assertNull(tp.getTokenInfo("anyString"));
    }

    @Test
    public void testSingleProvider() {
        TokenProvider base = mock(TokenProvider.class);

        TokenProvider tp = CompositeTokenProvider.newInstance(base);

        assertSame(base, tp);
        assertSame(base, CompositeTokenProvider.newInstance(ImmutableList.of(base)));
    }

    @Test
    public void testCreateCompositeProvider() {
        assertTrue(composite instanceof CompositeTokenProvider);
    }

    @Test
    public void testCreateCompositeProviderFromList() {
        TokenProvider base = mock(TokenProvider.class);
        TokenProvider tp = CompositeTokenProvider.newInstance(ImmutableList.of(base, base));
        assertTrue(tp instanceof CompositeTokenProvider);
    }

    @Test
    public void testDoCreateToken() {
        assertTrue(composite.doCreateToken(new SimpleCredentials(USERID, new char[0])));
        assertFalse(composite.doCreateToken(new GuestCredentials()));
        assertFalse(composite.doCreateToken(new Credentials() {}));
    }

    @Test
    public void testCreateTokenFromCredentials() {
        assertSame(info, composite.createToken(new SimpleCredentials(USERID, new char[0])));
        assertNull(composite.createToken(new GuestCredentials()));
        assertNull(composite.createToken(new Credentials() {
        }));
    }

    @Test
    public void testCreateTokenFromId() {
        assertSame(info, composite.createToken(USERID, Map.of()));
    }

    @Test
    public void testCreateTokenFromUnknownId() {
        assertNull(composite.createToken("unknown", Map.of()));
    }

    @Test
    public void testCreateTokenFromIdFirstWins() {
        TokenInfo ti = mock(TokenInfo.class);
        TokenProvider tp1 = when(mock(TokenProvider.class).createToken(USERID, Map.of())).thenReturn(ti).getMock();
        TokenProvider tp2 = new TestTokenProvider();

        TokenProvider ctp = CompositeTokenProvider.newInstance(tp1, tp2);
        assertSame(ti, ctp.createToken(USERID, Map.of()));
    }

    @Test
    public void testGetTokenInfo() {
        assertSame(info, composite.getTokenInfo(TOKEN));
        assertNull(composite.getTokenInfo("any"));
    }

    private final class TestTokenProvider implements TokenProvider {
        @Override
        public boolean doCreateToken(@NotNull Credentials credentials) {
            return credentials instanceof SimpleCredentials;
        }

        @Nullable
        @Override
        public TokenInfo createToken(@NotNull Credentials credentials) {
            if (credentials instanceof SimpleCredentials) {
                return info;
            } else {
                return null;
            }
        }

        @Nullable
        @Override
        public TokenInfo createToken(@NotNull String userId, @NotNull Map<String, ?> attributes) {
            if (USERID.equals(userId)) {
                return info;
            } else {
                return null;
            }
        }

        @Nullable
        @Override
        public TokenInfo getTokenInfo(@NotNull String token) {
            if (TOKEN.equals(token)) {
                return info;
            } else {
                return null;
            }
        }
    };
}
