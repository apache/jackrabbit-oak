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

import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * TokenProviderImplTest...
 */
public class TokenProviderImplTest extends AbstractSecurityTest {

    private TokenProviderImpl tokenProvider;

    @Before
    public void before() throws Exception {
        super.before();

        tokenProvider = new TokenProviderImpl(admin.getLatestRoot(),
                ConfigurationParameters.EMPTY,
                securityProvider.getUserConfiguration());
    }


    @Test
    public void testDoCreateToken() throws Exception {
        assertFalse(tokenProvider.doCreateToken(new GuestCredentials()));
        assertFalse(tokenProvider.doCreateToken(new TokenCredentials("token")));
        assertFalse(tokenProvider.doCreateToken(getAdminCredentials()));

        SimpleCredentials sc = new SimpleCredentials("uid", "pw".toCharArray());
        assertFalse(tokenProvider.doCreateToken(sc));

        sc.setAttribute("any_attribute", "value");
        assertFalse(tokenProvider.doCreateToken(sc));

        sc.setAttribute(TokenProvider.TOKEN_ATTRIBUTE + "_key", "value");
        assertFalse(tokenProvider.doCreateToken(sc));

        sc.setAttribute(TokenProvider.TOKEN_ATTRIBUTE, "existing");
        assertFalse(tokenProvider.doCreateToken(sc));

        sc.setAttribute(TokenProvider.TOKEN_ATTRIBUTE, "");
        assertTrue(tokenProvider.doCreateToken(sc));
    }

    @Test
    public void testCreateTokenFromCredentials() throws Exception {
        // TODO
    }

    @Test
    public void testCreateTokenFromUserId() throws Exception {
        // TODO
    }

    @Test
    public void testGetTokenInfo() throws Exception {
        // TODO
    }

    @Test
    public void testRemoveToken() throws Exception {
        // TODO
    }

    @Test
    public void testResetTokenExpiration() throws Exception {
        // TODO
    }
}