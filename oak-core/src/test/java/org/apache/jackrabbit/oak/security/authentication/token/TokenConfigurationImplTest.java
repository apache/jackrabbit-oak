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

import java.security.Principal;
import java.util.List;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TokenConfigurationImplTest extends AbstractSecurityTest {

    private TokenConfigurationImpl tc;

    @Override
    public void before() throws Exception {
        super.before();
        tc = new TokenConfigurationImpl(getSecurityProvider());
        tc.setTreeProvider(getTreeProvider());
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters config = ConfigurationParameters.of(
                TokenProvider.PARAM_TOKEN_EXPIRATION, 60,
                TokenProvider.PARAM_TOKEN_REFRESH, true);
        return ConfigurationParameters.of(TokenConfiguration.NAME, config);
    }

    @Test
    public void testGetName() {
        assertEquals(TokenConfiguration.NAME, tc.getName());
    }

    @Test
    public void testConfigOptions() {
        long exp = tc.getParameters().getConfigValue(TokenProvider.PARAM_TOKEN_EXPIRATION, TokenProviderImpl.DEFAULT_TOKEN_EXPIRATION);
        assertEquals(60, exp);
    }

    @Test
    public void testConfigOptions2() {
        long exp = getConfig(TokenConfiguration.class).getParameters().getConfigValue(TokenProvider.PARAM_TOKEN_EXPIRATION, TokenProviderImpl.DEFAULT_TOKEN_EXPIRATION);
        assertEquals(60, exp);
    }

    @Test
    public void testRefresh() {
        boolean refresh = getConfig(TokenConfiguration.class).getParameters().getConfigValue(TokenProvider.PARAM_TOKEN_REFRESH, false);
        assertTrue(refresh);
    }

    @Test
    public void testGetValidators() {
        List<? extends ValidatorProvider> validators = tc.getValidators(root.getContentSession().getWorkspaceName(), ImmutableSet.<Principal>of(), new MoveTracker());
        assertNotNull(validators);
        assertEquals(1, validators.size());
        assertTrue(validators.get(0) instanceof TokenValidatorProvider);
    }

    @Test
    public void testGetTokenProvider() {
        TokenProvider tp = tc.getTokenProvider(root);
        assertTrue(tp instanceof TokenProviderImpl);
    }
}