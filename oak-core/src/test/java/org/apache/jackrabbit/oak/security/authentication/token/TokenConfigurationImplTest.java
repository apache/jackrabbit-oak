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

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TokenConfigurationImplTest extends AbstractSecurityTest {

    private static final int DEFAULT_EXPIRATION = 2 * 3600 * 1000;

    private TokenConfigurationImpl tc;

    @Override
    public void before() throws Exception {
        super.before();
        tc = new TokenConfigurationImpl(getSecurityProvider());
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters config = ConfigurationParameters.of(TokenProviderImpl.PARAM_TOKEN_EXPIRATION, 60);
        return ConfigurationParameters.of(TokenConfiguration.NAME, config);
    }

    @Test
    public void testConfigOptions() {
        int exp = tc.getParameters().getConfigValue(TokenProvider.PARAM_TOKEN_EXPIRATION, DEFAULT_EXPIRATION);
        assertEquals(60, exp);
    }

    @Test
    public void testConfigOptions2() {
        int exp = getConfig(TokenConfiguration.class).getParameters().getConfigValue(TokenProvider.PARAM_TOKEN_EXPIRATION, DEFAULT_EXPIRATION);
        assertEquals(60, exp);
    }
}