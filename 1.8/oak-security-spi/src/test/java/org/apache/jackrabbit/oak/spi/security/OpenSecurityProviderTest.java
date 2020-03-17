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
package org.apache.jackrabbit.oak.spi.security;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.OpenAuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.OpenAuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class OpenSecurityProviderTest {

    private final OpenSecurityProvider securityProvider = new OpenSecurityProvider();

    @Test
    public void testGetParameters() {
        assertSame(ConfigurationParameters.EMPTY, securityProvider.getParameters(null));
        assertSame(ConfigurationParameters.EMPTY, securityProvider.getParameters(AuthorizationConfiguration.NAME));
    }

    @Test
    public void testGetAuthorizationConfiguration() {
        assertTrue(securityProvider.getConfiguration(AuthorizationConfiguration.class) instanceof OpenAuthorizationConfiguration);
    }

    @Test
    public void testGetAuthenticationConfiguration() {
        assertTrue(securityProvider.getConfiguration(AuthenticationConfiguration.class) instanceof OpenAuthenticationConfiguration);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetUserConfiguration() {
        securityProvider.getConfiguration(UserConfiguration.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPrincipalConfiguration() {
        securityProvider.getConfiguration(PrincipalConfiguration.class);
    }

    @Test
    public void testGetConfigurations() {
        Iterable<? extends SecurityConfiguration> configurations = securityProvider.getConfigurations();
        assertEquals(2, Iterables.size(configurations));
    }
}