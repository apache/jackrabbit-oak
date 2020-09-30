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
package org.apache.jackrabbit.oak.security.internal;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

public class SecurityProviderBuilderTest extends AbstractSecurityTest {

    private SecurityProviderBuilder builder = SecurityProviderBuilder.newBuilder();

    @Test
    public void testDefault() {
        SecurityProvider sp = builder.build();
        assertNotNull(sp.getConfiguration(AuthenticationConfiguration.class));
        assertNotNull(sp.getConfiguration(AuthorizationConfiguration.class));
        assertNotNull(sp.getConfiguration(PrincipalConfiguration.class));
        assertNotNull(sp.getConfiguration(PrivilegeConfiguration.class));
        assertNotNull(sp.getConfiguration(TokenConfiguration.class));
        assertNotNull(sp.getConfiguration(UserConfiguration.class));
    }

    @Test
    public void testCompositeConfigurations() {
        AuthenticationConfiguration ac = (AuthenticationConfiguration) mock(CompositeConfiguration.class, withSettings().extraInterfaces(AuthenticationConfiguration.class));
        PrivilegeConfiguration pc = (PrivilegeConfiguration) mock(CompositeConfiguration.class, withSettings().extraInterfaces(PrivilegeConfiguration.class));
        UserConfiguration uc = (UserConfiguration) mock(CompositeConfiguration.class, withSettings().extraInterfaces(UserConfiguration.class));
        AuthorizationConfiguration auc = (AuthorizationConfiguration) mock(CompositeConfiguration.class, withSettings().extraInterfaces(AuthorizationConfiguration.class));
        PrincipalConfiguration pnc = (PrincipalConfiguration) mock(CompositeConfiguration.class, withSettings().extraInterfaces(PrincipalConfiguration.class));
        TokenConfiguration tc = (TokenConfiguration) mock(CompositeConfiguration.class, withSettings().extraInterfaces(TokenConfiguration.class));
        SecurityProvider sp = builder.with(
                ac, ConfigurationParameters.EMPTY,
                pc, ConfigurationParameters.EMPTY,
                uc, ConfigurationParameters.EMPTY,
                auc, ConfigurationParameters.EMPTY,
                pnc, ConfigurationParameters.EMPTY,
                tc, ConfigurationParameters.EMPTY).build();

        assertTrue(sp.getConfiguration(AuthenticationConfiguration.class) instanceof CompositeConfiguration);
        assertTrue(sp.getConfiguration(AuthorizationConfiguration.class) instanceof CompositeConfiguration);
        assertTrue(sp.getConfiguration(PrincipalConfiguration.class) instanceof CompositeConfiguration);
        assertTrue(sp.getConfiguration(PrivilegeConfiguration.class) instanceof CompositeConfiguration);
        assertTrue(sp.getConfiguration(TokenConfiguration.class) instanceof CompositeConfiguration);
        assertTrue(sp.getConfiguration(UserConfiguration.class) instanceof CompositeConfiguration);
    }

    @Test
    public void testSingularConfigurations() {
        SecurityProvider sp = builder.with(
                mock(AuthenticationConfiguration.class), ConfigurationParameters.EMPTY,
                mock(PrivilegeConfiguration.class), ConfigurationParameters.EMPTY,
                mock(UserConfiguration.class), ConfigurationParameters.EMPTY,
                mock(AuthorizationConfiguration.class), ConfigurationParameters.EMPTY,
                mock(PrincipalConfiguration.class), ConfigurationParameters.EMPTY,
                mock(TokenConfiguration.class), ConfigurationParameters.EMPTY).build();

        assertFalse(sp.getConfiguration(AuthenticationConfiguration.class) instanceof CompositeConfiguration);
        assertFalse(sp.getConfiguration(AuthorizationConfiguration.class) instanceof CompositeConfiguration);
        assertFalse(sp.getConfiguration(PrincipalConfiguration.class) instanceof CompositeConfiguration);
        assertFalse(sp.getConfiguration(PrivilegeConfiguration.class) instanceof CompositeConfiguration);
        assertFalse(sp.getConfiguration(TokenConfiguration.class) instanceof CompositeConfiguration);
        assertFalse(sp.getConfiguration(UserConfiguration.class) instanceof CompositeConfiguration);
    }
}
