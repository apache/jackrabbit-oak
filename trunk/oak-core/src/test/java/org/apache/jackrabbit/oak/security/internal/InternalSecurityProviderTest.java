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

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class InternalSecurityProviderTest {

    private static final ConfigurationParameters PARAMS = ConfigurationParameters.of("a", "value");

    private InternalSecurityProvider securityProvider = new InternalSecurityProvider();

    @Test
    public void testDefaultWhiteboard() {
        assertNull(securityProvider.getWhiteboard());
    }

    @Test
    public void testSetWhiteboard() {
        Whiteboard wb = new DefaultWhiteboard();
        securityProvider.setWhiteboard(wb);

        assertSame(wb, securityProvider.getWhiteboard());
    }

    @Test
    public void testDefaultPrincipalConfiguration() {
        PrincipalConfiguration defaultConfig = securityProvider.getConfiguration(PrincipalConfiguration.class);
        assertNull(defaultConfig);
    }

    @Test
    public void testSetPrincipalConfiguration() {
        PrincipalConfiguration pc = Mockito.mock(PrincipalConfiguration.class);
        when(pc.getParameters()).thenReturn(PARAMS);

        securityProvider.setPrincipalConfiguration(pc);

        assertSame(pc, securityProvider.getConfiguration(PrincipalConfiguration.class));

        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof PrincipalConfiguration) {
                assertSame(pc, sc);
            }
        }
        assertTrue(Iterables.contains(securityProvider.getConfigurations(), pc));

        assertEquals(PARAMS, securityProvider.getParameters(PrincipalConfiguration.NAME));
    }

    @Test
    public void testDefaultUserConfiguration() {
        assertNull(securityProvider.getConfiguration(UserConfiguration.class));
    }

    @Test
    public void testSetUserConfiguration() {
        UserConfiguration uc = Mockito.mock(UserConfiguration.class);
        when(uc.getParameters()).thenReturn(PARAMS);

        securityProvider.setUserConfiguration(uc);

        assertSame(uc, securityProvider.getConfiguration(UserConfiguration.class));
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof UserConfiguration) {
                assertSame(uc, sc);
            }
        }
        assertEquals(PARAMS, securityProvider.getParameters(UserConfiguration.NAME));
    }

    @Test
    public void testDefaultAuthenticationConfiguration() {
        assertNull(securityProvider.getConfiguration(AuthenticationConfiguration.class));
    }

    @Test
    public void testSetAuthenticationConfiguration() {
        AuthenticationConfiguration ac = Mockito.mock(AuthenticationConfiguration.class);
        when(ac.getParameters()).thenReturn(PARAMS);

        securityProvider.setAuthenticationConfiguration(ac);

        assertSame(ac, securityProvider.getConfiguration(AuthenticationConfiguration.class));
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof AuthenticationConfiguration) {
                assertSame(ac, sc);
            }
        }

        assertEquals(PARAMS, securityProvider.getParameters(AuthenticationConfiguration.NAME));
    }

    @Test
    public void testDefaultAuthorizationConfiguration() {
        assertNull(securityProvider.getConfiguration(AuthorizationConfiguration.class));
    }

    @Test
    public void testSetAuthorizationConfiguration() {
        AuthorizationConfiguration ac = Mockito.mock(AuthorizationConfiguration.class);
        when(ac.getParameters()).thenReturn(PARAMS);
        securityProvider.setAuthorizationConfiguration(ac);

        assertSame(ac, securityProvider.getConfiguration(AuthorizationConfiguration.class));
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof AuthorizationConfiguration) {
                assertSame(ac, sc);
            }
        }

        assertEquals(PARAMS, securityProvider.getParameters(AuthorizationConfiguration.NAME));
    }

    @Test
    public void testDefaultPrivilegeConfiguration() {
        assertNull(securityProvider.getConfiguration(PrivilegeConfiguration.class));
    }

    @Test
    public void testSetPrivilegeConfiguration() {
        PrivilegeConfiguration pc = Mockito.mock(PrivilegeConfiguration.class);
        when(pc.getParameters()).thenReturn(PARAMS);
        securityProvider.setPrivilegeConfiguration(pc);

        assertSame(pc, securityProvider.getConfiguration(PrivilegeConfiguration.class));
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof PrivilegeConfiguration) {
                assertSame(pc, sc);
            }
        }

        assertEquals(PARAMS, securityProvider.getParameters(PrivilegeConfiguration.NAME));
    }

    @Test
    public void testDefaultTokenConfiguration() {
        assertNull(securityProvider.getConfiguration(TokenConfiguration.class));
    }

    @Test
    public void testSetTokenConfiguration() {
        TokenConfiguration tc = Mockito.mock(TokenConfiguration.class);
        when(tc.getParameters()).thenReturn(PARAMS);
        securityProvider.setTokenConfiguration(tc);

        assertSame(tc, securityProvider.getConfiguration(TokenConfiguration.class));
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof TokenConfiguration) {
                assertSame(tc, sc);
            }
        }
        assertEquals(PARAMS, securityProvider.getParameters(TokenConfiguration.NAME));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetUnknownConfiguration() {
        securityProvider.getConfiguration(SecurityConfiguration.class);
    }

    @Test
    public void testGetParametersForNull() {
        assertSame(ConfigurationParameters.EMPTY, securityProvider.getParameters(null));
    }

    @Test
    public void testGetParametersForUnknown() {
        assertSame(ConfigurationParameters.EMPTY, securityProvider.getParameters("unknownName"));
    }
}