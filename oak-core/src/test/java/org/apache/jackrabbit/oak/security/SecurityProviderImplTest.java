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
package org.apache.jackrabbit.oak.security;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.CompositePrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class SecurityProviderImplTest {

    private SecurityProviderImpl securityProvider = new SecurityProviderImpl();

    @Test
    public void testBindPrincipalConfiguration() {
        PrincipalConfiguration defaultConfig = securityProvider.getConfiguration(PrincipalConfiguration.class);
        assertTrue(defaultConfig instanceof CompositePrincipalConfiguration);
        CompositePrincipalConfiguration cpc = (CompositePrincipalConfiguration) defaultConfig;


        PrincipalConfiguration pc = Mockito.mock(PrincipalConfiguration.class);
        when(pc.getParameters()).thenReturn(ConfigurationParameters.EMPTY);

        securityProvider.bindPrincipalConfiguration(pc);

        assertNotSame(pc, securityProvider.getConfiguration(PrincipalConfiguration.class));
        assertSame(defaultConfig, securityProvider.getConfiguration(PrincipalConfiguration.class));

        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof PrincipalConfiguration) {
                assertSame(defaultConfig, sc);
            }
        }
        assertTrue(cpc.getConfigurations().contains(pc));
    }

    @Test
    public void testUnbinPrincipalConfiguration() {
        CompositePrincipalConfiguration cpc = (CompositePrincipalConfiguration) securityProvider.getConfiguration(PrincipalConfiguration.class);

        PrincipalConfiguration pc = Mockito.mock(PrincipalConfiguration.class);
        when(pc.getParameters()).thenReturn(ConfigurationParameters.EMPTY);

        securityProvider.bindPrincipalConfiguration(pc);
        assertTrue(cpc.getConfigurations().contains(pc));

        securityProvider.unbindPrincipalConfiguration(pc);
        assertFalse(cpc.getConfigurations().contains(pc));
    }


    @Test
    public void testBindUserConfiguration() {
        UserConfiguration uc = Mockito.mock(UserConfiguration.class);
        securityProvider.bindUserConfiguration(uc);

        assertSame(uc, securityProvider.getConfiguration(UserConfiguration.class));
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof UserConfiguration) {
                assertSame(uc, sc);
            }
        }
    }

    @Test
    public void testUnBindUserConfiguration() {
        UserConfiguration uc = Mockito.mock(UserConfiguration.class);
        securityProvider.bindUserConfiguration(uc);
        securityProvider.unbindUserConfiguration(uc);

        assertNull(securityProvider.getConfiguration(UserConfiguration.class));
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof UserConfiguration) {
                fail();
            }
        }
    }

    @Test
    public void testBindAuthenticationConfiguration() {
        AuthenticationConfiguration ac = Mockito.mock(AuthenticationConfiguration.class);
        securityProvider.bindAuthenticationConfiguration(ac);

        assertSame(ac, securityProvider.getConfiguration(AuthenticationConfiguration.class));
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof AuthenticationConfiguration) {
                assertSame(ac, sc);
            }
        }
    }

    @Test
    public void testUnBindAuthenticationConfiguration() {
        AuthenticationConfiguration ac = Mockito.mock(AuthenticationConfiguration.class);
        securityProvider.bindAuthenticationConfiguration(ac);
        securityProvider.unbindAuthenticationConfiguration(ac);

        assertNull(securityProvider.getConfiguration(AuthenticationConfiguration.class));
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof AuthenticationConfiguration) {
                fail();
            }
        }
    }

    @Test
    public void testBindPrivilegeConfiguration() {
        PrivilegeConfiguration pc = Mockito.mock(PrivilegeConfiguration.class);
        securityProvider.bindPrivilegeConfiguration(pc);

        assertSame(pc, securityProvider.getConfiguration(PrivilegeConfiguration.class));
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof PrivilegeConfiguration) {
                assertSame(pc, sc);
            }
        }
    }

    @Test
    public void testUnBindPrivilegeConfiguration() {
        PrivilegeConfiguration pc = Mockito.mock(PrivilegeConfiguration.class);
        securityProvider.bindPrivilegeConfiguration(pc);
        securityProvider.unbindPrivilegeConfiguration(pc);

        assertNull(securityProvider.getConfiguration(PrivilegeConfiguration.class));
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            if (sc instanceof PrivilegeConfiguration) {
                fail();
            }
        }
    }
}