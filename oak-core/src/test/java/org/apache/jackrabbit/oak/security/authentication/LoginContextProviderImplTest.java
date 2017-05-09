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
package org.apache.jackrabbit.oak.security.authentication;

import java.security.Principal;
import java.security.PrivilegedAction;
import java.util.HashMap;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.GuestLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.JaasLoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthContext;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LoginContextProviderImplTest extends AbstractSecurityTest {

    private LoginContextProviderImpl lcProvider;

    @Override
    public void before() throws Exception {
        super.before();

        lcProvider = new LoginContextProviderImpl(AuthenticationConfiguration.DEFAULT_APP_NAME, ConfigurationParameters.EMPTY, getContentRepository(), getSecurityProvider(), new DefaultWhiteboard());
    }

    @Test
    public void testGetLoginContext() throws Exception {
        LoginContext ctx = lcProvider.getLoginContext(new SimpleCredentials(getTestUser().getID(), getTestUser().getID().toCharArray()), root.getContentSession().getWorkspaceName());

        Subject subject = ctx.getSubject();
        assertNotNull(subject);
        assertFalse(subject.isReadOnly());
        assertTrue(subject.getPrincipals().isEmpty());
    }

    @Test
    public void getLoginContextWithoutCredentials() throws Exception {
        LoginContext ctx = lcProvider.getLoginContext(null, root.getContentSession().getWorkspaceName());
        assertNotNull(ctx);
        assertTrue(ctx instanceof JaasLoginContext);
    }

    @Test
    public void testGetPreAuthLoginContext() {
        Subject subject = new Subject(true, ImmutableSet.<Principal>of(), ImmutableSet.of(), ImmutableSet.of());
        LoginContext ctx = Subject.doAs(subject, new PrivilegedAction<LoginContext>() {
            @Override
            public LoginContext run() {
                try {
                    return lcProvider.getLoginContext(null, null);
                } catch (LoginException e) {
                    throw new RuntimeException();
                }
            }

        });

        assertTrue(ctx instanceof PreAuthContext);
        assertSame(subject, ctx.getSubject());
    }

    @Test
    public void testGetLoginContextWithInvalidProviderConfig() throws Exception {
        ConfigurationParameters params = ConfigurationParameters.of(AuthenticationConfiguration.PARAM_CONFIG_SPI_NAME, "invalid");
        LoginContextProvider provider = new LoginContextProviderImpl(AuthenticationConfiguration.DEFAULT_APP_NAME, params, getContentRepository(), getSecurityProvider(), new DefaultWhiteboard());

        // invalid configuration falls back to default configuration
        LoginContext ctx = provider.getLoginContext(new SimpleCredentials(getTestUser().getID(), getTestUser().getID().toCharArray()), null);
        ctx.login();
    }

    @Test
    public void testGetLoginContextWithConfigurationPreset() throws Exception {
        Configuration.setConfiguration(new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String applicationName) {
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(GuestLoginModule.class.getName(), AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL, new HashMap())
                };
            }
        });

        LoginContextProvider provider = new LoginContextProviderImpl(AuthenticationConfiguration.DEFAULT_APP_NAME, ConfigurationParameters.EMPTY, getContentRepository(), getSecurityProvider(), new DefaultWhiteboard());
        LoginContext ctx = provider.getLoginContext(null, null);
        ctx.login();

        assertFalse(ctx.getSubject().getPublicCredentials(GuestCredentials.class).isEmpty());
    }

    @Test
    public void testGetLoginContextTwice() throws Exception {
        Configuration.setConfiguration(new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String applicationName) {
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(GuestLoginModule.class.getName(), AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL, new HashMap())
                };
            }
        });

        LoginContextProvider provider = new LoginContextProviderImpl(AuthenticationConfiguration.DEFAULT_APP_NAME, ConfigurationParameters.EMPTY, getContentRepository(), getSecurityProvider(), new DefaultWhiteboard());
        provider.getLoginContext(null, null);
        LoginContext ctx = provider.getLoginContext(null, null);

        ctx.login();
        assertFalse(ctx.getSubject().getPublicCredentials(GuestCredentials.class).isEmpty());
    }
}