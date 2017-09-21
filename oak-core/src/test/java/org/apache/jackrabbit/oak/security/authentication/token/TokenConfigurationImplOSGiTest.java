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

import java.util.Hashtable;
import javax.jcr.SimpleCredentials;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;
import org.osgi.framework.ServiceRegistration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TokenConfigurationImplOSGiTest extends AbstractSecurityTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private final TokenConfigurationImpl tokenConfiguration = new TokenConfigurationImpl();

    private SimpleCredentials sc;

    @Override
    public void before() throws Exception {
        super.before();

        tokenConfiguration.setSecurityProvider(getSecurityProvider());

        context.registerInjectActivateService(tokenConfiguration, ImmutableMap.<String, Object>of(
                TokenProvider.PARAM_TOKEN_EXPIRATION, 25,
                TokenProvider.PARAM_TOKEN_LENGTH, 4));

        sc = new SimpleCredentials(getTestUser().getID(), new char[0]);
        sc.setAttribute(TokenConstants.TOKEN_ATTRIBUTE, "");
    }

    @Test
    public void testGetParameters() {
        ConfigurationParameters params = tokenConfiguration.getParameters();
        assertEquals(25, params.getConfigValue(TokenProvider.PARAM_TOKEN_EXPIRATION, TokenProviderImpl.DEFAULT_TOKEN_EXPIRATION).longValue());
        assertEquals(4, params.getConfigValue(TokenProvider.PARAM_TOKEN_LENGTH, TokenProviderImpl.DEFAULT_KEY_SIZE).intValue());
    }

    @Test
    public void testDefaultCredentialsSupport() throws Exception {
        TokenProvider tp = tokenConfiguration.getTokenProvider(root);
        assertTrue(tp.doCreateToken(sc));
    }

    @Test
    public void testBindCredentialsSupport() {
        context.registerService(CredentialsSupport.class, new TestCredentialsSupport(sc.getUserID()));

        TokenProvider tp = tokenConfiguration.getTokenProvider(root);

        assertFalse(tp.doCreateToken(sc));
        assertTrue(tp.doCreateToken(new TestCredentialsSupport.Creds()));
    }

    @Test
    public void testUnbindCredentialsSupport() {
        CredentialsSupport testSupport = new TestCredentialsSupport(sc.getUserID());
        ServiceRegistration registration = context.bundleContext().registerService(CredentialsSupport.class.getName(), testSupport, new Hashtable());

        TokenProvider tp = tokenConfiguration.getTokenProvider(root);
        assertFalse(tp.doCreateToken(sc));
        assertTrue(tp.doCreateToken(new TestCredentialsSupport.Creds()));

        registration.unregister();

        tp = tokenConfiguration.getTokenProvider(root);
        assertTrue(tp.doCreateToken(sc));
        assertFalse(tp.doCreateToken(new TestCredentialsSupport.Creds()));

    }
}