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

import java.util.Collections;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.token.CompositeTokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TokenLoginModuleCredentialsSupportTest extends AbstractSecurityTest {

    private String userId;
    private TokenConfigurationImpl tc;
    private CredentialsSupport credentialsSupport;

    @Before
    public void before() throws Exception {
        super.before();

        userId = getTestUser().getID();
        credentialsSupport = new TestCredentialsSupport(userId);

        CompositeTokenConfiguration composite = ((CompositeTokenConfiguration) getSecurityProvider().getConfiguration(TokenConfiguration.class));
        tc = (TokenConfigurationImpl) composite.getDefaultConfig();
        tc.bindCredentialsSupport(credentialsSupport);
    }

    @Override
    public void after() throws Exception {
        try {
            tc.unbindCredentialsSupport(credentialsSupport);
        } finally {
            root.refresh();
            super.after();
        }
    }

    @Override
    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                AppConfigurationEntry tokenEntry = new AppConfigurationEntry(
                        TokenLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                        Collections.<String, Object>emptyMap());

                AppConfigurationEntry testEntry = new AppConfigurationEntry(
                        TestLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                        ImmutableMap.of("credsSupport", credentialsSupport));

                AppConfigurationEntry defaultEntry = new AppConfigurationEntry(
                        LoginModuleImpl.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        Collections.<String, Object>emptyMap());

                return new AppConfigurationEntry[] {tokenEntry, testEntry, defaultEntry};
            }
        };
    }

    @Test
    public void testCustomCredentials() throws Exception {
        TestCredentialsSupport.Creds credentials = new TestCredentialsSupport.Creds();

        ContentSession cs = login(credentials);
        try {
            assertEquals(userId, cs.getAuthInfo().getUserID());

            Map<String, ?> attributes = credentialsSupport.getAttributes(credentials);
            String token = attributes.get(TokenConstants.TOKEN_ATTRIBUTE).toString();
            assertFalse(token.isEmpty());

            cs.close();

            cs = login(new TokenCredentials(token));
            assertEquals(userId, cs.getAuthInfo().getUserID());
        } finally {
            cs.close();
        }
    }
}
