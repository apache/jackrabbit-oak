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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import java.util.Collections;
import java.util.Map;
import javax.jcr.Credentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.security.authentication.token.TokenConfigurationImpl;
import org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.credentials.CredentialsSupport;
import org.apache.jackrabbit.oak.spi.security.authentication.token.CompositeTokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TokenExternalLoginModuleTest extends CustomCredentialsSupportTest {

    private CredentialsSupport credentialsSupport;
    private Registration registration;
    private TokenConfigurationImpl tc;

    @Before
    public void before() throws Exception {
        super.before();

        credentialsSupport = getCredentialsSupport();

        // NOTE: should be replaced by proper OSGi setup
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
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(
                                TokenLoginModule.class.getName(),
                                AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                                Collections.<String, Object>emptyMap()),

                        new AppConfigurationEntry(
                                ExternalLoginModule.class.getName(),
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                options)
                };
            }
        };
    }

    @Test
    public void testTokenCreation() throws Exception {
        Credentials creds = createTestCredentials();
        assertTrue(credentialsSupport.setAttributes(creds, ImmutableMap.<String, Object>of(".token", "")));

        String expectedUserId = credentialsSupport.getUserId(creds);

        ContentSession cs = login(creds);
        try {
            assertEquals(expectedUserId, cs.getAuthInfo().getUserID());

            Map<String, ?> attributes = credentialsSupport.getAttributes(creds);
            String token = attributes.get(".token").toString();
            assertFalse(token.isEmpty());

            root.refresh();
            User user = getUserManager(root).getAuthorizable(expectedUserId, User.class);

            Tree tokenParent = root.getTree(user.getPath()).getChild(".tokens");
            assertTrue(tokenParent.exists());
            assertEquals(1, tokenParent.getChildrenCount(100));
        } finally {
            cs.close();
        }
    }

    @Test
    public void testTokenLogin() throws Exception {
        Credentials creds = createTestCredentials();
        assertTrue(credentialsSupport.setAttributes(creds, ImmutableMap.<String, Object>of(".token", "")));

        String expectedUserId = credentialsSupport.getUserId(creds);

        ContentSession cs = login(creds);
        try {
            String token = credentialsSupport.getAttributes(creds).get(".token").toString();
            cs.close();

            cs = login(new TokenCredentials(token));
            assertEquals(expectedUserId, cs.getAuthInfo().getUserID());
        } finally {
            cs.close();
        }
    }
}