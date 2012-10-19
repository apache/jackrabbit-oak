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

import java.util.Collections;
import javax.jcr.GuestCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.oak.AbstractOakTest;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.GuestLoginModule;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * LoginTest...
 */
public class GuestDefaultLoginModuleTest extends AbstractOakTest {

    SecurityProvider securityProvider = new SecurityProviderImpl();

    @Before
    public void before() throws Exception {
        super.before();
        Configuration.setConfiguration(new GuestDefaultConfiguration());
    }

    @After
    public void after() throws Exception {
        Configuration.setConfiguration(null);
    }

    @Override
    protected ContentRepository createRepository() {
        return new Oak(createMicroKernelWithInitialContent()).with(securityProvider).createContentRepository();
    }

    @Test
    public void testNullLogin() throws Exception {
        ContentSession cs = getContentRepository().login(null, null);
        try {
            AuthInfo authInfo = cs.getAuthInfo();
            String anonymousID = UserUtility.getAnonymousId(securityProvider.getUserConfiguration().getConfigurationParameters());
            assertEquals(anonymousID, authInfo.getUserID());
        } finally {
            cs.close();
        }
    }

    @Test
    public void testGuestLogin() throws Exception {
        ContentSession cs = getContentRepository().login(new GuestCredentials(), null);
        try {
            AuthInfo authInfo = cs.getAuthInfo();
            String anonymousID = UserUtility.getAnonymousId(securityProvider.getUserConfiguration().getConfigurationParameters());
            assertEquals(anonymousID, authInfo.getUserID());
        } finally {
            cs.close();
        }
    }

    private class GuestDefaultConfiguration extends Configuration {

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
            AppConfigurationEntry guestEntry = new AppConfigurationEntry(
                    GuestLoginModule.class.getName(),
                    AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
                    Collections.<String, Object>emptyMap());

            AppConfigurationEntry defaultEntry = new AppConfigurationEntry(
                    LoginModuleImpl.class.getName(),
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    Collections.<String, Object>emptyMap());

            return new AppConfigurationEntry[] {guestEntry, defaultEntry};
        }
    }
}