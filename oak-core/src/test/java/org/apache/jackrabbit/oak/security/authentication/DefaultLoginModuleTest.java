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
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractOakTest;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.UserUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * LoginTest...
 */
public class DefaultLoginModuleTest extends AbstractOakTest {

    SecurityProvider securityProvider = new SecurityProviderImpl();

    ContentSession admin;

    @Before
    public void before() throws Exception {
        super.before();

        admin = createAdminSession();
        Configuration.setConfiguration(new DefaultConfiguration());
    }

    @After
    public void after() throws Exception {
        Configuration.setConfiguration(null);
        if (admin != null) {
            admin.close();
        }
    }

    @Override
    protected ContentRepository createRepository() {
        return new Oak(createMicroKernelWithInitialContent()).with(securityProvider).createContentRepository();
    }

    @Test
    public void testNullLogin() throws Exception {
        ContentSession cs = null;
        try {
            cs = getContentRepository().login(null, null);
            fail("Null login should fail");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
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

    @Test
    public void testAnonymousLogin() throws Exception {
        String anonymousID = UserUtility.getAnonymousId(securityProvider.getUserConfiguration().getConfigurationParameters());

        Root root = admin.getLatestRoot();
        UserManager userMgr = securityProvider.getUserConfiguration().getUserManager(root, NamePathMapper.DEFAULT);

        // verify initial user-content looks like expected
        Authorizable anonymous = userMgr.getAuthorizable(anonymousID);
        assertNotNull(anonymous);
        assertFalse(root.getTree(anonymous.getPath()).hasProperty(UserConstants.REP_PASSWORD));

        ContentSession cs = null;
        try {
            cs = getContentRepository().login(new SimpleCredentials(anonymousID, new char[0]), null);
            fail("Login with anonymousID should fail since the initial setup doesn't provide a password.");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testUserLogin() throws Exception {
        String anonymousID = UserUtility.getAnonymousId(securityProvider.getUserConfiguration().getConfigurationParameters());

        Root root = admin.getLatestRoot();
        UserManager userManager = securityProvider.getUserConfiguration().getUserManager(root, NamePathMapper.DEFAULT);

        ContentSession cs = null;
        User user = null;
        try {
            user = userManager.createUser("test", "pw");
            root.commit();

            cs = getContentRepository().login(new SimpleCredentials("test", "pw".toCharArray()), null);
            AuthInfo authInfo = cs.getAuthInfo();
            assertEquals("test", authInfo.getUserID());
        } finally {
            if (user != null) {
                user.remove();
                root.commit();
            }
            if (cs != null) {
                cs.close();
            }
        }
    }

    private class DefaultConfiguration extends Configuration {

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
            AppConfigurationEntry defaultEntry = new AppConfigurationEntry(
                    LoginModuleImpl.class.getName(),
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    Collections.<String, Object>emptyMap());

            return new AppConfigurationEntry[] {defaultEntry};
        }
    }
}