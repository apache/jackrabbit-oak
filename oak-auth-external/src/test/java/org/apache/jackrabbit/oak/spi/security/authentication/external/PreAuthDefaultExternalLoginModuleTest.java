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
package org.apache.jackrabbit.oak.spi.security.authentication.external;

import java.util.HashMap;
import java.util.Map;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Testing improvements made for <a href="https://issues.apache.org/jira/browse/OAK-3508">OAK-3508</a>
 */
public class PreAuthDefaultExternalLoginModuleTest extends ExternalLoginModuleTestBase {

    private Map<String, Object> preAuthOptions = new HashMap<>();

    @Before
    public void before() throws Exception {
        super.before();
    }

    @After
    public void after() throws Exception {
        super.after();
    }

    /**
     * Example {
     *    your.org.PreAuthenticationLoginModule optional;
     *    org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl optional;
     *    org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalLoginModule sufficient
     *              sync.handlerName="your-synchandler_name"
     *              idp.name="your_idp_name";
     *    };
     */
    @Override
    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                AppConfigurationEntry entry1 = new AppConfigurationEntry(
                        PreAuthLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
                        preAuthOptions);

                AppConfigurationEntry entry2 = new AppConfigurationEntry(
                        LoginModuleImpl.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL,
                        new HashMap<String, Object>());

                AppConfigurationEntry entry3 = new AppConfigurationEntry(
                        ExternalLoginModule.class.getName(),
                        AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                        options);
                return new AppConfigurationEntry[]{entry1, entry2, entry3};
            }
        };
    }

    @Test
    public void testNonExistingUser() throws Exception {
        PreAuthCredentials creds = new PreAuthCredentials("nonExisting");

        ContentSession cs = null;
        try {
            cs = login(creds);
            fail();
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
            assertEquals(PreAuthCredentials.PRE_AUTH_DONE, creds.getMessage());

            root.refresh();
            assertNull(getUserManager(root).getAuthorizable(TestIdentityProvider.ID_TEST_USER));
        }
    }

    @Test
    public void testLocalUser() throws Exception {
        User testUser = getTestUser();
        PreAuthCredentials creds = new PreAuthCredentials(testUser.getID());

        ContentSession cs = null;
        try {
            cs = login(creds);

            assertEquals(PreAuthCredentials.PRE_AUTH_DONE, creds.getMessage());
            assertEquals(testUser.getID(), cs.getAuthInfo().getUserID());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testExternalUser() throws Exception {
        PreAuthCredentials creds = new PreAuthCredentials(TestIdentityProvider.ID_TEST_USER);

        ContentSession cs = null;
        try {
            cs = login(creds);

            assertEquals(PreAuthCredentials.PRE_AUTH_DONE, creds.getMessage());
            assertEquals(TestIdentityProvider.ID_TEST_USER, cs.getAuthInfo().getUserID());

            // user needs to be synchronized upon login
            root.refresh();
            assertNotNull(getUserManager(root).getAuthorizable(TestIdentityProvider.ID_TEST_USER));
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testExistingExternalReSync() throws Exception {
        // sync user upfront
        UserManager uMgr = getUserManager(root);
        SyncContext syncContext = new DefaultSyncContext(syncConfig, idp, uMgr, getValueFactory(root));
        SyncResult result = syncContext.sync(idp.getUser(TestIdentityProvider.ID_TEST_USER));
        long lastSynced = result.getIdentity().lastSynced();
        root.commit();

        PreAuthCredentials creds = new PreAuthCredentials(TestIdentityProvider.ID_TEST_USER);
        ContentSession cs = null;
        try {
            // wait until the synced user is expired
            waitUntilExpired(uMgr.getAuthorizable(TestIdentityProvider.ID_TEST_USER, User.class), root, syncConfig.user().getExpirationTime());

            cs = login(creds);

            assertEquals(PreAuthCredentials.PRE_AUTH_DONE, creds.getMessage());
            assertEquals(TestIdentityProvider.ID_TEST_USER, cs.getAuthInfo().getUserID());

            root.refresh();
            User u = getUserManager(root).getAuthorizable(TestIdentityProvider.ID_TEST_USER, User.class);
            assertNotNull(u);

            // user _should_ be re-synced
            assertFalse(lastSynced == DefaultSyncContext.createSyncedIdentity(u).lastSynced());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testExistingExternalNoSync() throws Exception {
        // prevent expiration of the user
        syncConfig.user().setExpirationTime(Long.MAX_VALUE);

        // sync user upfront
        SyncContext syncContext = new DefaultSyncContext(syncConfig, idp, getUserManager(root), getValueFactory(root));
        SyncResult result = syncContext.sync(idp.getUser(TestIdentityProvider.ID_TEST_USER));
        long lastSynced = result.getIdentity().lastSynced();
        root.commit();

        PreAuthCredentials creds = new PreAuthCredentials(TestIdentityProvider.ID_TEST_USER);
        ContentSession cs = null;
        try {
            cs = login(creds);

            assertEquals(PreAuthCredentials.PRE_AUTH_DONE, creds.getMessage());
            assertEquals(TestIdentityProvider.ID_TEST_USER, cs.getAuthInfo().getUserID());

            root.refresh();
            User u = getUserManager(root).getAuthorizable(TestIdentityProvider.ID_TEST_USER, User.class);
            assertNotNull(u);

            // user _should_ not have been re-synced
            assertEquals(lastSynced, DefaultSyncContext.createSyncedIdentity(u).lastSynced());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testForeign() throws Exception {
        // sync foreign user into the repository
        // NOTE: that should be considered a bug by the tool that does the sync
        // as it uses an IDP that is not configured with the login-chain!
        ExternalIdentityProvider foreign = new TestIdentityProvider("foreign");
        SyncContext syncContext = new DefaultSyncContext(syncConfig, foreign, getUserManager(root), getValueFactory(root));
        SyncResult result = syncContext.sync(foreign.getUser(TestIdentityProvider.ID_TEST_USER));
        long lastSynced = result.getIdentity().lastSynced();
        root.commit();

        PreAuthCredentials creds = new PreAuthCredentials(TestIdentityProvider.ID_TEST_USER);
        ContentSession cs = null;
        try {
            // login should succeed due the fact that the  _LoginModuleImpl_ succeeds for
            // an existing authorizable if _pre_auth_ is enabled.
            cs = login(creds);

            assertEquals(PreAuthCredentials.PRE_AUTH_DONE, creds.getMessage());

            // foreign user _must_ not have been touched by the _ExternalLoginModule_
            root.refresh();
            User u = getUserManager(root).getAuthorizable(TestIdentityProvider.ID_TEST_USER, User.class);
            assertNotNull(u);

            assertEquals(lastSynced, DefaultSyncContext.createSyncedIdentity(u).lastSynced());
        } finally {
            if (cs != null) {
                cs.close();
            }

        }
    }

    @Test
    public void testInvalidPreAuthCreds() throws Exception {
        PreAuthCredentials creds = new PreAuthCredentials(null);
        ContentSession cs = null;
        try {
            cs = login(creds);
            fail();
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
            assertEquals(PreAuthCredentials.PRE_AUTH_FAIL, creds.getMessage());

            root.refresh();
            assertNull(getUserManager(root).getAuthorizable(TestIdentityProvider.ID_TEST_USER));
        }
    }

    @Test
    public void testGuest() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(new GuestCredentials());
            assertEquals(UserConstants.DEFAULT_ANONYMOUS_ID, cs.getAuthInfo().getUserID());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testSimpleLocal() throws Exception {
        User testUser = getTestUser();
        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(testUser.getID(), testUser.getID().toCharArray()));
            assertEquals(testUser.getID(), cs.getAuthInfo().getUserID());
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testSimpleLocalDisabled() throws Exception {
        User testUser = getTestUser();
        testUser.disable("disable");
        root.commit();

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(testUser.getID(), testUser.getID().toCharArray()));
            fail();
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testSimpleNonExisting() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials("nonExisting", new char[0]));
            fail();
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testSimpleExternal() throws Exception {
        // verify that authentication against the IDP succeeds with the given creds.
        Credentials creds = new SimpleCredentials(TestIdentityProvider.ID_TEST_USER, new char[0]);
        ExternalUser externalUser = idp.authenticate(creds);
        assertNotNull(externalUser);
        assertEquals(TestIdentityProvider.ID_TEST_USER, externalUser.getId());

        // => repo login must also succeed and the user must be synced.
        ContentSession cs = null;
        try {
            cs = login(creds);
            assertEquals(TestIdentityProvider.ID_TEST_USER, cs.getAuthInfo().getUserID());

            root.refresh();
            User u = getUserManager(root).getAuthorizable(TestIdentityProvider.ID_TEST_USER, User.class);
            assertNotNull(u);
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }
}

