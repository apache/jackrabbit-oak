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
package org.apache.jackrabbit.oak.security.authentication.ldap;

import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncMode;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Ignore //ignore for the moment because "mvn test" runs into PermGen memory issues
public class LdapLoginStandaloneTest extends LdapLoginTestBase {

    @Override
    protected Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(
                                LdapLoginModule.class.getName(),
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                options)
                };
            }
        };
    }
    @Test
    public void testSyncUpdateAndGroups() throws Exception {

        if (!USE_COMMON_LDAP_FIXTURE) {
            createLdapFixture();
        }

        options.put(ExternalLoginModule.PARAM_SYNC_MODE, new String[]{SyncMode.UPDATE, SyncMode.CREATE_GROUP});

        // create user upfront in order to test update mode
        userManager.createUser(USER_ID, USER_PWD);
        root.commit();

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, USER_PWD.toCharArray()));

            root.refresh();
            Authorizable user = userManager.getAuthorizable(USER_ID);
            assertNotNull(user);
            assertTrue(user.hasProperty(USER_PROP));
            Authorizable group = userManager.getAuthorizable(GROUP_DN);
            assertTrue(group.hasProperty(GROUP_PROP));
            assertNotNull(group);
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testDefaultSync() throws Exception {

        if (!USE_COMMON_LDAP_FIXTURE) {
            createLdapFixture();
        }

        options.put(ExternalLoginModule.PARAM_SYNC_MODE, null);

        // create user upfront in order to test update mode
        userManager.createUser(USER_ID, USER_PWD);
        root.commit();

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, USER_PWD.toCharArray()));

            root.refresh();
            Authorizable user = userManager.getAuthorizable(USER_ID);
            assertNotNull(user);
            assertTrue(user.hasProperty(USER_PROP));
            Authorizable group = userManager.getAuthorizable(GROUP_DN);
            assertTrue(group.hasProperty(GROUP_PROP));
            assertNotNull(group);
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testSyncUpdate() throws Exception {

        if (!USE_COMMON_LDAP_FIXTURE) {
            createLdapFixture();
        }

        options.put(ExternalLoginModule.PARAM_SYNC_MODE, SyncMode.UPDATE);

        // create user upfront in order to test update mode
        userManager.createUser(USER_ID, USER_PWD);
        root.commit();

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, USER_PWD.toCharArray()));

            root.refresh();
            Authorizable user = userManager.getAuthorizable(USER_ID);
            assertNotNull(user);
            assertTrue(user.hasProperty(USER_PROP));
            assertNull(userManager.getAuthorizable(GROUP_DN));
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }
}
