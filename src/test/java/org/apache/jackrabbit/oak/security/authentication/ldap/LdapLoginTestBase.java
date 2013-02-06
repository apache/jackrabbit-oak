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

import java.util.HashMap;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginModule;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncMode;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class LdapLoginTestBase extends AbstractSecurityTest {

    protected static final InternalLdapServer LDAP_SERVER = new InternalLdapServer();

    protected static final String USER_ID = "foobar";
    protected static final String USER_PWD = "foobar";
    protected static final String USER_FIRSTNAME = "Foo";
    protected static final String USER_LASTNAME = "Bar";
    protected static final String USER_ATTR = "givenName";
    protected static final String USER_PROP = "profile/name";
    protected static final String GROUP_PROP = "profile/member";
    protected static final String GROUP_NAME = "foobargroup";

    protected static String GROUP_DN;

    //initialize LDAP server only once (fast, but might turn out to be not sufficiently flexible in the future)
    protected static final boolean USE_COMMON_LDAP_FIXTURE = true;

    protected final HashMap<String, Object> options = new HashMap<String, Object>();

    protected UserManager userManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        if (USE_COMMON_LDAP_FIXTURE) {
            LDAP_SERVER.setUp();
            createLdapFixture();
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (USE_COMMON_LDAP_FIXTURE) {
            LDAP_SERVER.tearDown();
        }
    }

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

    @Before
    public void before() throws Exception {
        super.before();

        if (!USE_COMMON_LDAP_FIXTURE) {
            LDAP_SERVER.setUp();
        }

        options.put(LdapSettings.KEY_HOST, "127.0.0.1");
        options.put(LdapSettings.KEY_PORT, String.valueOf(LDAP_SERVER.getPort()));
        options.put(LdapSettings.KEY_AUTHDN, ServerDNConstants.ADMIN_SYSTEM_DN);
        options.put(LdapSettings.KEY_AUTHPW, InternalLdapServer.ADMIN_PW);
        options.put(LdapSettings.KEY_USERROOT, ServerDNConstants.USERS_SYSTEM_DN);
        options.put(LdapSettings.KEY_GROUPROOT, ServerDNConstants.GROUPS_SYSTEM_DN);
        options.put(LdapSettings.KEY_AUTOCREATEUSER + USER_ATTR, USER_PROP);
        options.put(LdapSettings.KEY_AUTOCREATEGROUP + InternalLdapServer.GROUP_MEMBER_ATTR, GROUP_PROP);
        options.put(LdapSettings.KEY_GROUPFILTER, "(objectclass=" + InternalLdapServer.GROUP_CLASS_ATTR + ')');
        options.put(LdapSettings.KEY_GROUPMEMBERSHIPATTRIBUTE, InternalLdapServer.GROUP_MEMBER_ATTR);
        options.put(ExternalLoginModule.PARAM_SYNC_MODE, SyncMode.CREATE_USER);

        userManager = securityProvider.getUserConfiguration().getUserManager(root, NamePathMapper.DEFAULT);
    }

    @After
    public void after() throws Exception {

        if (!USE_COMMON_LDAP_FIXTURE) {
            LDAP_SERVER.tearDown();
        }

        try {
            Authorizable a = userManager.getAuthorizable(USER_ID);
            if (a != null) {
                a.remove();
            }
            if (GROUP_DN != null) {
                a = userManager.getAuthorizable(GROUP_DN);
                if (a != null) {
                    a.remove();
                }
            }
            root.commit();
        } finally {
            root.refresh();
            super.after();
        }
    }

    @Test
    public void testLoginFailed() throws Exception {

        if (!USE_COMMON_LDAP_FIXTURE) {
            createLdapFixture();
        }

        try {
            ContentSession cs = login(new SimpleCredentials(USER_ID, new char[0]));
            cs.close();
            fail("login failure expected");
        } catch (LoginException e) {
            // success
        } finally {
            assertNull(userManager.getAuthorizable(USER_ID));
        }
    }

    @Test
    public void testSyncCreateUser() throws Exception {

        if (!USE_COMMON_LDAP_FIXTURE) {
            createLdapFixture();
        }

        options.put(ExternalLoginModule.PARAM_SYNC_MODE, SyncMode.CREATE_USER);

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

    @Test
    public void testSyncCreateGroup() throws Exception {

        if (!USE_COMMON_LDAP_FIXTURE) {
            createLdapFixture();
        }

        options.put(ExternalLoginModule.PARAM_SYNC_MODE, SyncMode.CREATE_GROUP);

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, USER_PWD.toCharArray()));

            root.refresh();
            assertNull(userManager.getAuthorizable(USER_ID));
            assertNull(userManager.getAuthorizable(GROUP_DN));
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testSyncCreateUserAndGroups() throws Exception {

        if (!USE_COMMON_LDAP_FIXTURE) {
            createLdapFixture();
        }

        options.put(ExternalLoginModule.PARAM_SYNC_MODE, new String[]{SyncMode.CREATE_USER, SyncMode.CREATE_GROUP});

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
    public void testNoSync() throws Exception {

        if (!USE_COMMON_LDAP_FIXTURE) {
            createLdapFixture();
        }

        options.put(ExternalLoginModule.PARAM_SYNC_MODE, "");

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, USER_PWD.toCharArray()));

            root.refresh();
            assertNull(userManager.getAuthorizable(USER_ID));
            assertNull(userManager.getAuthorizable(GROUP_DN));
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    protected static void createLdapFixture() throws Exception {
        LDAP_SERVER.addMember(
                GROUP_DN = LDAP_SERVER.addGroup(GROUP_NAME),
                LDAP_SERVER.addUser(USER_FIRSTNAME, USER_LASTNAME, USER_ID, USER_PWD));
    }
}
