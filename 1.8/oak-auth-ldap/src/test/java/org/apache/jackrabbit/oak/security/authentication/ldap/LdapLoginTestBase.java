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

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.security.authentication.ldap.impl.LdapIdentityProvider;
import org.apache.jackrabbit.oak.security.authentication.ldap.impl.LdapProviderConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalLoginModuleTestBase;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class LdapLoginTestBase extends ExternalLoginModuleTestBase {

    protected static final InternalLdapServer LDAP_SERVER = new InternalLdapServer();

    protected static final String USER_ID = "foobar";
    protected static final String USER_PWD = "foobar";
    protected static final String USER_FIRSTNAME = "Foo";
    protected static final String USER_LASTNAME = "Bar";
    protected static final String USER_ATTR = "givenName";
    protected static final String USER_PROP = "profile/name";
    protected static final String GROUP_NAME = "foobargroup";

    protected static String GROUP_DN;

    protected static int NUM_CONCURRENT_LOGINS = 10;
    private static String[] CONCURRENT_TEST_USERS = new String[NUM_CONCURRENT_LOGINS];
    private static String[] CONCURRENT_GROUP_TEST_USERS = new String[NUM_CONCURRENT_LOGINS];

    //initialize LDAP server only once (fast, but might turn out to be not sufficiently flexible in the future)
    protected static final boolean USE_COMMON_LDAP_FIXTURE = true;

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

    @Before
    public void before() throws Exception {
        super.before();

        if (!USE_COMMON_LDAP_FIXTURE) {
            LDAP_SERVER.setUp();
            createLdapFixture();
        }

        UserConfiguration uc = securityProvider.getConfiguration(UserConfiguration.class);
        userManager = uc.getUserManager(root, NamePathMapper.DEFAULT);
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

    @Override
    protected void setSyncConfig(DefaultSyncConfig cfg) {
        if (cfg != null) {
            cfg.user().getPropertyMapping().put(USER_PROP, USER_ATTR);
        }
        super.setSyncConfig(cfg);
    }

    @Override
    protected ExternalIdentityProvider createIDP() {
        LdapProviderConfig cfg = new LdapProviderConfig()
                .setName("ldap")
                .setHostname("127.0.0.1")
                .setPort(LDAP_SERVER.getPort())
                .setBindDN(ServerDNConstants.ADMIN_SYSTEM_DN)
                .setBindPassword(InternalLdapServer.ADMIN_PW)
                .setGroupMemberAttribute(InternalLdapServer.GROUP_MEMBER_ATTR);

        cfg.getUserConfig()
                .setBaseDN(AbstractServer.EXAMPLE_DN)
                .setObjectClasses("inetOrgPerson");
        cfg.getGroupConfig()
                .setBaseDN(AbstractServer.EXAMPLE_DN)
                .setObjectClasses(InternalLdapServer.GROUP_CLASS_ATTR);

        cfg.getAdminPoolConfig().setMaxActive(0);
        cfg.getUserPoolConfig().setMaxActive(0);
        return new LdapIdentityProvider(cfg);
    }

    @Override
    protected void destroyIDP() {
        ((LdapIdentityProvider) idp).close();
    }

    /**
     * Null login must fail.
     *
     * @throws Exception
     * @see org.apache.jackrabbit.oak.security.authentication.ldap.GuestTokenDefaultLdapLoginModuleTest
     */
    @Test
    public void testNullLogin() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(null);
            fail("Expected null login to fail.");
        } catch (LoginException e) {
            // success
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testLoginFailed() throws Exception {
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
        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, USER_PWD.toCharArray()));

            root.refresh();
            Authorizable user = userManager.getAuthorizable(USER_ID);
            assertNotNull(user);
            assertTrue(user.hasProperty(USER_PROP));
            Tree userTree = cs.getLatestRoot().getTree(user.getPath());
            assertFalse(userTree.hasProperty(UserConstants.REP_PASSWORD));

            assertNull(userManager.getAuthorizable(GROUP_DN));
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testSyncCreateUserCaseInsensitive() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID.toUpperCase(), USER_PWD.toCharArray()));

            root.refresh();
            Authorizable user = userManager.getAuthorizable(USER_ID);
            assertNotNull(user);
            assertTrue(user.hasProperty(USER_PROP));
            Tree userTree = cs.getLatestRoot().getTree(user.getPath());
            assertFalse(userTree.hasProperty(UserConstants.REP_PASSWORD));

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
        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, USER_PWD.toCharArray()));

            root.refresh();
            assertNotNull(userManager.getAuthorizable(USER_ID));
            assertNotNull(userManager.getAuthorizable(GROUP_NAME));
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testSyncUpdate() throws Exception {
        // create user upfront in order to test update mode
        Authorizable user = userManager.createUser(USER_ID, null);
        ExternalUser externalUser = idp.getUser(USER_ID);
        user.setProperty("rep:externalId", new ValueFactoryImpl(root, NamePathMapper.DEFAULT).createValue(externalUser.getExternalId().getString()));
        root.commit();

        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, USER_PWD.toCharArray()));

            root.refresh();
            user = userManager.getAuthorizable(USER_ID);
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
    public void testLoginSetsAuthInfo() throws Exception {
        ContentSession cs = null;
        try {
            SimpleCredentials sc = new SimpleCredentials(USER_ID, USER_PWD.toCharArray());
            sc.setAttribute("attr", "val");

            cs = login(sc);
            AuthInfo ai = cs.getAuthInfo();

            assertEquals(USER_ID, ai.getUserID());
            assertEquals("val", ai.getAttribute("attr"));
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testPrincipalsFromAuthInfo() throws Exception {
        ContentSession cs = null;
        try {
            SimpleCredentials sc = new SimpleCredentials(USER_ID, USER_PWD.toCharArray());
            sc.setAttribute("attr", "val");

            cs = login(sc);
            AuthInfo ai = cs.getAuthInfo();

            root.refresh();
            PrincipalProvider pp = getSecurityProvider().getConfiguration(PrincipalConfiguration.class).getPrincipalProvider(root, NamePathMapper.DEFAULT);
            Set<? extends Principal> expected = pp.getPrincipals(USER_ID);
            assertEquals(3, expected.size());
            assertEquals(expected, ai.getPrincipals());

        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    @Test
    public void testReLogin() throws Exception {
        ContentSession cs = null;
        try {
            cs = login(new SimpleCredentials(USER_ID, USER_PWD.toCharArray()));

            root.refresh();
            Authorizable user = userManager.getAuthorizable(USER_ID);
            assertNotNull(user);
            assertFalse(root.getTree(user.getPath()).hasProperty(UserConstants.REP_PASSWORD));

            cs.close();
            // login again
            cs = login(new SimpleCredentials(USER_ID, USER_PWD.toCharArray()));
            root.refresh();
            assertEquals(USER_ID, cs.getAuthInfo().getUserID());
        } finally {
            if (cs != null) {
                cs.close();
            }
            options.clear();
        }
    }

    @Test
    public void testConcurrentLogin() throws Exception {
        concurrentLogin(CONCURRENT_TEST_USERS);
    }

    @Test
    public void testConcurrentLoginSameGroup() throws Exception {
        concurrentLogin(CONCURRENT_GROUP_TEST_USERS);
    }

    private void concurrentLogin(String [] users) throws Exception {
        final List<Exception> exceptions = new ArrayList<Exception>();
        List<Thread> workers = new ArrayList<Thread>();
        for (String userId: users) {
            final String uid = userId;
            workers.add(new Thread(new Runnable() {
                public void run() {
                    try {
                        login(new SimpleCredentials(uid, USER_PWD.toCharArray())).close();
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            }));
        }
        for (Thread t : workers) {
            t.start();
        }
        for (Thread t : workers) {
            t.join();
        }
        for (Exception e : exceptions) {
            e.printStackTrace();
        }
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
    }

    protected static void createLdapFixture() throws Exception {
        String userDN = LDAP_SERVER.addUser(USER_FIRSTNAME, USER_LASTNAME, USER_ID, USER_PWD);
        GROUP_DN = LDAP_SERVER.addGroup(GROUP_NAME, userDN);
        for (int i = 0; i < NUM_CONCURRENT_LOGINS * 2; i++) {
            final String userId = "user-" + i;
            userDN = LDAP_SERVER.addUser(userId, "test", userId, USER_PWD);
            if (i%2 == 0) {
                CONCURRENT_GROUP_TEST_USERS[i/2] = userId;
                LDAP_SERVER.addMember(GROUP_DN, userDN);
            } else {
                CONCURRENT_TEST_USERS[i/2] = userId;
            }
        }
    }
}
