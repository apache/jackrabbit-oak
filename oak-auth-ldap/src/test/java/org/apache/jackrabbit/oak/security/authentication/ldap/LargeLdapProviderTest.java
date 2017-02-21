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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.jackrabbit.oak.security.authentication.ldap.impl.LdapIdentityProvider;
import org.apache.jackrabbit.oak.security.authentication.ldap.impl.LdapProviderConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.util.Text;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class LargeLdapProviderTest {

    protected static final InternalLdapServer LDAP_SERVER = new InternalLdapServer();

    //initialize LDAP server only once (fast, but might turn out to be not sufficiently flexible in the future)
    protected static final boolean USE_COMMON_LDAP_FIXTURE = false;

    public static final String IDP_NAME = "ldap";

    protected static final String GROUP_NAME = "foobargroup";

    protected static String GROUP_DN;

    protected static String[] TEST_MEMBERS;

    protected static int NUM_USERS = 2222;

    protected static int SIZE_LIMIT = 50;

    protected LdapIdentityProvider idp;

    protected LdapProviderConfig providerConfig;

    @BeforeClass
    public static void beforeClass() throws Exception {
        if (USE_COMMON_LDAP_FIXTURE) {
            LDAP_SERVER.setUp();
            LDAP_SERVER.setMaxSizeLimit(SIZE_LIMIT);
            initLdapFixture(LDAP_SERVER);
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
        if (!USE_COMMON_LDAP_FIXTURE) {
            LDAP_SERVER.setUp();
            LDAP_SERVER.setMaxSizeLimit(SIZE_LIMIT);
            initLdapFixture(LDAP_SERVER);
        }
        idp = createIDP();
    }

    @After
    public void after() throws Exception {
        if (!USE_COMMON_LDAP_FIXTURE) {
            LDAP_SERVER.tearDown();
        }
        if (idp != null) {
            idp.close();
            idp = null;
        }
    }

    protected LdapIdentityProvider createIDP() {
        providerConfig = new LdapProviderConfig()
                .setName(IDP_NAME)
                .setHostname("127.0.0.1")
                .setPort(LDAP_SERVER.getPort())
                .setBindDN(USER_DN)
                .setBindPassword(USER_PWD)
                .setGroupMemberAttribute("member");

        providerConfig.getUserConfig()
                .setBaseDN(AbstractServer.EXAMPLE_DN)
                .setObjectClasses("inetOrgPerson");

        providerConfig.getGroupConfig()
                .setBaseDN(AbstractServer.EXAMPLE_DN)
                .setObjectClasses(InternalLdapServer.GROUP_CLASS_ATTR);

        providerConfig.getAdminPoolConfig().setMaxActive(0);
        providerConfig.getUserPoolConfig().setMaxActive(0);
        return new LdapIdentityProvider(providerConfig);
    }

    protected static final String USER_ID = "foobar";
    protected static final String USER_PWD = "foobar";
    protected static final String USER_FIRSTNAME = "Foo";
    protected static final String USER_LASTNAME = "Bar";

    protected static String USER_DN;

    protected static void initLdapFixture(InternalLdapServer server) throws Exception {
        ArrayList<String> members = new ArrayList<String>();

        USER_DN = LDAP_SERVER.addUser(USER_FIRSTNAME, USER_LASTNAME, USER_ID, USER_PWD);
        GROUP_DN = server.addGroup(GROUP_NAME, USER_DN);
        members.add(USER_DN);

        List<String> userDNs = new ArrayList<String>();
        for (int i = 0; i < NUM_USERS; i++) {
            final String userId = "user-" + i;
            String userDN = server.addUser(userId, "test", userId, "test");
            userDNs.add(userDN);
            members.add(userDN);
        }
        LDAP_SERVER.addMembers(GROUP_DN, userDNs);
        TEST_MEMBERS = members.toArray(new String[members.size()]);
    }


    @Test
    public void testGetMembers() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(GROUP_DN, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("Group instance", id instanceof ExternalGroup);

        ExternalGroup grp = (ExternalGroup) id;
        assertIfEquals("Group members", TEST_MEMBERS, grp.getDeclaredMembers());
    }

    @Test
    @Ignore("OAK-2874")
    public void testListUsers() throws Exception {
        Iterator<ExternalUser> users = idp.listUsers();
        List<ExternalIdentityRef> refs = new ArrayList<ExternalIdentityRef>();
        while (users.hasNext()) {
            refs.add(users.next().getExternalId());
        }
        assertIfEquals("Test users", TEST_MEMBERS, refs);
    }

    public static void assertIfEquals(String message, String[] expected, Iterable<ExternalIdentityRef> result) {
        List<String> dns = new LinkedList<String>();
        for (ExternalIdentityRef ref: result) {
            dns.add(ref.getId());
        }
        Collections.sort(dns);
        Arrays.sort(expected);
        String exp = Text.implode(expected, ",\n");
        String res = Text.implode(dns.toArray(new String[dns.size()]), ",\n");
        assertEquals(message, exp, res);
    }

}
