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
package org.apache.jackrabbit.oak.security.authentication.ldap.impl;

import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.jackrabbit.oak.security.authentication.ldap.InternalLdapServer;
import org.apache.jackrabbit.oak.security.authentication.ldap.LdapServerClassLoader;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.util.Text;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;

import javax.jcr.SimpleCredentials;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public abstract class AbstractLdapIdentityProviderTest {

    public static final String TEST_USER0_DN = "cn=Rat Ratterson,ou=users,ou=system";
    public static final String TEST_USER0_UID = "ratty";

    public static final String TEST_USER1_DN = "cn=Horatio Hornblower,ou=users,ou=system";
    public static final String TEST_USER1_UID = "hhornblo";
    public static final String TEST_USER1_PATH = "cn=Horatio Hornblower/ou=users/ou=system";

    public static final String TEST_USER2_DN = "cn=William Bush,ou=users,ou=system";
    public static final String TEST_USER3_DN = "cn=Thomas Quist,ou=users,ou=system";
    public static final String TEST_USER4_DN = "cn=Moultrie Crystal,ou=users,ou=system";

    public static final String TEST_USER5_UID = "=007=";
    public static final String TEST_USER5_DN = "cn=Special\\, Agent [007],ou=users,ou=system";
    public static final String TEST_USER5_PATH = "cn=Special\\, Agent %5B007%5D/ou=users/ou=system";

    public static final String TEST_GROUP1_DN = "cn=HMS Lydia,ou=crews,ou=groups,ou=system";
    public static final String TEST_GROUP1_NAME = "HMS Lydia";
    public static final String[] TEST_GROUP1_MEMBERS = {
            TEST_USER0_DN, TEST_USER1_DN, TEST_USER2_DN, TEST_USER3_DN, TEST_USER4_DN
    };

    public static final String TEST_GROUP2_DN = "cn=HMS Victory,ou=crews,ou=groups,ou=system";
    public static final String TEST_GROUP2_NAME = "HMS Victory";

    public static final String TEST_GROUP3_DN = "cn=HMS Bounty,ou=crews,ou=groups,ou=system";
    public static final String TEST_GROUP3_NAME = "HMS Bounty";

    public static final String[] TEST_USER0_GROUPS = {TEST_GROUP1_DN, TEST_GROUP2_DN, TEST_GROUP3_DN};
    public static final String[] TEST_USER1_GROUPS = {TEST_GROUP1_DN};

    //loaded by a separate ClassLoader unavailable to the client (needed because the server is using old libraries)
    protected LdapServerClassLoader.Proxy proxy;

    private static final String TUTORIAL_LDIF = "apache-ds-tutorial.ldif";
    public static final String IDP_NAME = "ldap";

    public static final String[] DEFAULT_USER_PROPERTIES = new String[] { "objectclass", "uid", "givenname", "description", "sn", "cn"};

    protected LdapIdentityProvider idp;
    protected LdapProviderConfig providerConfig;
    
    protected boolean useSSL;
    protected boolean useTLS;

    @Before
    public void before() throws Exception {
        LdapServerClassLoader serverClassLoader = LdapServerClassLoader.createServerClassLoader();
        proxy = serverClassLoader.createAndSetupServer(useSSL);
        proxy.loadLdif(getClass().getResourceAsStream(TUTORIAL_LDIF));
        idp = createIDP();
    }

    @After
    public void after() throws Exception {
        proxy.tearDown();
        if (idp != null) {
            idp.close();
            idp = null;
        }
    }

    @NotNull
    protected LdapIdentityProvider createIDP() {
        //The attribute "mail" is excluded deliberately
        return createIDP(DEFAULT_USER_PROPERTIES);
    }

    @NotNull
    protected LdapIdentityProvider createIDP(@NotNull String[] userProperties) {
        providerConfig = createProviderConfig(userProperties);
        return new LdapIdentityProvider(providerConfig);
    }

    @NotNull
    protected LdapProviderConfig createProviderConfig(@NotNull String[] userProperties) {
        LdapProviderConfig providerConfig = new LdapProviderConfig()
                .setName(IDP_NAME)
                .setHostname("127.0.0.1")
                .setPort(proxy.port)
                .setBindDN(ServerDNConstants.ADMIN_SYSTEM_DN)
                .setBindPassword(InternalLdapServer.ADMIN_PW)
                .setGroupMemberAttribute("uniquemember")
                .setCustomAttributes(userProperties);

        providerConfig.getUserConfig()
                .setBaseDN(ServerDNConstants.USERS_SYSTEM_DN)
                .setObjectClasses("inetOrgPerson");
        providerConfig.getGroupConfig()
                .setBaseDN(ServerDNConstants.GROUPS_SYSTEM_DN)
                .setObjectClasses("groupOfUniqueNames");

        providerConfig.getAdminPoolConfig().setMaxActive(0);
        providerConfig.getUserPoolConfig().setMaxActive(0);
        return providerConfig;
    }

    public static void assertAuthenticate(@NotNull LdapIdentityProvider idp, @NotNull String id, @NotNull String expectedId, @NotNull String expectedUserRef) throws Exception {
        assertAuthenticate(idp, new SimpleCredentials(id, "pass".toCharArray()), expectedId, expectedUserRef);
    }

    public static void assertAuthenticate(@NotNull LdapIdentityProvider idp, @NotNull SimpleCredentials credentials, @NotNull String expectedId, @NotNull String expectedUserRef) throws Exception {
        ExternalUser user = idp.authenticate(credentials);
        assertNotNull("User 1 must authenticate", user);
        assertEquals("User Ref", expectedUserRef, ((LdapUser)user).getEntry().getDn().getName());
        assertEquals("User Id", expectedId, user.getExternalId().getId());
    }

    public static void assertIfEquals(@NotNull String message, @NotNull String[] expected, @NotNull Iterable<ExternalIdentityRef> result) {
        List<String> dns = new LinkedList<>();
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