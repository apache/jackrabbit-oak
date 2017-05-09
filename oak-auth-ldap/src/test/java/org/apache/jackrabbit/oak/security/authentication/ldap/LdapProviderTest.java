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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import org.apache.directory.api.util.Strings;
import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.jackrabbit.oak.security.authentication.ldap.impl.LdapIdentityProvider;
import org.apache.jackrabbit.oak.security.authentication.ldap.impl.LdapProviderConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.util.Text;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class LdapProviderTest {

    protected static final InternalLdapServer LDAP_SERVER = new InternalLdapServer();

    //initialize LDAP server only once (fast, but might turn out to be not sufficiently flexible in the future)
    protected static final boolean USE_COMMON_LDAP_FIXTURE = false;

    private static final String TUTORIAL_LDIF = "apache-ds-tutorial.ldif";

    private static final String ERRONEOUS_LDIF = "erroneous.ldif";

    public static final String IDP_NAME = "ldap";

    protected LdapIdentityProvider idp;

    protected LdapProviderConfig providerConfig;

    @BeforeClass
    public static void beforeClass() throws Exception {
        if (USE_COMMON_LDAP_FIXTURE) {
            LDAP_SERVER.setUp();
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
        //The attribute "mail" is excluded deliberately
        return createIDP(new String[] { "objectclass", "uid", "givenname", "description", "sn"});
    }

    protected LdapIdentityProvider createIDP(String[] userProperties) {
        providerConfig = new LdapProviderConfig()
                .setName(IDP_NAME)
                .setHostname("127.0.0.1")
                .setPort(LDAP_SERVER.getPort())
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
        return new LdapIdentityProvider(providerConfig);
    }

    protected static void initLdapFixture(InternalLdapServer server) throws Exception {
        InputStream tutorialLDIF = LdapProviderTest.class.getResourceAsStream(TUTORIAL_LDIF);
        server.loadLdif(tutorialLDIF);
    }

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

    @Test
    public void testGetUserByRef() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_USER1_DN, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("User instance", id instanceof ExternalUser);
        assertEquals("User ID", TEST_USER1_UID, id.getId());
    }
    
    /**
     * Test case to reproduce OAK-3396 where an ldap user entry
     * without a uid caused a NullpointerException in LdapIdentityProvider.createUser
     */
    @Test
    public void testListUsersWithMissingUid() throws Exception {
        // the ERRONEOUS_LDIF contains an entry without uid
        InputStream erroneousDIF = LdapProviderTest.class.getResourceAsStream(ERRONEOUS_LDIF);
        LDAP_SERVER.loadLdif(erroneousDIF);
        Iterator<ExternalUser> users = idp.listUsers();
        // without the LdapInvalidAttributeValueException a NPE would result here:
        while(users.hasNext()) {
            ExternalUser user = users.next();
            // the 'Faulty Entry' of the ERRONEOUS_LDIF should be filtered out
            // (by LdapIdentityProvider.listUsers.getNext())
            assertTrue(!user.getPrincipalName().startsWith("cn=Faulty Entry"));
        }
    }

    @Test
    public void testGetUserByUserId() throws Exception {
        ExternalUser user = idp.getUser(TEST_USER1_UID);
        assertNotNull("User 1 must exist", user);
        assertEquals("User Ref", TEST_USER1_DN, user.getExternalId().getId());
    }

    @Test
    public void testGetUserProperties() throws Exception {
        ExternalUser user = idp.getUser(TEST_USER1_UID);
        assertNotNull("User 1 must exist", user);

        Map<String, ?> properties = user.getProperties();
        assertThat((Map<String, Collection<String>>) properties,
                Matchers.<String, Collection<String>>hasEntry(
                        Matchers.equalTo("objectclass"),
                        Matchers.containsInAnyOrder( "inetOrgPerson", "top", "person", "organizationalPerson")));
        assertThat(properties, Matchers.<String, Object>hasEntry("uid", "hhornblo"));
        assertThat(properties, Matchers.<String, Object>hasEntry("givenname", "Horatio"));
        assertThat(properties, Matchers.<String, Object>hasEntry("description", "Capt. Horatio Hornblower, R.N"));
        assertThat(properties, Matchers.<String, Object>hasEntry("sn", "Hornblower"));

        assertThat(properties, Matchers.not(Matchers.<String, Object>hasEntry("mail", "hhornblo@royalnavy.mod.uk")));
    }

    @Test
    public void testAuthenticate() throws Exception {
        SimpleCredentials creds = new SimpleCredentials(TEST_USER1_UID, "pass".toCharArray());
        ExternalUser user = idp.authenticate(creds);
        assertNotNull("User 1 must authenticate", user);
        assertEquals("User Ref", TEST_USER1_DN, user.getExternalId().getId());
    }

    @Test
    public void testAuthenticateValidateFalseFalse() throws Exception {
        providerConfig.getAdminPoolConfig()
                .setMaxActive(2)
                .setLookupOnValidate(false);
        providerConfig.getUserPoolConfig()
                .setMaxActive(2)
                .setLookupOnValidate(false);
        idp.close();
        idp = new LdapIdentityProvider(providerConfig);

        SimpleCredentials creds = new SimpleCredentials(TEST_USER1_UID, "pass".toCharArray());
        for (int i=0; i<8; i++) {
            ExternalUser user = idp.authenticate(creds);
            assertNotNull("User 1 must authenticate", user);
            assertEquals("User Ref", TEST_USER1_DN, user.getExternalId().getId());
        }
    }

    @Test
    public void testAuthenticateValidateFalseTrue() throws Exception {
        providerConfig.getAdminPoolConfig()
                .setMaxActive(2)
                .setLookupOnValidate(false);
        providerConfig.getUserPoolConfig()
                .setMaxActive(2)
                .setLookupOnValidate(true);
        idp.close();
        idp = new LdapIdentityProvider(providerConfig);

        SimpleCredentials creds = new SimpleCredentials(TEST_USER1_UID, "pass".toCharArray());
        for (int i=0; i<8; i++) {
            ExternalUser user = idp.authenticate(creds);
            assertNotNull("User 1 must authenticate", user);
            assertEquals("User Ref", TEST_USER1_DN, user.getExternalId().getId());
        }
    }

    @Test
    public void testAuthenticateValidateTrueFalse() throws Exception {
        providerConfig.getAdminPoolConfig()
                .setMaxActive(2)
                .setLookupOnValidate(true);
        providerConfig.getUserPoolConfig()
                .setMaxActive(2)
                .setLookupOnValidate(false);
        idp.close();
        idp = new LdapIdentityProvider(providerConfig);

        SimpleCredentials creds = new SimpleCredentials(TEST_USER1_UID, "pass".toCharArray());
        for (int i=0; i<8; i++) {
            ExternalUser user = idp.authenticate(creds);
            assertNotNull("User 1 must authenticate (i=" + i + ")", user);
            assertEquals("User Ref", TEST_USER1_DN, user.getExternalId().getId());
        }
    }

    @Test
    public void testAuthenticateValidateTrueTrue() throws Exception {
        providerConfig.getAdminPoolConfig()
                .setMaxActive(2)
                .setLookupOnValidate(true);
        providerConfig.getUserPoolConfig()
                .setMaxActive(2)
                .setLookupOnValidate(true);
        idp.close();
        idp = new LdapIdentityProvider(providerConfig);

        SimpleCredentials creds = new SimpleCredentials(TEST_USER1_UID, "pass".toCharArray());
        for (int i=0; i<8; i++) {
            ExternalUser user = idp.authenticate(creds);
            assertNotNull("User 1 must authenticate (i=" + i + ")", user);
            assertEquals("User Ref", TEST_USER1_DN, user.getExternalId().getId());
        }
    }

    @Test
    public void testAuthenticateCaseInsensitive() throws Exception {
        SimpleCredentials creds = new SimpleCredentials(TEST_USER1_UID.toUpperCase(), "pass".toCharArray());
        ExternalUser user = idp.authenticate(creds);
        assertNotNull("User 1 must authenticate", user);
        assertEquals("User Ref", TEST_USER1_DN, user.getExternalId().getId());
    }

    @Test
    public void testAuthenticateFail() throws Exception {
        SimpleCredentials creds = new SimpleCredentials(TEST_USER1_UID, "foobar".toCharArray());
        try {
            idp.authenticate(creds);
            fail("Authenticate must fail with LoginException for wrong password");
        } catch (LoginException e) {
            // ok
        }
    }

    @Test
    public void testAuthenticateMissing() throws Exception {
        SimpleCredentials creds = new SimpleCredentials("foobar" + TEST_USER1_UID, "pass".toCharArray());
        ExternalUser user = idp.authenticate(creds);
        assertNull("Authenticate must return NULL for unknown user", user);
    }

    @Test
    public void testGetUserByForeignRef() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_USER1_DN, "foobar");
        ExternalIdentity id = idp.getIdentity(ref);
        assertNull("Foreign ref must be null", id);
    }

    @Test
    public void testGetUnknownUserByRef() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef("bla=foo," + TEST_USER1_DN, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertNull("Unknown user must return null", id);
    }

    @Test
    public void testGetGroupByRef() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_GROUP1_DN, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("Group instance", id instanceof ExternalGroup);
        assertEquals("Group Name", TEST_GROUP1_NAME, id.getId());
    }

    @Test
    public void testGetGroupByName() throws Exception {
        ExternalGroup group = idp.getGroup(TEST_GROUP1_NAME);
        assertNotNull("Group 1 must exist", group);
        assertEquals("Group Ref", TEST_GROUP1_DN, group.getExternalId().getId());
    }


    @Test
    public void testGetMembers() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_GROUP1_DN, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("Group instance", id instanceof ExternalGroup);

        ExternalGroup grp = (ExternalGroup) id;
        assertIfEquals("Group members", TEST_GROUP1_MEMBERS, grp.getDeclaredMembers());
    }

    @Test
    public void testGetGroups() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_USER1_DN, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("User instance", id instanceof ExternalUser);
        assertIfEquals("Groups", TEST_USER1_GROUPS, id.getDeclaredGroups());
    }

    @Test
    public void testGetGroups2() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_USER0_DN, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("User instance", id instanceof ExternalUser);
        assertIfEquals("Groups", TEST_USER0_GROUPS, id.getDeclaredGroups());
    }

    @Test
    public void testNullIntermediatePath() throws Exception {
        providerConfig.getUserConfig().setMakeDnPath(false);
        ExternalUser user = idp.getUser(TEST_USER1_UID);
        assertNotNull("User 1 must exist", user);
        assertNull("Intermediate path must be null", user.getIntermediatePath());
    }

    @Test
    public void testSplitDNIntermediatePath() throws Exception {
        providerConfig.getUserConfig().setMakeDnPath(true);
        ExternalUser user = idp.getUser(TEST_USER1_UID);
        assertNotNull("User 1 must exist", user);
        assertEquals("Intermediate path must be the split dn", TEST_USER1_PATH, user.getIntermediatePath());
    }

    @Test
    public void testSplitDNIntermediatePath2() throws Exception {
        providerConfig.getUserConfig().setMakeDnPath(true);
        ExternalUser user = idp.getUser(TEST_USER5_UID);
        assertNotNull("User 5 must exist", user);
        assertEquals("Intermediate path must be the split dn", TEST_USER5_PATH, user.getIntermediatePath());
    }

    @Test
    public void testRemoveEmptyString() throws Exception {
        providerConfig.setCustomAttributes(new String[] {"a", Strings.EMPTY_STRING, "b" });
        assertArrayEquals("Array must not contain empty strings", new String[] {"a", "b" }, providerConfig.getCustomAttributes());
    }

    @Test
    public void testResolvePrincipalNameUser() throws ExternalIdentityException {
        ExternalUser user = idp.getUser(TEST_USER5_UID);
        assertNotNull(user);
        assertEquals(user.getPrincipalName(), idp.fromExternalIdentityRef(user.getExternalId()));
    }

    @Test
    public void testResolvePrincipalNameGroup() throws ExternalIdentityException {
        ExternalGroup gr = idp.getGroup(TEST_GROUP1_NAME);
        assertNotNull(gr);

        assertEquals(gr.getPrincipalName(), idp.fromExternalIdentityRef(gr.getExternalId()));
    }

    @Test(expected = ExternalIdentityException.class)
    public void testResolvePrincipalNameForeignExtId() throws Exception {
        idp.fromExternalIdentityRef(new ExternalIdentityRef("anyId", "anotherProviderName"));
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
