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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.directory.api.util.Strings;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.hamcrest.Matchers;
import org.junit.Test;

import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.security.authentication.ldap.impl.LdapProviderConfig.PARAM_USER_EXTRA_FILTER_DEFAULT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LdapIdentityProviderTest extends AbstractLdapIdentityProviderTest {

    private static final String ERRONEOUS_LDIF = "erroneous.ldif";

    @Test
    public void testGetUserByRef() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_USER1_DN, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("User instance", id instanceof ExternalUser);
        assertEquals("User ID", TEST_USER1_UID, id.getId());
    }

    @Test
    public void testListUsers() throws Exception {
        Iterator<ExternalUser> users = idp.listUsers();
        Iterator<String> ids = Iterators.transform(users, externalUser -> externalUser.getId());

        Set<String> expectedIds = Set.of(TEST_USER0_UID, TEST_USER1_UID, TEST_USER5_UID, "hnelson", "thardy", "tquist", "fchristi", "wbush", "cbuckley", "jhallett", "mchrysta", "wbligh", "jfryer");
        assertEquals(expectedIds, CollectionUtils.toSet(ids));
    }

    @Test
    public void testListUsersWithExtraFilter() throws Exception {
        providerConfig.getUserConfig().setExtraFilter(PARAM_USER_EXTRA_FILTER_DEFAULT);
        Iterator<ExternalUser> users = idp.listUsers();
        Iterator<String> ids = Iterators.transform(users, externalUser -> externalUser.getId());

        Set<String> expectedIds = Set.of(TEST_USER0_UID, TEST_USER1_UID, TEST_USER5_UID, "hnelson", "thardy", "tquist", "fchristi", "wbush", "cbuckley", "jhallett", "mchrysta", "wbligh", "jfryer");
        assertEquals(expectedIds, CollectionUtils.toSet(ids));
    }

    /**
     * Test case to reproduce OAK-3396 where an ldap user entry
     * without a uid caused a NullpointerException in LdapIdentityProvider.createUser
     */
    @Test
    public void testListUsersWithMissingUid() throws Exception {
        // the ERRONEOUS_LDIF contains an entry without uid
        proxy.loadLdif(getClass().getResourceAsStream(ERRONEOUS_LDIF));
        Iterator<ExternalUser> users = idp.listUsers();
        // make sure we got a result
        assertTrue(users.hasNext());
        // without the LdapInvalidAttributeValueException a NPE would result here:
        while(users.hasNext()) {
            ExternalUser user = users.next();
            // the 'Faulty Entry' of the ERRONEOUS_LDIF should be filtered out
            // (by LdapIdentityProvider.listUsers.getNext())
            assertFalse(user.getPrincipalName().startsWith("cn=Faulty Entry"));
        }
    }

    @Test
    public void testGetUserByUserId() throws Exception {
        ExternalUser user = idp.getUser(TEST_USER1_UID);
        assertNotNull("User 1 must exist", user);
        assertEquals("User Ref", TEST_USER1_DN, ((LdapUser)user).getEntry().getDn().getName());
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
        assertAuthenticate(idp, TEST_USER1_UID, TEST_USER1_DN, TEST_USER1_DN);
    }

    @Test
    public void testAuthenticateCaseInsensitive() throws Exception {
        SimpleCredentials creds = new SimpleCredentials(TEST_USER1_UID.toUpperCase(), "pass".toCharArray());
        assertAuthenticate(idp, creds, TEST_USER1_DN, TEST_USER1_DN);
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
        assertEquals("Group Ref", TEST_GROUP1_DN, ((LdapIdentity)group).getEntry().getDn().getName());
    }

    @Test
    public void testGetGroupByUnknownName() throws Exception {
        ExternalGroup group = idp.getGroup("unknown");
        assertNull(group);
    }

    @Test
    public void testGetDeclaredMembersByRef() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_GROUP1_DN, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("Group instance", id instanceof ExternalGroup);
        ExternalGroup grp = (ExternalGroup) id;
        assertIfEquals("Group members", TEST_GROUP1_MEMBERS, grp.getDeclaredMembers());
    }


    @Test
    public void testGetDeclaredMembers() throws Exception {
        ExternalGroup gr = idp.getGroup(TEST_GROUP1_NAME);
        Iterable<ExternalIdentityRef> memberrefs = gr.getDeclaredMembers();
        Iterable<String> memberIds = Iterables.transform(memberrefs, externalIdentityRef -> externalIdentityRef.getId());

        Set<String> expected = Set.of(TEST_GROUP1_MEMBERS);
        assertEquals(expected, CollectionUtils.toSet(memberIds));
    }

    @Test
    public void testGetDeclaredMembersInvalidMemberAttribute() throws Exception {
        providerConfig.setGroupMemberAttribute("invalid");

        ExternalGroup gr = idp.getGroup(TEST_GROUP1_NAME);
        Iterable<ExternalIdentityRef> memberrefs = gr.getDeclaredMembers();
        assertTrue(Iterables.isEmpty(memberrefs));
    }

    @Test
    public void testGetDeclaredGroupsByRef() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_USER1_DN, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("User instance", id instanceof ExternalUser);
        assertIfEquals("Groups", TEST_USER1_GROUPS, id.getDeclaredGroups());
    }

    @Test
    public void testGetDeclaredGroupsByRef2() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_USER0_DN, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("User instance", id instanceof ExternalUser);
        assertIfEquals("Groups", TEST_USER0_GROUPS, id.getDeclaredGroups());
    }

    @Test
    public void testGetDeclaredGroupMissingIdAttribute() throws Exception {
        providerConfig.getGroupConfig().setIdAttribute(null);

        ExternalUser user = idp.getUser(TEST_USER1_UID);
        Iterable<ExternalIdentityRef> groupRefs = user.getDeclaredGroups();
        Iterable<String> groupIds = Iterables.transform(groupRefs, externalIdentityRef -> externalIdentityRef.getId());
        assertEquals(Set.of(TEST_USER1_GROUPS), CollectionUtils.toSet(groupIds));
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

    @Test
    public void testListGroups() throws Exception {
        Iterator<ExternalGroup> groups = idp.listGroups();
        Iterator<String> ids = Iterators.transform(groups, externalGroup -> externalGroup.getId());

        Set<String> expectedIds = Set.of(TEST_GROUP1_NAME, TEST_GROUP2_NAME, TEST_GROUP3_NAME, "Administrators");
        assertEquals(expectedIds, CollectionUtils.toSet(ids));
    }

    @Test
    public void testListGroupsWithEmptyExtraFilter() throws Exception {
        providerConfig.getGroupConfig().setExtraFilter("");

        Iterator<ExternalGroup> groups = idp.listGroups();
        Iterator<String> ids = Iterators.transform(groups, externalGroup -> externalGroup.getId());

        Set<String> expectedIds = Set.of(TEST_GROUP1_NAME, TEST_GROUP2_NAME, TEST_GROUP3_NAME, "Administrators");
        assertEquals(expectedIds, CollectionUtils.toSet(ids));
    }
}
