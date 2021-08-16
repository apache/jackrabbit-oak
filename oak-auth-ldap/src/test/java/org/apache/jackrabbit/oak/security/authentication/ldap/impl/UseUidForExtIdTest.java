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

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.SimpleCredentials;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class UseUidForExtIdTest extends AbstractLdapIdentityProviderTest {

    @Override
    @NotNull
    protected LdapProviderConfig createProviderConfig(@NotNull String[] userProperties) {
        LdapProviderConfig config = super.createProviderConfig(userProperties);
        config.setUseUidForExtId(true);
        return config;
    }

    @Test
    public void testGetUserByRef() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_USER1_UID, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("User instance", id instanceof ExternalUser);
        assertEquals("User ID", TEST_USER1_UID, id.getId());
    }

    @Test
    public void testAuthenticate() throws Exception {
        assertAuthenticate(idp, TEST_USER1_UID, TEST_USER1_UID, TEST_USER1_DN);
    }

    @Test
    public void testAuthenticateCaseInsensitive() throws Exception {
        SimpleCredentials creds = new SimpleCredentials(TEST_USER1_UID.toUpperCase(), "pass".toCharArray());
        assertAuthenticate(idp, creds, TEST_USER1_UID.toUpperCase(), TEST_USER1_DN);
    }

    @Test
    public void testGetUserByForeignRef() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_USER1_UID, "foobar");
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
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_GROUP1_NAME, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertEquals("Group Name", TEST_GROUP1_NAME, id.getId());
    }

    @Test
    public void testGetMembers() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_GROUP1_NAME, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("Group instance", id instanceof ExternalGroup);
        ExternalGroup grp = (ExternalGroup) id;
        assertIfEquals("Group members", TEST_GROUP1_MEMBERS, grp.getDeclaredMembers());
    }

    @Test
    public void testGetGroups() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_USER1_UID, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("User instance", id instanceof ExternalUser);
        assertIfEquals("Groups", TEST_USER1_GROUPS, id.getDeclaredGroups());
    }

    @Test
    public void testGetGroups2() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef(TEST_USER0_UID, IDP_NAME);
        ExternalIdentity id = idp.getIdentity(ref);
        assertTrue("User instance", id instanceof ExternalUser);
        assertIfEquals("Groups", TEST_USER0_GROUPS, id.getDeclaredGroups());
    }
}
