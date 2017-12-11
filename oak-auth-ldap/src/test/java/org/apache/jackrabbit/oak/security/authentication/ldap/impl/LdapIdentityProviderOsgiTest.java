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

import javax.jcr.GuestCredentials;

import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LdapIdentityProviderOsgiTest {

    @Rule
    public final OsgiContext context = new OsgiContext();

    private LdapIdentityProvider provider = new LdapIdentityProvider();

    @Before
    public void before() throws Exception {
        context.registerInjectActivateService(provider);
    }

    @Test(expected = ExternalIdentityException.class)
    public void testFromExternalIdentityRefForeign() throws Exception {
        provider.fromExternalIdentityRef(new ExternalIdentityRef("id", "anotherName"));
    }

    @Test
    public void testFromExternalIdentityRef() throws Exception {
        assertEquals("id", provider.fromExternalIdentityRef(new ExternalIdentityRef("id", provider.getName())));
    }

    @Test
    public void testGetName() {
        assertEquals(LdapProviderConfig.PARAM_NAME_DEFAULT, provider.getName());
    }

    @Test
    public void testAuthenticateOtherCredentials() throws Exception {
        assertNull(provider.authenticate(new GuestCredentials()));
    }

    @Test
    public void testGetIdentityForeingRef() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef("id", "anotherName");
        assertNull(provider.getIdentity(ref));
    }

    @Test
    public void testGetDeclaredGroupRefsForeignRef() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef("id", "anotherName");
        assertTrue(provider.getDeclaredGroupRefs(ref).isEmpty());
    }

    @Test
    public void testGetDeclaredMemberRefsForeignRef() throws Exception {
        ExternalIdentityRef ref = new ExternalIdentityRef("id", "anotherName");
        assertTrue(provider.getDeclaredMemberRefs(ref).isEmpty());
    }

    @Test(expected = ExternalIdentityException.class)
    public void testGetUserMissingConnection() throws Exception {
        provider.getUser("user");
    }

    @Test(expected = ExternalIdentityException.class)
    public void testGetGroupMissingConnection() throws Exception {
        provider.getGroup("gr");
    }

    @Test(expected = ExternalIdentityException.class)
    public void testListGroupsMissingConnections() throws Exception {
        provider.listGroups().hasNext();
    }

    @Test(expected = ExternalIdentityException.class)
    public void testListUsersMissingConnections() throws Exception {
        provider.listUsers().hasNext();
    }
}