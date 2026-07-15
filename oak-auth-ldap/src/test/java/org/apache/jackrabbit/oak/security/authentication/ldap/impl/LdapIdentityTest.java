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

import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class LdapIdentityTest {

    final LdapIdentityProvider idp = mock(LdapIdentityProvider.class);
    final String id = "id";
    final ExternalIdentityRef ref = new ExternalIdentityRef(id, "providerName");
    final String path = "intermediate/path";
    final Entry entry = mock(Entry.class);

    final LdapIdentity identity = mockIdentity(idp, ref, id, path, entry);

    LdapIdentity mockIdentity(@NotNull LdapIdentityProvider idp, @NotNull ExternalIdentityRef ref, @NotNull String id, @NotNull String path, @NotNull Entry entry) {
        return mock(LdapIdentity.class, withSettings().useConstructor(idp, ref, id, path, entry).defaultAnswer(InvocationOnMock::callRealMethod));
    }

    @Test
    public void testGetProperties() {
        assertNotNull(identity.getProperties());
        assertTrue(identity.getProperties().isEmpty());
    }

    @Test
    public void testGetDeclaredGroups() throws Exception {
        Dn dn = when(mock(Dn.class).getName()).thenReturn("dn").getMock();
        when(entry.getDn()).thenReturn(dn);

        Map<String, ExternalIdentityRef> groupRefs = Map.of("gr", mock(ExternalIdentityRef.class));
        when(idp.getDeclaredGroupRefs(ref, "dn")).thenReturn(groupRefs);

        Collection<ExternalIdentityRef> expected = groupRefs.values();
        assertEquals(expected, identity.getDeclaredGroups());
        // result must be cached.... second invokation doesn't reach idp
        assertEquals(expected, identity.getDeclaredGroups());

        verify(idp).getDeclaredGroupRefs(ref, "dn");
    }

    @Test
    public void testToString() {
        String s = identity.toString();
        assertNotNull(s);
        assertEquals(s, mock(LdapIdentity.class, withSettings().useConstructor(null, ref, id, null, null).defaultAnswer(InvocationOnMock::callRealMethod)).toString());
        assertNotEquals(s, mock(LdapIdentity.class, withSettings().useConstructor(idp, ref, "otherId", path, entry).defaultAnswer(InvocationOnMock::callRealMethod)).toString());
    }
}