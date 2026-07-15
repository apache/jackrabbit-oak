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

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LdapGroupTest extends LdapIdentityTest {

    @Override
    LdapGroup mockIdentity(@NotNull LdapIdentityProvider idp, @NotNull ExternalIdentityRef ref, @NotNull String id, @NotNull String path, @NotNull Entry entry) {
        return new LdapGroup(idp, ref, id, path, entry);
    }

    @Test
    public void testGetDeclaredMembers() throws Exception {
        Dn dn = when(mock(Dn.class).getName()).thenReturn("dn").getMock();
        when(entry.getDn()).thenReturn(dn);

        Map<String, ExternalIdentityRef> memberRefs = Map.of("m", mock(ExternalIdentityRef.class));
        when(idp.getDeclaredMemberRefs(ref, "dn")).thenReturn(memberRefs);

        Collection<ExternalIdentityRef> expected = memberRefs.values();
        assertEquals(expected, ((LdapGroup) identity).getDeclaredMembers());
        // result must be cached.... second invokation doesn't reach idp
        assertEquals(expected, ((LdapGroup) identity).getDeclaredMembers());

        verify(idp).getDeclaredMemberRefs(ref, "dn");
    }
}