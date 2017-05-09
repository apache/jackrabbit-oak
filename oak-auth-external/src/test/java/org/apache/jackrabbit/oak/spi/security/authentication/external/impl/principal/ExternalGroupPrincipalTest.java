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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import java.security.Principal;
import java.util.Enumeration;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalGroup;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExternalGroupPrincipalTest extends AbstractPrincipalTest {

    @Test
    public void testIsMember() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        java.security.acl.Group principal = getGroupPrincipal(externalUser.getDeclaredGroups().iterator().next());

        assertTrue(principal.isMember(new PrincipalImpl(externalUser.getPrincipalName())));
        assertTrue(principal.isMember(getUserManager(root).getAuthorizable(USER_ID).getPrincipal()));
    }

    @Test
    public void testIsMemberExternalGroup() throws Exception {
        java.security.acl.Group principal = getGroupPrincipal();

        Iterable<String> exGroupPrincNames = Iterables.transform(ImmutableList.copyOf(idp.listGroups()), new Function<ExternalGroup, String>() {
            @Nullable
            @Override
            public String apply(ExternalGroup input) {
                return input.getPrincipalName();
            }
        });

        for (String principalName : exGroupPrincNames) {
            assertFalse(principal.isMember(new PrincipalImpl(principalName)));
        }
    }

    @Test
    public void testIsMemberLocalUser() throws Exception {
        java.security.acl.Group principal = getGroupPrincipal();

        assertFalse(principal.isMember(getTestUser().getPrincipal()));
        assertFalse(principal.isMember(new PrincipalImpl(getTestUser().getPrincipal().getName())));
    }

    @Test
    public void testIsMemberLocalGroup() throws Exception {
        Group gr = createTestGroup();
        java.security.acl.Group principal = getGroupPrincipal();

        assertFalse(principal.isMember(gr.getPrincipal()));
        assertFalse(principal.isMember(new PrincipalImpl(gr.getPrincipal().getName())));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddMember() throws Exception {
        java.security.acl.Group principal = getGroupPrincipal();
        principal.addMember(getTestUser().getPrincipal());
    }

    @Test
    public void testAddMemberExistingMember() throws Exception {
        java.security.acl.Group principal = getGroupPrincipal();
        assertFalse(principal.addMember(getUserManager(root).getAuthorizable(USER_ID).getPrincipal()));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveMember() throws Exception {
        java.security.acl.Group principal = getGroupPrincipal();
        principal.removeMember(getUserManager(root).getAuthorizable(USER_ID).getPrincipal());
    }

    @Test
    public void testRemoveMemberNotMember() throws Exception {
        java.security.acl.Group principal = getGroupPrincipal();
        assertFalse(principal.removeMember(getTestUser().getPrincipal()));
    }

    @Test
    public void testMembers() throws Exception {
        java.security.acl.Group principal = getGroupPrincipal();

        Principal[] expectedMembers = new Principal[] {
                getUserManager(root).getAuthorizable(USER_ID).getPrincipal(),
                new PrincipalImpl(idp.getUser(USER_ID).getPrincipalName())
        };

        for (Principal expected : expectedMembers) {
            Enumeration<? extends Principal> members = principal.members();
            assertTrue(members.hasMoreElements());
            assertEquals(expected, members.nextElement());
            assertFalse(members.hasMoreElements());
        }
    }
}