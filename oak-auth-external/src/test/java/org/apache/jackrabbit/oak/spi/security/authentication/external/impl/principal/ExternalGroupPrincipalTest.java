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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentity;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalUser;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.text.ParseException;
import java.util.Enumeration;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.TestIdentityProvider.ID_SECOND_USER;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_PRINCIPAL_NAMES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ExternalGroupPrincipalTest extends AbstractPrincipalTest {

    @Test
    public void testIsMember() throws Exception {
        ExternalUser externalUser = idp.getUser(USER_ID);
        GroupPrincipal principal = getGroupPrincipal(externalUser.getDeclaredGroups().iterator().next());

        assertTrue(principal.isMember(new PrincipalImpl(externalUser.getPrincipalName())));
        assertTrue(principal.isMember(getUserManager(root).getAuthorizable(USER_ID).getPrincipal()));
    }

    @Test
    public void testNotIsMember() throws Exception {
        GroupPrincipal principal = getGroupPrincipal(idp.getUser(USER_ID).getDeclaredGroups().iterator().next());

        Authorizable notMember = getUserManager(root).getAuthorizable(ID_SECOND_USER);
        assertFalse(principal.isMember(notMember.getPrincipal()));

        root.getTree(notMember.getPath()).setProperty(REP_EXTERNAL_PRINCIPAL_NAMES, ImmutableList.of("secondGroup"), Type.STRINGS);
        assertFalse(principal.isMember(notMember.getPrincipal()));

        root.getTree(notMember.getPath()).setProperty(REP_EXTERNAL_PRINCIPAL_NAMES, ImmutableList.of(), Type.STRINGS);
        assertFalse(principal.isMember(new PrincipalImpl(notMember.getPrincipal().getName())));
    }

    @Test
    public void testIsMemberExternalGroup() throws Exception {
        GroupPrincipal principal = getGroupPrincipal();

        Iterable<String> exGroupPrincNames = Iterables.transform(ImmutableList.copyOf(idp.listGroups()), ExternalIdentity::getPrincipalName);
        for (String principalName : exGroupPrincNames) {
            assertFalse(principal.isMember(new PrincipalImpl(principalName)));
        }
    }

    @Test
    public void testIsMemberLocalUser() throws Exception {
        GroupPrincipal principal = getGroupPrincipal();

        assertFalse(principal.isMember(getTestUser().getPrincipal()));
        assertFalse(principal.isMember(new PrincipalImpl(getTestUser().getPrincipal().getName())));
    }

    @Test
    public void testIsMemberLocalGroup() throws Exception {
        Group gr = createTestGroup();
        GroupPrincipal principal = getGroupPrincipal();

        String name = gr.getPrincipal().getName();
        assertFalse(principal.isMember(gr.getPrincipal()));
        assertFalse(principal.isMember(new PrincipalImpl(name)));
        ItemBasedPrincipal ibp = new ItemBasedPrincipal() {
            @Override
            public @NotNull String getPath() throws RepositoryException {
                return gr.getPath();
            }
            @Override
            public String getName() {
                return name;
            }
        };
        assertFalse(principal.isMember(ibp));
    }

    @Test
    public void testIsMemberUserLookupFails() throws Exception {
        UserManager um = spy(getUserManager(root));
        Authorizable a = um.getAuthorizable(USER_ID);
        when(um.getAuthorizable(any(Principal.class))).thenThrow(new RepositoryException());

        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(root, getNamePathMapper())).thenReturn(um).getMock();
        ExternalGroupPrincipalProvider pp = createPrincipalProvider(root, uc);

        ExternalIdentityRef ref = idp.getUser(USER_ID).getDeclaredGroups().iterator().next();
        String groupName = idp.getIdentity(ref).getPrincipalName();

        Principal gp = pp.getPrincipal(groupName);
        assertTrue(gp instanceof GroupPrincipal);
        assertFalse(((GroupPrincipal)gp).isMember(new PrincipalImpl(a.getPrincipal().getName())));
    }
    
    @Test
    public void testMembers() throws Exception {
        GroupPrincipal principal = getGroupPrincipal();
        Principal[] expectedMembers = new Principal[]{
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

    @Test
    public void testMembersUserLookupReturnsNull() throws Exception {
        UserManager um = spy(getUserManager(root));
        String userPath = um.getAuthorizable(USER_ID).getPath();
        when(um.getAuthorizableByPath(userPath)).thenReturn(null);

        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(root, getNamePathMapper())).thenReturn(um).getMock();
        ExternalGroupPrincipalProvider pp = createPrincipalProvider(root, uc);

        ExternalIdentityRef ref = idp.getUser(USER_ID).getDeclaredGroups().iterator().next();
        String groupName = idp.getIdentity(ref).getPrincipalName();

        Principal gp = pp.getPrincipal(groupName);
        assertTrue(gp instanceof GroupPrincipal);
        assertFalse(((GroupPrincipal)gp).members().hasMoreElements());
    }

    @Test
    public void testMembersUserLookupFails() throws Exception {
        UserManager um = spy(getUserManager(root));
        String userPath = um.getAuthorizable(USER_ID).getPath();
        when(um.getAuthorizableByPath(userPath)).thenThrow(new RepositoryException());

        UserConfiguration uc = when(mock(UserConfiguration.class).getUserManager(root, getNamePathMapper())).thenReturn(um).getMock();
        ExternalGroupPrincipalProvider pp = createPrincipalProvider(root, uc);

        ExternalIdentityRef ref = idp.getUser(USER_ID).getDeclaredGroups().iterator().next();
        String groupName = idp.getIdentity(ref).getPrincipalName();

        Principal gp = pp.getPrincipal(groupName);
        assertTrue(gp instanceof GroupPrincipal);
        assertFalse(((GroupPrincipal)gp).members().hasMoreElements());
    }
    
    @Test
    public void testMembersQueryFails() throws Exception {
        QueryEngine qe = mock(QueryEngine.class);
        when(qe.executeQuery(anyString(), anyString(), any(Map.class), any(Map.class))).thenThrow(new ParseException("fail", 0));

        Root r = spy(root);
        when(r.getQueryEngine()).thenReturn(qe);
        ExternalGroupPrincipalProvider pp = createPrincipalProvider(r, getUserConfiguration());

        Principal gp = pp.getMembershipPrincipals(getUserManager(root).getAuthorizable(USER_ID).getPrincipal()).iterator().next();
        assertTrue(gp instanceof GroupPrincipal);
        assertFalse(((GroupPrincipal)gp).members().hasMoreElements());
    }
}
