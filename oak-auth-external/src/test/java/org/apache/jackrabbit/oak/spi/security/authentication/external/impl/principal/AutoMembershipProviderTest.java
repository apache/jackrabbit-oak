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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.text.ParseException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class AutoMembershipProviderTest extends AbstractAutoMembershipTest {

    private AutoMembershipProvider provider;

    @Before
    public void before() throws Exception {
        super.before();
        provider = new AutoMembershipProvider(root, userManager, getNamePathMapper(), MAPPING, getAutoMembershipConfigMapping());
    }
    
    @After
    public void after() throws Exception {
        try {
            root.refresh();
            Authorizable a = userManager.getAuthorizable(USER_ID);
            if (a != null) {
                a.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }
    
    private void setExternalId(@NotNull String id, @NotNull String idpName) throws Exception {
        Root sr = getSystemRoot();
        sr.refresh();
        Authorizable a = getUserManager(sr).getAuthorizable(id);
        a.setProperty(REP_EXTERNAL_ID, getValueFactory(sr).createValue(new ExternalIdentityRef(USER_ID, idpName).getString()));
        sr.commit();
        root.refresh();
    }

    @Test
    public void testCoversAllMembers() throws RepositoryException {
        assertFalse(provider.coversAllMembers(automembershipGroup1));
        assertFalse(provider.coversAllMembers(userManager.createGroup(EveryonePrincipal.getInstance())));
        assertFalse(provider.coversAllMembers(mock(Group.class)));
    }
    
    @Test
    public void testGetMembersNoExternalUsers() throws Exception {
        // no user has rep:externalId set to the configured IPD-names
        assertFalse(provider.getMembers(automembershipGroup1, true).hasNext());
        assertFalse(provider.getMembers(automembershipGroup1, false).hasNext());
    }

    @Test
    public void testGetMembersExternalUser() throws Exception {
        setExternalId(getTestUser().getID(), IDP_VALID_AM);
        
        Iterator<Authorizable> it = provider.getMembers(automembershipGroup1, false);
        assertTrue(it.hasNext());
        assertEquals(getTestUser().getID(), it.next().getID());
        assertFalse(it.hasNext());

        it = provider.getMembers(automembershipGroup1, true);
        assertTrue(it.hasNext());
        assertEquals(getTestUser().getID(), it.next().getID());
        assertFalse(it.hasNext());
    }

    @Test
    public void testGetMembersExternalUserIdpMismatch() throws Exception {
        setExternalId(getTestUser().getID(), IDP_INVALID_AM);

        Iterator<Authorizable> it = provider.getMembers(automembershipGroup1, false);
        assertFalse(it.hasNext());

        it = provider.getMembers(automembershipGroup1, true);
        assertFalse(it.hasNext());
    }

    @Test
    public void testGetMembersExternalUserMultipleIdps() throws Exception {
        setExternalId(getTestUser().getID(), IDP_VALID_AM);
        User u = null;
        try {
            u = userManager.createUser("second", null);
            root.commit();
            setExternalId("second", IDP_MIXED_AM);

            Iterator<Authorizable> it = provider.getMembers(automembershipGroup1, false);
            assertEquals(2, Iterators.size(it));

            it = provider.getMembers(automembershipGroup1, true);
            assertEquals(2, Iterators.size(it));
        } finally {
            if (u != null) {
                u.remove();
                root.commit();
            }
        }
    }
    
    @Test
    public void testGetMembersExternalGroup() throws Exception {
        setExternalId(automembershipGroup1.getID(), IDP_VALID_AM);
        
        Iterator<Authorizable> it = provider.getMembers(automembershipGroup1, false);
        assertFalse(it.hasNext());

        it = provider.getMembers(automembershipGroup1, true);
        assertFalse(it.hasNext());
    }
    
    @Test
    public void testGetMembersCannotRetrievePrincipalFromGroup() throws Exception {
        Group gr = mock(Group.class);
        when(gr.getPrincipal()).thenThrow(new RepositoryException());
        
        assertFalse(provider.getMembers(gr, false).hasNext());
        assertFalse(provider.getMembers(gr, true).hasNext());
    }
    
    @Test
    public void testGetMembersGroupNotConfigured() throws Exception {
        Group gr = mock(Group.class);
        when(gr.getPrincipal()).thenReturn(EveryonePrincipal.getInstance());
        
        assertFalse(provider.getMembers(gr, false).hasNext());
        assertFalse(provider.getMembers(gr, true).hasNext());
    }

    @Test
    public void testGetMembersLookupByPathFails() throws Exception {
        setExternalId(getTestUser().getID(), IDP_VALID_AM);

        UserManager um = spy(userManager);
        doThrow(new RepositoryException()).when(um).getAuthorizableByPath(anyString());
        
        AutoMembershipProvider amp = new AutoMembershipProvider(root, um, getNamePathMapper(), MAPPING, getAutoMembershipConfigMapping());
        assertFalse(amp.getMembers(automembershipGroup1, false).hasNext());
    }

    @Test(expected = RepositoryException.class)
    public void testGetMembersQueryFails() throws Exception {
        QueryEngine qe = mock(QueryEngine.class);
        when(qe.executeQuery(anyString(), anyString(), any(Map.class), any(Map.class))).thenThrow(new ParseException("query failed", 0));
        Root r = when(mock(Root.class).getQueryEngine()).thenReturn(qe).getMock();
        
        AutoMembershipProvider amp = new AutoMembershipProvider(r, userManager, getNamePathMapper(), MAPPING, getAutoMembershipConfigMapping());
        assertFalse(amp.getMembers(automembershipGroup1, false).hasNext());
    }
    
    @Test
    public void testIsMemberLocalUser() throws Exception {
        assertFalse(provider.isMember(automembershipGroup1, getTestUser(), true));
        assertFalse(provider.isMember(automembershipGroup1, getTestUser(), false));
    }

    @Test
    public void testIsMemberSelf() throws Exception {
        assertFalse(provider.isMember(automembershipGroup1, automembershipGroup1, true));
        assertFalse(provider.isMember(automembershipGroup1, automembershipGroup1, false));
    }

    @Test
    public void testIsMemberExternalUser() throws Exception {
        setExternalId(getTestUser().getID(), IDP_VALID_AM);

        assertTrue(provider.isMember(automembershipGroup1, getTestUser(), false));
        assertTrue(provider.isMember(automembershipGroup1, getTestUser(), true));
    }
    
    @Test
    public void testIsMemberExternalUserIdpMismatch() throws Exception {
        setExternalId(getTestUser().getID(), IDP_INVALID_AM);

        assertFalse(provider.isMember(automembershipGroup1, getTestUser(), false));
    }

    @Test
    public void testIsMemberExternalGroup() throws Exception {
        setExternalId(automembershipGroup1.getID(), IDP_VALID_AM);
        
        assertFalse(provider.isMember(automembershipGroup1, automembershipGroup1, false));
        assertFalse(provider.isMember(automembershipGroup1, automembershipGroup1, true));
    }
    
    @Test
    public void testGetMembershipLocalUser() throws Exception {
        assertFalse(provider.getMembership(getTestUser(), true).hasNext());
        assertFalse(provider.getMembership(getTestUser(), false).hasNext());
    }

    @Test
    public void testGetMembershipSelf() throws Exception {
        assertFalse(provider.getMembership(automembershipGroup1, true).hasNext());
        assertFalse(provider.getMembership(automembershipGroup1, false).hasNext());
    }
    
    @Test
    public void testGetMembershipExternalUser() throws Exception {
        setExternalId(getTestUser().getID(), IDP_VALID_AM);

        Set<Group> groups = ImmutableSet.copyOf(provider.getMembership(getTestUser(), false));
        assertEquals(2, groups.size());
        assertTrue(groups.contains(automembershipGroup1));
        assertTrue(groups.contains(automembershipGroup2));
    }

    @Test
    public void testGetMembershipExternalUserInherited() throws Exception {
        setExternalId(getTestUser().getID(), IDP_VALID_AM);

        Set<Group> groups = ImmutableSet.copyOf(provider.getMembership(getTestUser(), true));
        assertEquals(2, groups.size());
        assertTrue(groups.contains(automembershipGroup1));
    }

    @Test
    public void testGetMembershipExternalUserNestedGroups() throws Exception {
        setExternalId(getTestUser().getID(), IDP_VALID_AM);
        
        Group baseGroup = userManager.createGroup("baseGroup");
        try {
            baseGroup.addMember(automembershipGroup1);
            root.commit();

            Set<Group> groups = ImmutableSet.copyOf(provider.getMembership(getTestUser(), false));
            assertEquals(2, groups.size());
            assertTrue(groups.contains(automembershipGroup1));
            assertTrue(groups.contains(automembershipGroup2));

            groups = ImmutableSet.copyOf(provider.getMembership(getTestUser(), true));
            assertEquals(3, groups.size());
            assertTrue(groups.contains(automembershipGroup1));
            assertTrue(groups.contains(automembershipGroup2));
            assertTrue(groups.contains(baseGroup));
        } finally {
            baseGroup.remove();
            root.commit();
        }
    }

    @Test
    public void testGetMembershipExternalUserEveryoneGroupExists() throws Exception {
        setExternalId(getTestUser().getID(), IDP_VALID_AM);

        // create dynamic group everyone. both automembershipGroups are members thereof without explicit add-member
        Group everyone = userManager.createGroup(EveryonePrincipal.getInstance());
        // in addition establish a 2 level inheritance for automembershipGroup1 
        automembershipGroup2.addMember(automembershipGroup1);
        root.commit();

        Set<Group> groups = ImmutableSet.copyOf(provider.getMembership(getTestUser(), false));
        assertEquals(2, groups.size());
        assertTrue(groups.contains(automembershipGroup1));
        assertTrue(groups.contains(automembershipGroup2));

        groups = ImmutableSet.copyOf(provider.getMembership(getTestUser(), true));
        assertEquals(3, groups.size()); // all duplicates must be properly filtered
        assertTrue(groups.contains(automembershipGroup1));
        assertTrue(groups.contains(automembershipGroup2));
        assertTrue(groups.contains(everyone));
    }

    @Test
    public void testGetMembershipExternalUserIdpMismatch() throws Exception {
        setExternalId(getTestUser().getID(), IDP_INVALID_AM);

        assertFalse(provider.getMembership(getTestUser(), false).hasNext());
        assertFalse(provider.getMembership(getTestUser(), true).hasNext());
    }

    @Test
    public void testGetMembershipExternalGroup() throws Exception {
        setExternalId(automembershipGroup1.getID(), IDP_VALID_AM);

        assertFalse(provider.getMembership(automembershipGroup1, false).hasNext());
        assertFalse(provider.getMembership(automembershipGroup1, true).hasNext());
    }
    
    @Test
    public void testGetMembershipAutogroupIsUser() throws Exception {
        UserManager um = spy(userManager);
        
        User user = mock(User.class);
        when(user.isGroup()).thenReturn(false);
        when(um.getAuthorizable(automembershipGroup1.getPrincipal())).thenReturn(user);
        when(um.getAuthorizable(automembershipGroup2.getPrincipal())).thenReturn(user);

        setExternalId(getTestUser().getID(), IDP_VALID_AM);

        AutoMembershipProvider amp = new AutoMembershipProvider(root, um, getNamePathMapper(), MAPPING, getAutoMembershipConfigMapping());
        assertFalse(amp.getMembership(getTestUser(), false).hasNext());
    }

    @Test
    public void testGetMembershipAutogroupGroupLookupFails() throws Exception {
        UserManager um = spy(userManager);

        User user = mock(User.class);
        when(user.isGroup()).thenReturn(false);
        when(um.getAuthorizable(any(Principal.class))).thenThrow(new RepositoryException());

        setExternalId(getTestUser().getID(), IDP_VALID_AM);
        
        AutoMembershipProvider amp = new AutoMembershipProvider(root, um, getNamePathMapper(), MAPPING, getAutoMembershipConfigMapping());
        assertFalse(amp.getMembership(getTestUser(), false).hasNext());
    }

    @Test
    public void testGetMembershipAutogroupGroupMemberOfFails() throws Exception {
        // establish nested groups
        automembershipGroup2.addMember(automembershipGroup1);
        root.commit();
        
        Group spiedGroup = spy(automembershipGroup1);
        when(spiedGroup.memberOf()).thenThrow(new RepositoryException());
        
        UserManager um = spy(userManager);
        when(um.getAuthorizable(automembershipGroup1.getPrincipal())).thenReturn(spiedGroup);

        setExternalId(getTestUser().getID(), IDP_VALID_AM);

        AutoMembershipProvider amp = new AutoMembershipProvider(root, um, getNamePathMapper(), MAPPING, getAutoMembershipConfigMapping());
        Set<Group> membership = ImmutableSet.copyOf(amp.getMembership(getTestUser(), true));
        assertEquals(2, membership.size());
    }
    
    @Test
    public void testGetMembershipAutogroupRemoved() throws Exception {
        setExternalId(getTestUser().getID(), IDP_VALID_AM);
        
        automembershipGroup1.remove();
        assertEquals(1, Iterators.size(provider.getMembership(getTestUser(), false)));
        assertEquals(1, Iterators.size(provider.getMembership(getTestUser(), true)));
        
        automembershipGroup2.remove();
        assertFalse(provider.getMembership(getTestUser(), false).hasNext());
        assertFalse(provider.getMembership(getTestUser(), true).hasNext());
    }
}