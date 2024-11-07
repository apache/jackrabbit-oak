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

import org.apache.jackrabbit.guava.common.collect.ImmutableMap;
import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.spi.security.authentication.external.ExternalIdentityRef;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jcr.RepositoryException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.ExternalIdentityConstants.REP_EXTERNAL_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class AutoMembershipProviderTest extends AbstractAutoMembershipTest {
    
    @Parameterized.Parameters(name = "name={1}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] { false, "Dynamic-Groups = false" },
                new Object[] { true, "Dynamic-Groups = true" });
    }
    
    private final boolean dynamicGroupsEnabled;
    
    private AutoMembershipProvider provider;

    public AutoMembershipProviderTest(boolean dynamicGroupsEnabled, @NotNull String name) {
        this.dynamicGroupsEnabled = dynamicGroupsEnabled;
    }

    @Before
    public void before() throws Exception {
        super.before();
        provider = createAutoMembershipProvider(root, userManager);
    }

    private void setExternalId(@NotNull String id, @NotNull String idpName) throws Exception {
        Root sr = getSystemRoot();
        sr.refresh();
        Authorizable a = getUserManager(sr).getAuthorizable(id);
        a.setProperty(REP_EXTERNAL_ID, getValueFactory(sr).createValue(new ExternalIdentityRef(id, idpName).getString()));
        sr.commit();
        root.refresh();
    }

    @Override
    protected @NotNull DefaultSyncConfig createSyncConfig() {
        DefaultSyncConfig dsc = super.createSyncConfig();
        dsc.user().setDynamicMembership(true).setAutoMembership(MAPPING.get(IDP_VALID_AM));
        if (dynamicGroupsEnabled) {
            dsc.group().setDynamicGroups(true).setAutoMembership(MAPPING_GROUP.get(IDP_VALID_AM));
        }
        return dsc;
    }

    @NotNull
    private AutoMembershipProvider createAutoMembershipProvider(@NotNull Root root, @NotNull UserManager userManager) {
        Map<String, String[]> groupMapping = (dynamicGroupsEnabled) ? MAPPING_GROUP : null;
        return new AutoMembershipProvider(root, userManager, getNamePathMapper(), MAPPING, groupMapping, getAutoMembershipConfigMapping());
    }
    
    private static void assertMatchingEntries(@NotNull Iterator<Authorizable> it, @NotNull String... expectedIds) {
        Set<String> ids = ImmutableSet.copyOf(Iterators.transform(it, authorizable -> {
            try {
                return authorizable.getID();
            } catch (RepositoryException repositoryException) {
                return "";
            }
        }));
        assertEquals(ImmutableSet.copyOf(expectedIds), ids);
    }

    @Test
    public void testCoversAllMembers() throws RepositoryException {
        assertFalse(provider.coversAllMembers(automembershipGroup1));
        assertFalse(provider.coversAllMembers(userManager.createGroup(EveryonePrincipal.getInstance())));
        Group gr = mock(Group.class);
        assertFalse(provider.coversAllMembers(gr));
        verifyNoInteractions(gr);
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
    public void testGetMembersExternalGroupExist() throws Exception {
        Group testGroup = getTestGroup();
        setExternalId(testGroup.getID(), IDP_VALID_AM);

        if (dynamicGroupsEnabled) {
            // external group does have 'automembershipGroup3' as configured autom-membership
            assertMatchingEntries(provider.getMembers(automembershipGroup3, false), testGroup.getID());

            // external group doesn't have 'automembershipGroup1' as configured autom-membership
            assertFalse(provider.getMembers(automembershipGroup1, false).hasNext());
        } else {
            assertFalse(provider.getMembers(automembershipGroup3, false).hasNext());
        }
    }

    @Test
    public void testGetMembersExternalGroupExistInherit() throws Exception {
        Group testGroup = getTestGroup();
        setExternalId(testGroup.getID(), IDP_VALID_AM);

        if (dynamicGroupsEnabled) {
            // external group does have 'automembershipGroup3' as configured autom-membership
            assertMatchingEntries(provider.getMembers(automembershipGroup3, true), testGroup.getID());
            
            // external group doesn't have 'automembershipGroup1' as configured autom-membership
            assertFalse(provider.getMembers(automembershipGroup1, true).hasNext());
            
        } else {
            assertFalse(provider.getMembers(automembershipGroup3, true).hasNext());
        }
    }

    @Test
    public void testGetMembersMatchingUsersAndGroups() throws Exception {
        Group testGroup = getTestGroup();
        setExternalId(testGroup.getID(), IDP_VALID_AM);
        String testUserId = getTestUser().getID();
        setExternalId(testUserId, IDP_VALID_AM);
        
        // create provider with a group mapping that contains same group as user-mapping
        if (dynamicGroupsEnabled) {
            Map<String, String[]> grMapping = ImmutableMap.of(IDP_VALID_AM, new String[] {AUTOMEMBERSHIP_GROUP_ID_1});
            AutoMembershipProvider amp = new AutoMembershipProvider(root, userManager, getNamePathMapper(), MAPPING, grMapping, getAutoMembershipConfigMapping());
            // external group does have 'automembershipGroup3' as configured autom-membership
            Iterator<Authorizable> it = amp.getMembers(automembershipGroup1, false);
            assertMatchingEntries(it, testGroup.getID(), testUserId);
        } else {
            AutoMembershipProvider amp = new AutoMembershipProvider(root, userManager, getNamePathMapper(), MAPPING, null, getAutoMembershipConfigMapping());
            Iterator<Authorizable> it = amp.getMembers(automembershipGroup1, false);
            assertMatchingEntries(it, testUserId);
        }
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
        
        AutoMembershipProvider amp = createAutoMembershipProvider(root, um);
        assertFalse(amp.getMembers(automembershipGroup1, false).hasNext());
    }

    @Test(expected = RepositoryException.class)
    public void testGetMembersQueryFails() throws Exception {
        QueryEngine qe = mock(QueryEngine.class);
        when(qe.executeQuery(anyString(), anyString(), any(Map.class), any(Map.class))).thenThrow(new ParseException("query failed", 0));
        Root r = when(mock(Root.class).getQueryEngine()).thenReturn(qe).getMock();
        
        AutoMembershipProvider amp = createAutoMembershipProvider(r, userManager);
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
    public void testIsMemberExternalUserInherited() throws Exception {
        User testUser = getTestUser();
        setExternalId(testUser.getID(), IDP_VALID_AM);
        
        Group testGroup = getTestGroup(automembershipGroup1);

        assertFalse(provider.isMember(testGroup, testUser, false));
        assertTrue(provider.isMember(testGroup, testUser, true));
    }

    @Test
    public void testIsMemberExternalUserInheritedNested() throws Exception {
        User testUser = getTestUser();
        setExternalId(testUser.getID(), IDP_VALID_AM);

        Group testGroup = getTestGroup();
        Group base = userManager.createGroup("baseGroup");
        base.addMember(testGroup);
        root.commit();

        assertFalse(provider.isMember(base, testUser, false));
        assertFalse(provider.isMember(base, testUser, true));
        
        // add 'automembership-group' as nested members
        testGroup.addMember(automembershipGroup1);
        root.commit();

        assertFalse(provider.isMember(base, testUser, false));
        assertTrue(provider.isMember(base, testUser, true));
    }
    
    @Test
    public void testIsMemberExternalUserIdpMismatch() throws Exception {
        setExternalId(getTestUser().getID(), IDP_INVALID_AM);

        assertFalse(provider.isMember(automembershipGroup1, getTestUser(), false));
    }

    @Test
    public void testIsMemberExternalGroupSelf() throws Exception { 
        Group testGroup = getTestGroup();
        setExternalId(testGroup.getID(), IDP_VALID_AM);
        
        assertFalse(provider.isMember(testGroup, testGroup, false));
        assertFalse(provider.isMember(testGroup, testGroup, true));
    }

    @Test
    public void testIsMemberExternalGroup() throws Exception {
        Group testGroup = getTestGroup();
        setExternalId(testGroup.getID(), IDP_VALID_AM);

        if (dynamicGroupsEnabled) {
            assertTrue(provider.isMember(automembershipGroup3, testGroup, false));
            assertTrue(provider.isMember(automembershipGroup3, testGroup, true));
            
            // automembershipGroup1 not configured for groups
            assertFalse(provider.isMember(automembershipGroup1, testGroup, false));
            assertFalse(provider.isMember(automembershipGroup1, testGroup, true));
        } else {
            for (Group gr : new Group[] {automembershipGroup1, automembershipGroup3}) {
                assertFalse(provider.isMember(gr, testGroup, false));
                assertFalse(provider.isMember(gr, testGroup, true));
            }
        }
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

        Group baseGroup = getTestGroup(automembershipGroup1);

        Set<Group> groups = ImmutableSet.copyOf(provider.getMembership(getTestUser(), false));
        assertEquals(2, groups.size());
        assertTrue(groups.contains(automembershipGroup1));
        assertTrue(groups.contains(automembershipGroup2));

        groups = ImmutableSet.copyOf(provider.getMembership(getTestUser(), true));
        assertEquals(3, groups.size());
        assertTrue(groups.contains(automembershipGroup1));
        assertTrue(groups.contains(automembershipGroup2));
        assertTrue(groups.contains(baseGroup));
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
        assertEquals(2, groups.size()); // all duplicates must be properly filtered and everyone must be omitted
        assertTrue(groups.contains(automembershipGroup1));
        assertTrue(groups.contains(automembershipGroup2));
        assertFalse(groups.contains(everyone));
    }

    @Test
    public void testGetMembershipExternalUserIdpMismatch() throws Exception {
        setExternalId(getTestUser().getID(), IDP_INVALID_AM);

        assertFalse(provider.getMembership(getTestUser(), false).hasNext());
        assertFalse(provider.getMembership(getTestUser(), true).hasNext());
    }

    @Test
    public void testGetMembershipExternalGroup() throws Exception {
        Group testGroup = getTestGroup();
        setExternalId(testGroup.getID(), IDP_VALID_AM);

        if (dynamicGroupsEnabled) {
            Iterator<Group> it = provider.getMembership(testGroup, false);
            assertTrue(it.hasNext());
            assertEquals(automembershipGroup3.getID(), it.next().getID());
            assertFalse(it.hasNext());

            it = provider.getMembership(testGroup, false);
            assertTrue(provider.getMembership(testGroup, true).hasNext());
            assertEquals(automembershipGroup3.getID(), it.next().getID());
            assertFalse(it.hasNext());
        } else {
            assertFalse(provider.getMembership(testGroup, false).hasNext());
            assertFalse(provider.getMembership(testGroup, true).hasNext());
        }
    }
    
    @Test
    public void testGetMembershipAutogroupIsUser() throws Exception {
        UserManager um = spy(userManager);
        
        User user = mock(User.class);
        when(user.isGroup()).thenReturn(false);
        doReturn(user).when(um).getAuthorizable(automembershipGroup1.getID());
        doReturn(user).when(um).getAuthorizable(automembershipGroup2.getID());

        setExternalId(getTestUser().getID(), IDP_VALID_AM);

        AutoMembershipProvider amp = createAutoMembershipProvider(root, um);
        assertFalse(amp.getMembership(getTestUser(), false).hasNext());
        
        verify(um, times(2)).getAuthorizable(anyString());
        verifyNoMoreInteractions(um);
    }

    @Test
    public void testGetMembershipAutogroupGroupLookupFails() throws Exception {
        UserManager um = spy(userManager);

        User user = mock(User.class);
        when(user.isGroup()).thenReturn(false);
        doThrow(new RepositoryException()).when(um).getAuthorizable(any(String.class));

        setExternalId(getTestUser().getID(), IDP_VALID_AM);
        
        AutoMembershipProvider amp = createAutoMembershipProvider(root, um);
        assertFalse(amp.getMembership(getTestUser(), false).hasNext());
        
        verify(um, times(2)).getAuthorizable(anyString());
        verifyNoMoreInteractions(um);
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

        AutoMembershipProvider amp = createAutoMembershipProvider(root, um);
        Set<Group> membership = ImmutableSet.copyOf(amp.getMembership(getTestUser(), true));
        assertEquals(2, membership.size());
    }
    
    @Test
    public void testGetMembershipAutogroupRemoved() throws Exception {
        setExternalId(getTestUser().getID(), IDP_VALID_AM);
        
        automembershipGroup1.remove();
        assertEquals(1, Iterators.size(provider.getMembership(getTestUser(), false)));
        assertEquals(1, Iterators.size(provider.getMembership(getTestUser(), true)));
        
        // remove second group : but read principal from cache
        automembershipGroup2.remove();
        assertFalse(provider.getMembership(getTestUser(), false).hasNext());
        assertFalse(provider.getMembership(getTestUser(), true).hasNext());
    }
}