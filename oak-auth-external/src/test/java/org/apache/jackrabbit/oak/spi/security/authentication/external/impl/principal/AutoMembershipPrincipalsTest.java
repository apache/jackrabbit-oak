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

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipConfig;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AutoMembershipPrincipalsTest extends AbstractAutoMembershipTest {
    
    private AutoMembershipPrincipals amp;
    private final Authorizable authorizable = mock(Authorizable.class);
    
    private final AutoMembershipConfig amConfig = mock(AutoMembershipConfig.class);
    
    @Before
    public void before() throws Exception {
        super.before();
        amp = new AutoMembershipPrincipals(userManager, MAPPING, getAutoMembershipConfigMapping());
        
        when(amConfig.getAutoMembership(authorizable)).thenReturn(Set.of(automembershipGroup3.getID()));
        when(amConfig.getAutoMembers(any(UserManager.class), any(Group.class))).thenReturn(Collections.emptyIterator());
        when(amConfig.getAutoMembers(userManager, automembershipGroup3)).thenReturn(Iterators.singletonIterator(authorizable));
    }

    @Override
    Map<String, AutoMembershipConfig> getAutoMembershipConfigMapping() {
        return Collections.singletonMap(IDP_VALID_AM, amConfig);
    }

    @NotNull
    private Set<Principal> getAutoMembership(@NotNull String idpName, @NotNull Authorizable authorizable, boolean includeInherited) {
        return amp.getAutoMembership(idpName, authorizable, includeInherited).keySet();
    }

    @Test
    public void testGetPrincipalsUnknownIdp() {
        assertTrue(amp.getAutoMembership("unknown",authorizable, false).isEmpty());
        verifyNoInteractions(authorizable, amConfig);
    }

    @Test
    public void testGetPrincipalsUnknownGroup() {
        Collection<Principal> principals = getAutoMembership(IDP_INVALID_AM, authorizable, false);
        assertTrue(principals.isEmpty());
        verifyNoInteractions(authorizable, amConfig);
    }

    @Test
    public void testGetPrincipalsMultipleGroups() throws Exception {
        when(amConfig.getAutoMembership(authorizable)).thenReturn(Collections.emptySet());

        Collection<Principal> principals = getAutoMembership(IDP_VALID_AM, authorizable, false);
        assertFalse(principals.isEmpty());
        Set<Principal> expected = CollectionUtils.toSet(automembershipGroup1.getPrincipal(), automembershipGroup2.getPrincipal());
        assertEquals(expected, principals);

        // change behavior of automembership-config
        when(amConfig.getAutoMembership(authorizable)).thenReturn(Set.of(automembershipGroup3.getID()));
        
        principals = getAutoMembership(IDP_VALID_AM, authorizable, false);
        assertFalse(principals.isEmpty());
        expected.add(automembershipGroup3.getPrincipal());
        assertEquals(expected, principals);
        
        verifyNoInteractions(authorizable);
        verify(amConfig, times(2)).getAutoMembership(authorizable);
    }
    
    @Test
    public void testGetPrincipalsMixed() throws Exception {
        Collection<Principal> principals = getAutoMembership(IDP_MIXED_AM, authorizable, false);
        assertFalse(principals.isEmpty());
        assertEquals(Set.of(automembershipGroup1.getPrincipal()), ImmutableSet.copyOf(principals));
        verifyNoInteractions(authorizable, amConfig);
    }

    @Test
    public void testGetPrincipalsInherited() throws Exception {
        when(amConfig.getAutoMembership(authorizable)).thenReturn(Collections.emptySet());
        
        Group testGroup = getTestGroup(automembershipGroup1, automembershipGroup2);

        Collection<Principal> principals = getAutoMembership(IDP_VALID_AM, authorizable, true);
        Set<Principal> expected = Set.of(testGroup.getPrincipal(), automembershipGroup1.getPrincipal(), automembershipGroup2.getPrincipal());
        assertEquals(expected, principals);

        verifyNoInteractions(authorizable);
        verify(amConfig).getAutoMembership(authorizable);
    }    
    
    @Test
    public void testGroupLookupFails() throws Exception {
        UserManager um = spy(userManager);
        when(um.getAuthorizable(anyString())).thenThrow(new RepositoryException());
        
        AutoMembershipPrincipals amprincipals = new AutoMembershipPrincipals(um, MAPPING, getAutoMembershipConfigMapping());
        assertTrue(amprincipals.getAutoMembership(IDP_VALID_AM, authorizable, false).isEmpty());
        verifyNoInteractions(authorizable);
        verify(amConfig).getAutoMembership(authorizable);
        verifyNoMoreInteractions(amConfig);
    }

    @Test
    public void testGroupLookupByPrincipalFails() throws Exception {
        UserManager um = spy(userManager);

        AutoMembershipPrincipals amprincipals = new AutoMembershipPrincipals(um, MAPPING, Collections.emptyMap());
        assertEquals(2, amprincipals.getAutoMembership(IDP_VALID_AM, authorizable, false).size());

        when(um.getAuthorizable(automembershipGroup1.getPrincipal())).thenThrow(new RepositoryException());
        assertEquals(1, amprincipals.getAutoMembership(IDP_VALID_AM, authorizable, false).size());
    }

    @Test
    public void testGroupLookupByPrincipalReturnsUser() throws Exception {
        UserManager um = spy(userManager);

        AutoMembershipPrincipals amprincipals = new AutoMembershipPrincipals(um, MAPPING, Collections.emptyMap());
        assertEquals(2, amprincipals.getAutoMembership(IDP_VALID_AM, authorizable, false).size());

        Authorizable a = mock(Authorizable.class);
        when(a.isGroup()).thenReturn(false);
        when(um.getAuthorizable(automembershipGroup1.getPrincipal())).thenReturn(a);
        assertEquals(1, amprincipals.getAutoMembership(IDP_VALID_AM, authorizable, false).size());
        
        verify(a).isGroup();
    }
    
    @Test
    public void testInheritedGroupLookupFails() throws Exception {
        UserManager um = spy(userManager);

        Group testGroup = getTestGroup(automembershipGroup1);

        AutoMembershipPrincipals amprincipals = new AutoMembershipPrincipals(um, MAPPING, Collections.emptyMap());
        Map<Principal, Group> map = amprincipals.getAutoMembership(IDP_VALID_AM, authorizable, true);
        assertEquals(3, map.size());
        
        // make sure membership lookup for automembershipGroup1 fails
        Group gr = mock(Group.class);
        when(gr.isGroup()).thenReturn(true);
        when(gr.memberOf()).thenThrow(new RepositoryException());
        
        when(um.getAuthorizable(automembershipGroup1.getPrincipal())).thenReturn(gr);
        
        // principals from cache: looking up group by principal returns 
        // 'gr' which fails upon memberof call -> should be skipped.
        map = amprincipals.getAutoMembership(IDP_VALID_AM, authorizable, true);
        assertEquals(2, map.size());

        verify(gr).isGroup();
        verify(gr).memberOf();
        verifyNoMoreInteractions(gr);
        reset(gr, um);
    }

    @Test
    public void testInheritedFromAMConfig() throws Exception {
        Group testGroup = getTestGroup(automembershipGroup3);

        AutoMembershipPrincipals amprincipals = new AutoMembershipPrincipals(userManager, Collections.emptyMap(), 
                getAutoMembershipConfigMapping());

        Map<Principal, Group> map = amprincipals.getAutoMembership(IDP_VALID_AM, authorizable, true);
        assertEquals(2, map.size());
        
        Set<Principal> principals = map.keySet();
        assertTrue(principals.contains(testGroup.getPrincipal()));
        assertTrue(principals.contains(automembershipGroup3.getPrincipal()));
    }

    @Test
    public void testInheritedGroupPrincipalInvalid() throws Exception {
        UserManager um = spy(userManager);
        
        Group testGroup = getTestGroup(automembershipGroup1);
        GroupPrincipal p = (GroupPrincipal) testGroup.getPrincipal();

        AutoMembershipPrincipals amprincipals = new AutoMembershipPrincipals(um, MAPPING, Collections.emptyMap());
        Map<Principal, Group> map = amprincipals.getAutoMembership(IDP_VALID_AM, authorizable, true);
        assertEquals(3, map.size());

        // make sure membership lookup for automembershipGroup1 fails
        Principal invalid = new PrincipalImpl("invalid");
        Group inherited =  mock(Group.class);
        when(inherited.isGroup()).thenReturn(true);
        when(inherited.getPrincipal()).thenReturn(invalid);

        Group gr = mock(Group.class);
        when(gr.isGroup()).thenReturn(true);
        when(gr.memberOf()).thenReturn(Iterators.singletonIterator(inherited));
        when(um.getAuthorizable(automembershipGroup1.getPrincipal())).thenReturn(gr);

        // retrieve from cache
        map = amprincipals.getAutoMembership(IDP_VALID_AM, authorizable, true);
        assertEquals(2, map.size());

        Set<Principal> result = map.keySet();
        assertFalse(result.contains(p));
        assertFalse(result.contains(invalid));
        assertFalse(map.containsValue(testGroup));

        verify(gr).isGroup();
        verify(gr).memberOf();
        
        verify(inherited).getPrincipal();
        verify(inherited).getID();
        verifyNoMoreInteractions(gr, inherited);
        reset(gr, inherited, um);
    }

    @Test
    public void testIsInheritedMemberGroupLookupFails() throws Exception {
        AutoMembershipPrincipals amprincipals = new AutoMembershipPrincipals(userManager, MAPPING, Collections.emptyMap());
        assertFalse(amprincipals.isInheritedMember(IDP_INVALID_AM, getTestGroup(), authorizable));
    }

    @Test
    public void testIsInheritedMemberConfiguredUser() throws Exception {
        Map<String, String[]> mapping = Map.of(IDP_INVALID_AM, new String[] {getTestUser().getID()});
        AutoMembershipPrincipals amprincipals = new AutoMembershipPrincipals(userManager, mapping, Collections.emptyMap());
        assertFalse(amprincipals.isInheritedMember(IDP_INVALID_AM, getTestGroup(), authorizable));
    }
    
    @Test
    public void testEmptyMapping() {
        Map<String, String[]> m = spy(new HashMap<>());
        Map<String, AutoMembershipConfig> m2 = spy(new HashMap<>());
        UserManager um = spy(userManager);
        AutoMembershipPrincipals amp = new AutoMembershipPrincipals(um, m, m2);
        
        // verify 'getAutoMembership'
        assertTrue(amp.getAutoMembership(IDP_VALID_AM, authorizable, false).isEmpty());
        
        verify(m, times(1)).size();
        verify(m, times(1)).get(IDP_VALID_AM);
        verifyNoMoreInteractions(m);

        verify(m2, times(1)).get(IDP_VALID_AM);
        verifyNoMoreInteractions(m2);

        // verify 'getGlobalConfiguredIdpNames' (cache has been populated in the previous call)
        assertTrue(amp.getConfiguredIdpNames(() -> AUTOMEMBERSHIP_GROUP_ID_1).isEmpty());
        verify(m, times(1)).get(IDP_VALID_AM);
        verifyNoMoreInteractions(m2);

        // verify 'isMember'
        clearInvocations(m, m2);
        assertFalse(amp.isMember(IDP_VALID_AM, AUTOMEMBERSHIP_GROUP_ID_3, authorizable));
        verify(m).get(IDP_VALID_AM);
        verify(m2).get(IDP_VALID_AM);
        verifyNoMoreInteractions(m, m2);
        
        // um/authorizable have not been touched at all.
        verifyNoInteractions(um, authorizable);
    }

    @Test
    public void testEmptyMapping2() {
        Map<String, String[]> m = spy(new HashMap<>());
        Map<String, AutoMembershipConfig> m2 = spy(new HashMap<>());

        UserManager um = spy(userManager);
        AutoMembershipPrincipals amprincipals = new AutoMembershipPrincipals(um, m, m2);
        
        assertTrue(amprincipals.getConfiguredIdpNames(() -> AUTOMEMBERSHIP_GROUP_ID_1).isEmpty());
        assertTrue(amprincipals.getConfiguredIdpNames(() -> AUTOMEMBERSHIP_GROUP_ID_2).isEmpty());
        assertTrue(amprincipals.getConfiguredIdpNames(() -> NON_EXISTING_GROUP_ID).isEmpty());

        verify(m, times(3)).isEmpty();
        verify(m).size();
        verifyNoMoreInteractions(m);
        verifyNoInteractions(m2);

        assertTrue(amprincipals.getAutoMembership(IDP_VALID_AM, authorizable, false).isEmpty());

        verify(m, times(1)).get(anyString());
        verify(m2, times(1)).get(anyString());
        verifyNoMoreInteractions(m, m2);
        verifyNoInteractions(um, authorizable);
    }

    @Test
    public void testGetConfiguredIdpNames() {
        assertEquals(Set.of(IDP_VALID_AM, IDP_MIXED_AM), amp.getConfiguredIdpNames(() -> AUTOMEMBERSHIP_GROUP_ID_1));
        assertEquals(Set.of(IDP_VALID_AM), amp.getConfiguredIdpNames(() -> AUTOMEMBERSHIP_GROUP_ID_2));
        assertTrue(amp.getConfiguredIdpNames(() -> NON_EXISTING_GROUP_ID).isEmpty());
    }
    
    @Test
    public void testIsMember() {
        assertFalse(amp.isMember(IDP_INVALID_AM, AUTOMEMBERSHIP_GROUP_ID_1, authorizable));
        assertFalse(amp.isMember(IDP_INVALID_AM, AUTOMEMBERSHIP_GROUP_ID_2, authorizable));
        assertFalse(amp.isMember(IDP_INVALID_AM, AUTOMEMBERSHIP_GROUP_ID_3, authorizable));
        
        assertTrue(amp.isMember(IDP_VALID_AM, AUTOMEMBERSHIP_GROUP_ID_1, authorizable));
        assertTrue(amp.isMember(IDP_VALID_AM, AUTOMEMBERSHIP_GROUP_ID_2, authorizable));
        assertTrue(amp.isMember(IDP_VALID_AM, AUTOMEMBERSHIP_GROUP_ID_3, authorizable));
        
        assertTrue(amp.isMember(IDP_MIXED_AM, AUTOMEMBERSHIP_GROUP_ID_1, authorizable));
        assertFalse(amp.isMember(IDP_MIXED_AM, AUTOMEMBERSHIP_GROUP_ID_2, authorizable));
        assertFalse(amp.isMember(IDP_MIXED_AM, AUTOMEMBERSHIP_GROUP_ID_3, authorizable));
    }
    
    @Test
    public void testIsNotMember() throws Exception {
        Group testGroup = getTestGroup();
        assertFalse(amp.isMember(IDP_VALID_AM, testGroup.getID(), authorizable));
        
        Group nested = userManager.createGroup("nestedGroup");
        nested.addMember(testGroup);
        assertFalse(amp.isMember(IDP_VALID_AM, nested.getID(), authorizable));
    }

    @Test
    public void testIsNotInheritedMember() throws Exception {
        Group testGroup = getTestGroup();
        assertFalse(amp.isInheritedMember(IDP_VALID_AM, testGroup, authorizable));
        
        Group nested = userManager.createGroup("nestedGroup");
        nested.addMember(testGroup);
        assertFalse(amp.isInheritedMember(IDP_VALID_AM, nested, authorizable));
    }
    
    @Test
    public void testGetMembersFromAutoMembershipConfig() {
        assertFalse(amp.getMembersFromAutoMembershipConfig(automembershipGroup1).hasNext());
        assertFalse(amp.getMembersFromAutoMembershipConfig(automembershipGroup2).hasNext());
        
        assertTrue(Iterators.elementsEqual(Iterators.singletonIterator(authorizable), amp.getMembersFromAutoMembershipConfig(automembershipGroup3)));
    }
}