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
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipConfig;
import org.junit.After;
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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AutoMembershipPrincipalsTest extends AbstractAutoMembershipTest {

    static final String AUTOMEMBERSHIP_GROUP_ID_3 = "autoMembershipGroupId_3";
    
    private AutoMembershipPrincipals amp;
    private final Authorizable authorizable = mock(Authorizable.class);
    
    private Group automembershipGroup3;
    private final AutoMembershipConfig amConfig = mock(AutoMembershipConfig.class);
    
    @Before
    public void before() throws Exception {
        super.before();
        amp = new AutoMembershipPrincipals(userManager, MAPPING, getAutoMembershipConfigMapping());

        automembershipGroup3 = userManager.createGroup(AUTOMEMBERSHIP_GROUP_ID_3);
        root.commit();
        
        when(amConfig.getAutoMembership(authorizable)).thenReturn(ImmutableSet.of(automembershipGroup3.getID()));
        when(amConfig.getAutoMembers(any(UserManager.class), any(Group.class))).thenReturn(Iterators.emptyIterator());
        when(amConfig.getAutoMembers(userManager, automembershipGroup3)).thenReturn(Iterators.singletonIterator(authorizable));
    }
    
    @After
    public void after() throws Exception {
        try {
            automembershipGroup3.remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @Override
    Map<String, AutoMembershipConfig> getAutoMembershipConfigMapping() {
        return Collections.singletonMap(IDP_VALID_AM, amConfig);
    }

    @Test
    public void testGetPrincipalsUnknownIdp() {
        assertTrue(amp.getAutoMembership("unknown",authorizable).isEmpty());
        verifyNoInteractions(authorizable, amConfig);
    }

    @Test
    public void testGetPrincipalsUnknownGroup() {
        Collection<Principal> principals = amp.getAutoMembership(IDP_INVALID_AM, authorizable);
        assertTrue(principals.isEmpty());
        verifyNoInteractions(authorizable, amConfig);
    }

    @Test
    public void testGetPrincipalsMultipleGroups() throws Exception {
        when(amConfig.getAutoMembership(authorizable)).thenReturn(Collections.emptySet());

        Collection<Principal> principals = amp.getAutoMembership(IDP_VALID_AM, authorizable);
        assertFalse(principals.isEmpty());
        Set<Principal> expected = Sets.newHashSet(automembershipGroup1.getPrincipal(), automembershipGroup2.getPrincipal());
        assertEquals(expected, principals);

        // change behavior of automembership-config
        when(amConfig.getAutoMembership(authorizable)).thenReturn(ImmutableSet.of(automembershipGroup3.getID()));
        
        principals = amp.getAutoMembership(IDP_VALID_AM, authorizable);
        assertFalse(principals.isEmpty());
        expected.add(automembershipGroup3.getPrincipal());
        assertEquals(expected, principals);
        
        verifyNoInteractions(authorizable);
        verify(amConfig, times(2)).getAutoMembership(authorizable);
    }
    
    @Test
    public void testGetPrincipalsMixed() throws Exception {
        Collection<Principal> principals = amp.getAutoMembership(IDP_MIXED_AM, authorizable);
        assertFalse(principals.isEmpty());
        assertEquals(ImmutableSet.of(automembershipGroup1.getPrincipal()), ImmutableSet.copyOf(principals));
        verifyNoInteractions(authorizable, amConfig);
    }
    
    @Test
    public void testGroupLookupFails() throws Exception {
        UserManager um = spy(userManager);
        when(um.getAuthorizable(anyString())).thenThrow(new RepositoryException());
        
        AutoMembershipPrincipals amprincipals = new AutoMembershipPrincipals(um, MAPPING, getAutoMembershipConfigMapping());
        assertTrue(amprincipals.getAutoMembership(IDP_VALID_AM, authorizable).isEmpty());
        verifyNoInteractions(authorizable);
        verify(amConfig).getAutoMembership(authorizable);
        verifyNoMoreInteractions(amConfig);
    }
    
    @Test
    public void testEmptyMapping() {
        Map<String, String[]> m = spy(new HashMap<>());
        Map<String, AutoMembershipConfig> m2 = spy(new HashMap<>());
        UserManager um = spy(userManager);
        AutoMembershipPrincipals amp = new AutoMembershipPrincipals(um, m, m2);
        
        // verify 'getAutoMembership'
        assertTrue(amp.getAutoMembership(IDP_VALID_AM, authorizable).isEmpty());
        
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

        assertTrue(amprincipals.getAutoMembership(IDP_VALID_AM, authorizable).isEmpty());

        verify(m, times(1)).get(anyString());
        verify(m2, times(1)).get(anyString());
        verifyNoMoreInteractions(m, m2);
        verifyNoInteractions(um, authorizable);
    }

    @Test
    public void testGetConfiguredIdpNames() {
        assertEquals(ImmutableSet.of(IDP_VALID_AM, IDP_MIXED_AM), amp.getConfiguredIdpNames(() -> AUTOMEMBERSHIP_GROUP_ID_1));
        assertEquals(ImmutableSet.of(IDP_VALID_AM), amp.getConfiguredIdpNames(() -> AUTOMEMBERSHIP_GROUP_ID_2));
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
    public void testGetMembersFromAutoMembershipConfig() {
        assertFalse(amp.getMembersFromAutoMembershipConfig(automembershipGroup1).hasNext());
        assertFalse(amp.getMembersFromAutoMembershipConfig(automembershipGroup2).hasNext());
        
        assertTrue(Iterators.elementsEqual(Iterators.singletonIterator(authorizable), amp.getMembersFromAutoMembershipConfig(automembershipGroup3)));
    }
}