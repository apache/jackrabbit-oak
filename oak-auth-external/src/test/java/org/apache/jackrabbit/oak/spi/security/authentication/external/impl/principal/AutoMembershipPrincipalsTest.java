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
import org.apache.jackrabbit.api.security.user.UserManager;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AutoMembershipPrincipalsTest extends AbstractAutoMembershipTest {

    private AutoMembershipPrincipals amp;
    
    @Before
    public void before() throws Exception {
        super.before();
        amp = new AutoMembershipPrincipals(userManager, MAPPING);
    }

    @Test
    public void testGetPrincipalsUnknownIdp() {
        assertTrue(amp.getPrincipals("unknown").isEmpty());
    }

    @Test
    public void testGetPrincipalsUnknownGroup() {
        Collection<Principal> principals = amp.getPrincipals(IDP_INVALID_AM);
        assertTrue(principals.isEmpty());
    }

    @Test
    public void testGetPrincipalsMultipleGroups() throws Exception {
        Collection<Principal> principals = amp.getPrincipals(IDP_VALID_AM);
        assertFalse(principals.isEmpty());
        assertEquals(ImmutableSet.of(automembershipGroup1.getPrincipal(), automembershipGroup2.getPrincipal()), ImmutableSet.copyOf(principals));
    }
    
    @Test
    public void testGetPrincipalsMixed() throws Exception {
        Collection<Principal> principals = amp.getPrincipals(IDP_MIXED_AM);
        assertFalse(principals.isEmpty());
        assertEquals(ImmutableSet.of(automembershipGroup1.getPrincipal()), ImmutableSet.copyOf(principals));
    }
    
    @Test
    public void testGroupLookupFails() throws Exception {
        UserManager um = spy(userManager);
        when(um.getAuthorizable(anyString())).thenThrow(new RepositoryException());
        
        AutoMembershipPrincipals amprincipals = new AutoMembershipPrincipals(um, MAPPING);
        assertTrue(amprincipals.getPrincipals(IDP_VALID_AM).isEmpty());
    }
    
    @Test
    public void testEmptyMapping() {
        Map<String, String[]> m = spy(new HashMap<>());
        AutoMembershipPrincipals amprincipals = new AutoMembershipPrincipals(userManager, m);
        
        assertTrue(amprincipals.getPrincipals(null).isEmpty());
        assertTrue(amprincipals.getPrincipals(IDP_VALID_AM).isEmpty());
        
        verify(m, times(1)).size();
        verify(m, times(1)).get(anyString());
        verifyNoMoreInteractions(m);
    }
}