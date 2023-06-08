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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.RepositoryException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AutoMembershipCycleTest extends AbstractAutoMembershipTest {

    private AutoMembershipPrincipals amp;
    private final Authorizable authorizable = mock(Authorizable.class);
    
    @Before
    public void before() throws Exception {
        super.before();
        Map<String, String[]> mapping = Collections.singletonMap(IDP_VALID_AM, new String[] {AUTOMEMBERSHIP_GROUP_ID_1});
        amp = new AutoMembershipPrincipals(userManager, mapping, getAutoMembershipConfigMapping());
    }
    
    @Test
    public void testIsInheritedMemberCircularIncludingAutoMembership() throws Exception {
        Group amGr1 = mockGroup(AUTOMEMBERSHIP_GROUP_ID_1);
        Group gr = mockGroup("testGroup");

        // mock a membership cycle
        List<Authorizable> members = Arrays.asList(authorizable, amGr1);
        when(gr.getDeclaredMembers()).thenReturn(members.iterator(), members.iterator(), members.iterator()).thenThrow(new RuntimeException("cicle"));
        when(amGr1.getDeclaredMembers()).thenReturn(Iterators.singletonIterator(gr), Iterators.singletonIterator(gr), Iterators.singletonIterator(gr)).thenThrow(new RuntimeException("cicle"));

        // declared automembership
        assertTrue(amp.isInheritedMember(IDP_VALID_AM, amGr1, authorizable));
        verify(amGr1, never()).getDeclaredMembers();

        // declared automembership in amGr1
        assertTrue(amp.isInheritedMember(IDP_VALID_AM, gr, authorizable));
        verify(gr).getDeclaredMembers();
        verify(amGr1, never()).getDeclaredMembers();
        clearInvocations(amGr1, gr);

        // declared automembership
        assertTrue(amp.isInheritedMember(IDP_VALID_AM, amGr1, gr));
        verify(amGr1, never()).getDeclaredMembers();
        verify(gr, never()).getDeclaredMembers();
        
        assertTrue(amp.isInheritedMember(IDP_VALID_AM, gr, amGr1));
        verify(gr).getDeclaredMembers();
        verify(amGr1, never()).getDeclaredMembers();
    }

    @Test
    public void testIsInheritedMemberCircularWithoutAutoMembership() throws Exception {
        // mock a membership cycle
        Group gr = mockGroup("testGroup");
        Group gr2 = mockGroup("testGroup2");

        when(gr.getDeclaredMembers()).thenReturn(Iterators.singletonIterator(gr2)).thenThrow(new RuntimeException("circle"));
        when(gr2.getDeclaredMembers()).thenReturn(Iterators.singletonIterator(gr)).thenThrow(new RuntimeException("circle"));

        assertFalse(amp.isInheritedMember(IDP_VALID_AM, gr, gr2));
        
        verify(gr, times(1)).getDeclaredMembers();
        verify(gr, times(2)).getID();
        verify(gr, times(1)).isGroup();
        verify(gr2, times(1)).getDeclaredMembers();
        verify(gr2, times(1)).getID();
        verify(gr2, times(1)).isGroup();
        verifyNoMoreInteractions(gr, gr2);
    }
    
    private static @NotNull Group mockGroup(@NotNull String id) throws RepositoryException {
        Group gr = when(mock(Group.class).isGroup()).thenReturn(true).getMock();
        when(gr.getID()).thenReturn(id);
        return gr;
    }
}