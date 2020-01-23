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
package org.apache.jackrabbit.oak.spi.security.principal;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.junit.Test;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GroupPrincipalsTest {

    @Test
    public void testIsGroup() {
        Principal p0 = new PrincipalImpl("test");
        assertFalse(GroupPrincipals.isGroup(p0));

        Group g = new Group() {

            @Override
            public String getName() {
                return "testG";
            }

            @Override
            public boolean removeMember(Principal user) {
                return false;
            }

            @Override
            public Enumeration<? extends Principal> members() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isMember(Principal member) {
                return false;
            }

            @Override
            public boolean addMember(Principal user) {
                return false;
            }
        };

        assertTrue(GroupPrincipals.isGroup(g));
        assertTrue(GroupPrincipals.isGroup(new GroupPrincipalWrapper(g)));
    }

    @Test
    public void testTransformGroupSet() {
        Group g = new Group() {

            @Override
            public String getName() {
                return "testG";
            }

            @Override
            public boolean removeMember(Principal user) {
                return false;
            }

            @Override
            public Enumeration<? extends Principal> members() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isMember(Principal member) {
                return false;
            }

            @Override
            public boolean addMember(Principal user) {
                return false;
            }
        };

        Set<Principal> t = GroupPrincipals.transform(ImmutableSet.of(g));
        assertEquals(1, t.size());
        Principal p = t.iterator().next();
        assertEquals(p.getName(), g.getName());
    }

    @Test
    public void testTransformEnumeration() {
        Group g = when(mock(Group.class).getName()).thenReturn("g").getMock();
        Set<Principal> set = ImmutableSet.of(g, mock(Principal.class), mock(GroupPrincipal.class));
        Enumeration<? extends Principal> e = GroupPrincipals.transform(Collections.enumeration(set));

        Set<Principal> t = Sets.newHashSet(Iterators.forEnumeration(e));
        assertEquals(set.size(), t.size());
        for (Principal p : t) {
            assertFalse(p instanceof Group);
        }
    }

    @Test
    public void testTransformEmptyEnumeration() {
        Enumeration members = Collections.emptyEnumeration();

        Enumeration<Principal> t = GroupPrincipals.transform(members);
        assertFalse(t.hasMoreElements());
    }

    @Test
    public void testMembersOfNonGroup() {
        assertEquals(Collections.emptyEnumeration(), GroupPrincipals.members(mock(Principal.class)));
    }

    @Test
    public void testMembersOfGroupPrincipal() {
        GroupPrincipal gp = mock(GroupPrincipal.class);

        GroupPrincipals.members(gp);
        verify(gp, times(1)).members();
    }

    @Test
    public void testMembersOfGroup() {
        Group g = mock(Group.class);

        GroupPrincipals.members(g);
        verify(g, times(1)).members();
    }

    @Test
    public void testIsMemberOfNonGroup() {
        assertFalse(GroupPrincipals.isMember(mock(Principal.class), mock(Principal.class)));
    }

    @Test
    public void testIsMemberOfGroupPrincipal() {
        GroupPrincipal gp = mock(GroupPrincipal.class);
        Principal p = mock(Principal.class);

        GroupPrincipals.isMember(gp, p);
        verify(gp, times(1)).isMember(p);
    }

    @Test
    public void testIsMemberOfGroup() {
        Group g = mock(Group.class);
        Principal p = mock(Principal.class);

        GroupPrincipals.isMember(g, p);
        verify(g, times(1)).isMember(p);
    }
}
