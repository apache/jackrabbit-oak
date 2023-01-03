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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.security.Principal;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class GroupPrincipalsTest {

    @Test
    public void testIsGroup() {
        Principal p0 = new PrincipalImpl("test");
        assertFalse(GroupPrincipals.isGroup(p0));

        GroupPrincipal g = new GroupPrincipal() {

            @Override
            public String getName() {
                return "testG";
            }

            @Override
            public @NotNull Enumeration<? extends Principal> members() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isMember(@NotNull Principal member) {
                return false;
            }
        };

        assertTrue(GroupPrincipals.isGroup(g));
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
}
