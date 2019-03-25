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

import java.security.Principal;
import java.util.Enumeration;

import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.JackrabbitPrincipal;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class EveryonePrincipalTest  {

    private final EveryonePrincipal everyone = EveryonePrincipal.getInstance();

    @Test
    public void testGetName() {
        assertEquals(EveryonePrincipal.NAME, everyone.getName());
    }

    @Test
    public void testAddMember() {
        assertFalse(everyone.addMember(mock(Principal.class)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveMember() {
        everyone.removeMember(mock(Principal.class));
    }

    @Test
    public void testIsMember() {
        assertTrue(everyone.isMember(new PrincipalImpl("test")));
    }

    @Test
    public void testIsMemberSelf() {
        assertFalse(everyone.isMember(everyone));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMembers() {
        EveryonePrincipal.getInstance().members();
    }

    @Test
    public void testEquals() {
        assertEquals(everyone, EveryonePrincipal.getInstance());
    }

    @Test
    public void testSame() {
        assertSame(everyone, EveryonePrincipal.getInstance());
    }

    @Test
    public void testHashCode() {
        assertEquals(everyone.hashCode(), EveryonePrincipal.getInstance().hashCode());
    }

    @Test
    public void testNotEqualsOtherPrincipalWithSameName() {
        Principal someotherEveryone = new Principal() {
            public String getName() {
                return EveryonePrincipal.NAME;
            }
        };
        assertNotEquals(everyone, someotherEveryone);
    }

    @Test
    public void testEqualsOtherJackrabbitPrincipal() {
        Principal someotherEveryone = new OtherEveryone();
        assertNotEquals(everyone, someotherEveryone);
    }

    @Test
    public void testEqualsOtherJackrabbitGroup() {
        Principal someotherEveryone = new OtherEveryoneGroup();
        assertEquals(everyone, someotherEveryone);
    }

    @Test
    public void testToString() {
        assertNotNull(everyone.toString());
        assertNotEquals(EveryonePrincipal.NAME, everyone.toString());
    }

    //--------------------------------------------------------------------------

    private class OtherEveryone implements JackrabbitPrincipal {
        public String getName() {
            return EveryonePrincipal.NAME;
        }
        @Override
        public boolean equals(Object o) {
            if (o instanceof JackrabbitPrincipal) {
                return getName().equals(((JackrabbitPrincipal) o).getName());
            }
            return false;
        }
        @Override
        public int hashCode() {
            return getName().hashCode();
        }
    }

    private class OtherEveryoneGroup extends OtherEveryone implements GroupPrincipal {

        @Override
        public boolean isMember(Principal principal) {
            return true;
        }

        @Override
        public Enumeration<? extends Principal> members() {
            throw new UnsupportedOperationException();
        }
    }
}
