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
package org.apache.jackrabbit.api;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.security.Principal;
import java.util.Collection;

import javax.jcr.AccessDeniedException;
import javax.jcr.Item;
import javax.jcr.ItemNotFoundException;
import javax.jcr.Node;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.junit.Test;
import org.mockito.Answers;

public class JackrabbitSessionTest {
    
    @Test
    public void testGetParentOrNull() throws Exception {
        JackrabbitSession s = mock(JackrabbitSession.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));
        Node parent = mock(Node.class);
        Item item = mock(Item.class);
        
        when(item.getParent()).thenReturn(parent).getMock();
        assertSame(parent, s.getParentOrNull(item));
        
        doThrow(new AccessDeniedException()).when(item).getParent();
        assertNull(s.getParentOrNull(item));

        doThrow(new ItemNotFoundException()).when(item).getParent();
        assertNull(s.getParentOrNull(item));
    }

    @Test
    public void testGetBoundPrincipals() throws Exception {
        JackrabbitSession s = mock(JackrabbitSession.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));
        // no user id set
        IllegalStateException ise = assertThrows(IllegalStateException.class, s::getBoundPrincipals);
        assertTrue(ise.getMessage().contains("user ID"));
        when(s.getUserID()).thenReturn("admin");
        UserManager um = mock(UserManager.class);
        when(s.getUserManager()).thenReturn(um);
        // no authorizable found for user id
        ise = assertThrows(IllegalStateException.class, s::getBoundPrincipals);
        assertTrue(ise.getMessage().contains("No authorizable found for user ID"));
        // mock user manager to return principals
        Authorizable user = mock(User.class);
        when(um.getAuthorizable("admin")).thenReturn(user);
        PrincipalManager pm = mock(PrincipalManager.class);
        when(s.getPrincipalManager()).thenReturn(pm);
        Principal adminPrincipal = mock(Principal.class);
        when(user.getPrincipal()).thenReturn(adminPrincipal);
        when(adminPrincipal.getName()).thenReturn("admin");
        Principal everyonePrincipal = mock(Principal.class);
        when(everyonePrincipal.getName()).thenReturn("everyone");
        PrincipalIterator pi = new SingletonPrincipalIterator(everyonePrincipal);
        when(pm.getPrincipals(PrincipalManager.SEARCH_TYPE_ALL)).thenReturn(pi);
        when(pm.getGroupMembership(adminPrincipal)).thenReturn(pi);
        assertImmutablePrincipals(s.getBoundPrincipals(), "admin", "everyone"); // should not throw exception
    }

    private static final class SingletonPrincipalIterator implements PrincipalIterator {
        private final Principal principal;
        boolean hasNext = true;

        private SingletonPrincipalIterator(Principal principal) {
            this.principal = principal;
        }

        @Override
        public Principal nextPrincipal() {
            if(!hasNext) {
                throw new java.util.NoSuchElementException("No more principals in SingletonPrincipalIterator");
            } else {
                hasNext = false;
            }
            return principal;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public long getSize() {
            return 1;
        }

        @Override
        public void skip(long skipNum) {
            throw new UnsupportedOperationException("skip not supported in SingletonPrincipalIterator");
        }

        @Override
        public long getPosition() {
            if (hasNext) {
                return 0;
            } else {
                return 1;
            }
        }

        @Override
        public Object next() {
            return nextPrincipal();
        }
    }

    public static void assertImmutablePrincipals(Collection<Principal> actualPrincipals, String... expectedPrincipalNames) {
        assertNotNull(actualPrincipals);
        for (String expectedPrincipalName  : expectedPrincipalNames) {
            assertTrue("Given collection did not contain expected principal name \'" + expectedPrincipalName + "'", actualPrincipals.stream().anyMatch(p -> p.getName().equals(expectedPrincipalName) ));
        }
        // make sure it is not modifiable
        assertThrows(UnsupportedOperationException.class, actualPrincipals::clear);
    }
}
