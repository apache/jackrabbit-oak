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
import java.security.acl.Group;
import java.util.Iterator;
import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CompositePrincipalProviderTest {

    private final TestPrincipalProvider pp1 = new TestPrincipalProvider();
    private final TestPrincipalProvider pp2 = new TestPrincipalProvider("p1", "p2");
    private final PrincipalProvider cpp = CompositePrincipalProvider.of(ImmutableList.<PrincipalProvider>of(pp1, pp2));

    private Iterable<Principal> testPrincipals() {
        return Iterables.concat(pp1.getTestPrincipals(), pp2.getTestPrincipals());
    }

    private static void assertIterator(@Nonnull Iterable<? extends Principal> expected, @Nonnull Iterator<? extends Principal> result) {
        assertEquals(ImmutableSet.copyOf(expected), ImmutableSet.copyOf(result));
    }

    @Test
    public void testOfEmptyList() {
        assertSame(EmptyPrincipalProvider.INSTANCE, CompositePrincipalProvider.of(ImmutableList.<PrincipalProvider>of()));
    }

    @Test
    public void testOfSingletonList() {
        PrincipalProvider pp = new TestPrincipalProvider(true);
        assertSame(pp, CompositePrincipalProvider.of(ImmutableList.of(pp)));
    }

    @Test
    public void testOfList() {
        assertNotSame(pp1, cpp);
        assertNotSame(pp2, cpp);
        assertTrue(cpp instanceof CompositePrincipalProvider);
    }

    @Test
    public void getPrincipalUnknown() {
        assertNull(cpp.getPrincipal(TestPrincipalProvider.UNKNOWN.getName()));
    }

    @Test
    public void getPrincipal() {
        for (Principal principal : testPrincipals()) {
            assertEquals(principal, cpp.getPrincipal(principal.getName()));
        }
    }

    @Test
    public void getGroupMembership() {
        for (Principal principal : testPrincipals()) {
            boolean atleastEveryone = cpp.getGroupMembership(principal).contains(EveryonePrincipal.getInstance());
            assertTrue("All principals (except everyone) must be member of the everyone group. Violation: "+principal.getName(), atleastEveryone);
        }
    }

    @Test
    public void getGroupMembershipUnknown() {
        assertTrue(cpp.getGroupMembership(TestPrincipalProvider.UNKNOWN).isEmpty());
    }

    @Test
    public void testGetPrincipalsByUnknownId() {
        assertTrue(cpp.getPrincipals(TestPrincipalProvider.getIDFromPrincipal(TestPrincipalProvider.UNKNOWN)).isEmpty());
    }

    @Test
    public void findPrincipalsUnknown() {
        assertFalse(cpp.findPrincipals(TestPrincipalProvider.UNKNOWN.getName(), PrincipalManager.SEARCH_TYPE_ALL).hasNext());
        assertFalse(cpp.findPrincipals(TestPrincipalProvider.UNKNOWN.getName(), PrincipalManager.SEARCH_TYPE_NOT_GROUP).hasNext());
        assertFalse(cpp.findPrincipals(TestPrincipalProvider.UNKNOWN.getName(), PrincipalManager.SEARCH_TYPE_GROUP).hasNext());
    }

    @Test
    public void findPrincipalsByTypeGroup() {
        Iterable<? extends Principal> expected = Iterables.concat(ImmutableSet.of(EveryonePrincipal.getInstance()), Iterables.filter(testPrincipals(), new Predicate<Principal>() {
            @Override
            public boolean apply(Principal input) {
                return input instanceof Group;
            }
        }));

        Iterator<? extends Principal> result = cpp.findPrincipals(PrincipalManager.SEARCH_TYPE_GROUP);
        assertIterator(expected, result);
    }

    @Test
    public void findPrincipalsByTypeNotGroup() {
        Iterable<? extends Principal> expected = Iterables.filter(testPrincipals(), new Predicate<Principal>() {
            @Override
            public boolean apply(Principal input) {
                return !(input instanceof Group);
            }
        });

        Iterator<? extends Principal> result = cpp.findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        assertIterator(expected, result);
    }

    @Test
    public void findPrincipalsByTypeAll() {
        Iterator<? extends Principal> result = cpp.findPrincipals(PrincipalManager.SEARCH_TYPE_ALL);
        assertIterator(Iterables.concat(ImmutableSet.of(EveryonePrincipal.getInstance()), testPrincipals()), result);
    }
}