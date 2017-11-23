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
import java.util.Enumeration;
import java.util.Iterator;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PrincipalManagerImplTest {

    private final TestPrincipalProvider provider = new TestPrincipalProvider();
    private final PrincipalManagerImpl principalMgr = new PrincipalManagerImpl(provider);
    private Iterable<Principal> testPrincipals = provider.getTestPrincipals();

    private static boolean isGroup(Principal p) {
        return p instanceof Group;
    }

    private static void assertIterator(@Nonnull Iterator<? extends Principal> expected, @Nonnull Iterator<? extends Principal> result) {
        assertEquals(ImmutableSet.copyOf(expected), ImmutableSet.copyOf(result));
    }

    @Test
    public void testGetEveryone() {
        Principal principal = principalMgr.getEveryone();
        assertTrue(isGroup(principal));
    }

    @Test
    public void testGetEveryone2() {
        Principal principal = new PrincipalManagerImpl(new TestPrincipalProvider(false)).getEveryone();
        assertSame(EveryonePrincipal.getInstance(), principal);
    }

    @Test
    public void testGetPrincipalEveryone() {
        assertEquals(EveryonePrincipal.getInstance(), principalMgr.getPrincipal(EveryonePrincipal.NAME));
    }

    @Test
    public void testHasPrincipalEveryone() {
        assertTrue(principalMgr.hasPrincipal(EveryonePrincipal.NAME));
    }

    @Test
    public void testHasPrincipal() {
        for (Principal pcpl : testPrincipals) {
            assertTrue(principalMgr.hasPrincipal(pcpl.getName()));
        }
    }

    @Test
    public void testHasPrincipalUnknown() {
        assertFalse(principalMgr.hasPrincipal(TestPrincipalProvider.UNKNOWN.getName()));
    }

    @Test
    public void testGetPrincipalUnknown() {
        assertNull(principalMgr.getPrincipal(TestPrincipalProvider.UNKNOWN.getName()));
    }

    @Test
    public void testGetPrincipal() {
        for (Principal principal : testPrincipals){
            Principal pp = principalMgr.getPrincipal(principal.getName());
            assertNotNull(pp);
            assertEquals("PrincipalManager.getPrincipal returned Principal with different Name", principal.getName(), pp.getName());
            assertEquals("PrincipalManager.getPrincipal returned different Principal", principal, pp);
        }
    }

    @Test
    public void testGetPrincipalsNonGroup() {
        Iterator<? extends Principal> expected = provider.findPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        PrincipalIterator it = principalMgr.getPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);

        assertIterator(expected, it);
    }

    @Test
    public void testGetPrincipalsNonGroupContainsNoGroups() {
        PrincipalIterator it = principalMgr.getPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            assertFalse(isGroup(p));
        }
    }

    @Test
    public void testGetPrincipalsGroup() {
        Iterator<? extends Principal> expected = provider.findPrincipals(PrincipalManager.SEARCH_TYPE_GROUP);
        PrincipalIterator it = principalMgr.getPrincipals(PrincipalManager.SEARCH_TYPE_GROUP);

        assertIterator(expected, it);
    }

    @Test
    public void testGetPrincipalsGroupContainsGroups() {
        PrincipalIterator it = principalMgr.getPrincipals(PrincipalManager.SEARCH_TYPE_GROUP);
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            assertTrue(isGroup(p));
        }
    }

    @Test
    public void testGetPrincipalsAll() {
        Iterator<? extends Principal> expected = provider.findPrincipals(PrincipalManager.SEARCH_TYPE_ALL);
        PrincipalIterator it = principalMgr.getPrincipals(PrincipalManager.SEARCH_TYPE_ALL);

        assertIterator(expected, it);
    }

    @Test
    public void testAllMembersKnown() {
        for (Principal p : testPrincipals) {
            if (isGroup(p)) {
                Enumeration<? extends Principal> en = ((Group) p).members();
                while (en.hasMoreElements()) {
                    Principal memb = en.nextElement();
                    assertTrue(principalMgr.hasPrincipal(memb.getName()));
                }
            }
        }
    }

    @Test
    public void testGroupMembershipNonGroup() {
        assertMembership(principalMgr, PrincipalManager.SEARCH_TYPE_NOT_GROUP);
    }

    @Test
    public void testGroupMembershipGroup() {
        assertMembership(principalMgr, PrincipalManager.SEARCH_TYPE_GROUP);
    }

    @Test
    public void testGroupMembershipAll() {
        assertMembership(principalMgr, PrincipalManager.SEARCH_TYPE_ALL);
    }

    private static void assertMembership(@Nonnull PrincipalManager mgr, int searchType) {
        PrincipalIterator it = mgr.getPrincipals(searchType);
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            if (p.equals(EveryonePrincipal.getInstance())) {
                continue;
            }
            boolean atleastEveryone = false;
            for (PrincipalIterator membership = mgr.getGroupMembership(p); membership.hasNext();) {
                Principal gr = membership.nextPrincipal();
                assertTrue(isGroup(gr));
                if (gr.equals(EveryonePrincipal.getInstance())) {
                    atleastEveryone = true;
                }
            }
            assertTrue("All principals (except everyone) must be member of the everyone group.", atleastEveryone);
        }
    }

    @Test
    public void testGetGroupMembershipEveryoneEmpty() {
        assertFalse(principalMgr.getGroupMembership(EveryonePrincipal.getInstance()).hasNext());
    }

    @Test
    public void testGetGroupMembershipEveryoneWithoutEveryone() {
        assertFalse(Iterators.contains(principalMgr.getGroupMembership(EveryonePrincipal.getInstance()), EveryonePrincipal.getInstance()));
    }

    @Test
    public void testGetMembersConsistentWithMembership() {
        Principal everyone = principalMgr.getEveryone();
        PrincipalIterator it = principalMgr.getPrincipals(PrincipalManager.SEARCH_TYPE_GROUP);
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            if (p.equals(everyone)) {
                continue;
            }

            assertTrue(isGroup(p));

            Enumeration<? extends Principal> members = ((Group) p).members();
            while (members.hasMoreElements()) {
                Principal memb = members.nextElement();

                Principal group = null;
                PrincipalIterator mship = principalMgr.getGroupMembership(memb);
                while (mship.hasNext() && group == null) {
                    Principal gr = mship.nextPrincipal();
                    if (p.equals(gr)) {
                        group = gr;
                    }
                }
                assertNotNull("Group member " + memb.getName() + "does not reveal group upon getGroupMembership", p.getName());
            }
        }
    }

    @Test
    public void testFindPrincipal() {
        for (Principal pcpl : testPrincipals) {

            PrincipalIterator it = principalMgr.findPrincipals(pcpl.getName());
            assertTrue("findPrincipals does not find principal with filter '" + pcpl.getName() + '\'', Iterators.contains(it, pcpl));
        }
    }

    @Test
    public void testFindPrincipalByTypeGroup() {
        for (Principal pcpl : testPrincipals) {
            if (isGroup(pcpl)) {
                PrincipalIterator it = principalMgr.findPrincipals(pcpl.getName(), PrincipalManager.SEARCH_TYPE_GROUP);
                assertTrue("findPrincipals does not find principal with filter '" + pcpl.getName() + '\'', Iterators.contains(it, pcpl));
            } else {
                PrincipalIterator it = principalMgr.findPrincipals(pcpl.getName(), PrincipalManager.SEARCH_TYPE_NOT_GROUP);
                assertTrue("findPrincipals does not find principal with filter '" + pcpl.getName() + '\'', Iterators.contains(it, pcpl));
            }
        }
    }


    @Test
    public void testFindPrincipalByType() {
        for (Principal pcpl : testPrincipals) {
            if (isGroup(pcpl)) {
                PrincipalIterator it = principalMgr.findPrincipals(pcpl.getName(), PrincipalManager.SEARCH_TYPE_GROUP);
                assertTrue("findPrincipals does not find principal with filter '" + pcpl.getName() + '\'', Iterators.contains(it, pcpl));
            } else {
                PrincipalIterator it = principalMgr.findPrincipals(pcpl.getName(), PrincipalManager.SEARCH_TYPE_NOT_GROUP);
                assertTrue("findPrincipals does not find principal with filter '" + pcpl.getName() + '\'', Iterators.contains(it, pcpl));
            }
        }
    }

    @Test
    public void testFindPrincipalByTypeAll() {
        for (Principal pcpl : testPrincipals) {
            PrincipalIterator it = principalMgr.findPrincipals(pcpl.getName(), PrincipalManager.SEARCH_TYPE_ALL);
            assertTrue("findPrincipals does not find principal with filter '" + pcpl.getName() + '\'', Iterators.contains(it, pcpl));
        }
    }

    @Test
    public void testFindEveryone() {
        // untyped search -> everyone must be part of the result set
        PrincipalIterator it = principalMgr.findPrincipals(EveryonePrincipal.NAME);
        assertTrue("findPrincipals does not find principal with filter '" + EveryonePrincipal.NAME + '\'', Iterators.contains(it, EveryonePrincipal.getInstance()));
    }

    @Test
    public void testFindEveryoneTypeGroup() {
        // search group only -> everyone must be part of the result set
        PrincipalIterator it = principalMgr.findPrincipals(EveryonePrincipal.NAME, PrincipalManager.SEARCH_TYPE_GROUP);
        assertTrue("findPrincipals does not find principal with filter '" + EveryonePrincipal.NAME + '\'', Iterators.contains(it, EveryonePrincipal.getInstance()));
    }

    @Test
    public void testFindEveryoneTypeNonGroup() {
        // search non-group only -> everyone should not be part of the result set
        PrincipalIterator it = principalMgr.findPrincipals(EveryonePrincipal.NAME, PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        assertFalse("findPrincipals did find principal with filter '" + EveryonePrincipal.NAME + '\'', Iterators.contains(it, EveryonePrincipal.getInstance()));
    }

    @Test
    public void testFindUnknownByTypeAll() {
        String unknownHint = TestPrincipalProvider.UNKNOWN.getName().substring(0, 4);
        assertFalse(principalMgr.findPrincipals(unknownHint, PrincipalManager.SEARCH_TYPE_ALL).hasNext());
    }

    @Test
    public void testFindUnknownByTypeGroup() {
        String unknownHint = TestPrincipalProvider.UNKNOWN.getName().substring(0, 4);
        assertFalse(principalMgr.findPrincipals(unknownHint, PrincipalManager.SEARCH_TYPE_GROUP).hasNext());
    }

    @Test
    public void testFindUnknownByTypeNotGroup() {
        String unknownHint = TestPrincipalProvider.UNKNOWN.getName().substring(0, 4);
        assertFalse(principalMgr.findPrincipals(unknownHint, PrincipalManager.SEARCH_TYPE_NOT_GROUP).hasNext());
    }
}