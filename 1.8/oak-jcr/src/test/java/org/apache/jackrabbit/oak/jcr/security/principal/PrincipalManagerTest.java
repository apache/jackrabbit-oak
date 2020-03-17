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
package org.apache.jackrabbit.oak.jcr.security.principal;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import javax.jcr.Credentials;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Before;
import org.junit.Test;

/**
 * {@code PrincipalManagerTest}...
 */
public class PrincipalManagerTest extends AbstractJCRTest {

    private PrincipalManager principalMgr;
    private Group everyone;

    private Principal[] adminPrincipals;

    @Before
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        if (!(superuser instanceof JackrabbitSession)) {
            superuser.logout();
            throw new NotExecutableException();
        }
        principalMgr = ((JackrabbitSession) superuser).getPrincipalManager();
        everyone = (Group) principalMgr.getEveryone();

        adminPrincipals = getPrincipals(getHelper().getSuperuserCredentials());
    }

    private Principal[] getPrincipals(Credentials credentials) throws Exception {
        Set<Principal> principals = new HashSet<Principal>();
        if (credentials instanceof SimpleCredentials) {
            Principal p = principalMgr.getPrincipal(((SimpleCredentials) credentials).getUserID());
            if (p != null) {
                principals.add(p);
                PrincipalIterator principalIterator = principalMgr.getGroupMembership(p);
                while (principalIterator.hasNext()) {
                    principals.add(principalIterator.nextPrincipal());
                }
            }
        }
        return principals.toArray(new Principal[principals.size()]);
    }

    private static boolean isGroup(Principal p) {
        return p instanceof java.security.acl.Group;
    }

    @Test
    public void testGetEveryone() {
        Principal principal = principalMgr.getEveryone();
        assertTrue(principal != null);
        assertTrue(isGroup(principal));
        assertEquals(EveryonePrincipal.getInstance(), principal);
        assertFalse(principal instanceof ItemBasedPrincipal);
    }

    /**
     * @since oak
     */
    @Test
    public void testGetEveryoneByName() {
        assertTrue(principalMgr.hasPrincipal(EveryonePrincipal.NAME));
        assertNotNull(principalMgr.getPrincipal(EveryonePrincipal.NAME));
        assertEquals(EveryonePrincipal.getInstance(), principalMgr.getPrincipal(EveryonePrincipal.NAME));
    }

    @Test
    public void testSuperUserIsEveryOne() {
        for (Principal pcpl : adminPrincipals) {
            if (!(pcpl.equals(everyone))) {
                assertTrue(everyone.isMember(pcpl));
            }
        }
    }

    @Test
    public void testReadOnlyIsEveryOne() throws Exception {
        Session s = getHelper().getReadOnlySession();
        try {
            Principal[] pcpls = getPrincipals(getHelper().getReadOnlyCredentials());
            for (Principal pcpl : pcpls) {
                if (!(pcpl.equals(everyone))) {
                    assertTrue(everyone.isMember(pcpl));
                }
            }
        } finally {
            s.logout();
        }
    }

    @Test
    public void testHasPrincipal() {
        assertTrue(principalMgr.hasPrincipal(everyone.getName()));

        for (Principal pcpl : adminPrincipals) {
            assertTrue(principalMgr.hasPrincipal(pcpl.getName()));
        }
    }

    @Test
    public void testGetPrincipal() {
        Principal p = principalMgr.getPrincipal(everyone.getName());
        assertEquals(everyone, p);

        for (Principal pcpl : adminPrincipals) {
            Principal pp = principalMgr.getPrincipal(pcpl.getName());
            assertEquals("PrincipalManager.getPrincipal returned Principal with different Name", pcpl.getName(), pp.getName());
        }
    }

    @Test
    public void testGetPrincipalGetName() {
        for (Principal pcpl : adminPrincipals) {
            Principal pp = principalMgr.getPrincipal(pcpl.getName());
            assertEquals("PrincipalManager.getPrincipal returned Principal with different Name", pcpl.getName(), pp.getName());
        }
    }

    @Test
    public void testGetPrincipals() {
        PrincipalIterator it = principalMgr.getPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            assertFalse(isGroup(p));
        }
    }

    @Test
    public void testGetGroupPrincipals() {
        PrincipalIterator it = principalMgr.getPrincipals(PrincipalManager.SEARCH_TYPE_GROUP);
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            assertTrue(isGroup(p));
        }
    }

    @Test
    public void testGetAllPrincipals() {
        PrincipalIterator it = principalMgr.getPrincipals(PrincipalManager.SEARCH_TYPE_ALL);
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            assertTrue(principalMgr.hasPrincipal(p.getName()));
            assertEquals(principalMgr.getPrincipal(p.getName()), p);
        }
    }

    @Test
    public void testMembers() {
        PrincipalIterator it = principalMgr.getPrincipals(PrincipalManager.SEARCH_TYPE_ALL);
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            if (p.equals(principalMgr.getEveryone())) {
                continue;
            }
            if (isGroup(p)) {
                Enumeration<? extends Principal> en = ((java.security.acl.Group) p).members();
                while (en.hasMoreElements()) {
                    Principal memb = en.nextElement();
                    assertTrue(principalMgr.hasPrincipal(memb.getName()));
                }
            }
        }
    }

    @Test
    public void testMembers2() throws Exception {
        Authorizable gr = null;
        try {
            gr = ((JackrabbitSession) superuser).getUserManager().createGroup(getClass().getName());
            superuser.save();
            PrincipalIterator it = principalMgr.getPrincipals(PrincipalManager.SEARCH_TYPE_ALL);
            while (it.hasNext()) {
                Principal p = it.nextPrincipal();
                if (p.equals(principalMgr.getEveryone())) {
                    continue;
                }
                if (isGroup(p)) {
                    Enumeration<? extends Principal> en = ((java.security.acl.Group) p).members();
                    while (en.hasMoreElements()) {
                        Principal memb = en.nextElement();
                        assertTrue(principalMgr.hasPrincipal(memb.getName()));
                    }
                }
            }
        } finally {
            if (gr != null) {
                gr.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testGroupMembership() {
        testMembership(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        testMembership(PrincipalManager.SEARCH_TYPE_GROUP);
        testMembership(PrincipalManager.SEARCH_TYPE_ALL);
    }

    private void testMembership(int searchType) {
        PrincipalIterator it = principalMgr.getPrincipals(searchType);
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            if (p.equals(everyone)) {
                continue;
            }
            boolean atleastEveryone = false;
            for (PrincipalIterator membership = principalMgr.getGroupMembership(p); membership.hasNext();) {
                Principal gr = membership.nextPrincipal();
                assertTrue(isGroup(gr));
                if (gr.equals(everyone)) {
                    atleastEveryone = true;
                }
            }
            assertTrue("All principals (except everyone) must be member of the everyone group.", atleastEveryone);
        }
    }

    @Test
    public void testEveryoneGroupMembership() {
        Principal everyone = EveryonePrincipal.getInstance();
        for (PrincipalIterator membership = principalMgr.getGroupMembership(everyone); membership.hasNext();) {
            Principal gr = membership.nextPrincipal();
            assertTrue(isGroup(gr));
            if (gr.equals(everyone)) {
                fail("Everyone must never be a member of the EveryOne group.");
            }
        }
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

            Enumeration<? extends Principal> members = ((java.security.acl.Group) p).members();
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
        for (Principal pcpl : adminPrincipals) {
            if (pcpl.equals(everyone)) {
                continue;
            }

            assertTrue(principalMgr.hasPrincipal(pcpl.getName()));

            PrincipalIterator it = principalMgr.findPrincipals(pcpl.getName());
            // search must find at least a single principal
            assertTrue("findPrincipals does not find principal with filter '" + pcpl.getName() + '\'', it.hasNext());
        }
    }

    @Test
    public void testFindPrincipalByType() {
        for (Principal pcpl : adminPrincipals) {
            if (pcpl.equals(everyone)) {
                // special case covered by another test
                continue;
            }

            assertTrue(principalMgr.hasPrincipal(pcpl.getName()));

            if (isGroup(pcpl)) {
                PrincipalIterator it = principalMgr.findPrincipals(pcpl.getName(),
                        PrincipalManager.SEARCH_TYPE_GROUP);
                // search must find at least a single matching group principal
                assertTrue("findPrincipals does not find principal with filter " + pcpl.getName(), it.hasNext());
            } else {
                PrincipalIterator it = principalMgr.findPrincipals(pcpl.getName(),
                        PrincipalManager.SEARCH_TYPE_NOT_GROUP);
                // search must find at least a single matching non-group principal
                assertTrue("findPrincipals does not find principal with filter '" + pcpl.getName() + "' and type " + PrincipalManager.SEARCH_TYPE_NOT_GROUP, it.hasNext());
            }
        }
    }

    @Test
    public void testFindPrincipalByTypeAll() {
        for (Principal pcpl : adminPrincipals) {
            if (pcpl.equals(everyone)) {
                // special case covered by another test
                continue;
            }

            assertTrue(principalMgr.hasPrincipal(pcpl.getName()));

            PrincipalIterator it = principalMgr.findPrincipals(pcpl.getName(), PrincipalManager.SEARCH_TYPE_ALL);
            PrincipalIterator it2 = principalMgr.findPrincipals(pcpl.getName());

            assertTrue("Principal "+ pcpl.getName() + " not found", it.hasNext());
            assertTrue("Principal "+ pcpl.getName() + " not found", it2.hasNext());

            // both search must reveal the same result and size
            assertTrue(it.getSize() == it2.getSize());

            Set<Principal> s1 = new HashSet<Principal>();
            Set<Principal> s2 = new HashSet<Principal>();
            while (it.hasNext() && it2.hasNext()) {
                s1.add(it.nextPrincipal());
                s2.add(it2.nextPrincipal());
            }

            assertEquals(s1, s2);
            assertFalse(it.hasNext() && it2.hasNext());
        }
    }

    @Test
    public void testFindEveryone() {
        Principal everyone = principalMgr.getEveryone();
        assertTrue(principalMgr.hasPrincipal(everyone.getName()));

        boolean containedInResult = false;

        // untyped search -> everyone must be part of the result set
        PrincipalIterator it = principalMgr.findPrincipals(everyone.getName());
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            if (p.getName().equals(everyone.getName())) {
                containedInResult = true;
            }
        }
        assertTrue(containedInResult);

        // search group only -> everyone must be part of the result set
        containedInResult = false;
        it = principalMgr.findPrincipals(everyone.getName(), PrincipalManager.SEARCH_TYPE_GROUP);
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            if (p.getName().equals(everyone.getName())) {
                containedInResult = true;
            }
        }
        assertTrue(containedInResult);

        // search non-group only -> everyone should not be part of the result set
        containedInResult = false;
        it = principalMgr.findPrincipals(everyone.getName(), PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        while (it.hasNext()) {
            Principal p = it.nextPrincipal();
            if (p.getName().equals(everyone.getName())) {
                containedInResult = true;
            }
        }
        assertFalse(containedInResult);
    }
}