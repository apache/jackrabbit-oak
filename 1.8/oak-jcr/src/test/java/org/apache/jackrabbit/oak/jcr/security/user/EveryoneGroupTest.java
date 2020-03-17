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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.security.Principal;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

/**
 * Tests for the group associated with {@code EveryonePrincipal}
 */
public class EveryoneGroupTest extends AbstractUserTest {

    private Group everyone;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        everyone = userMgr.createGroup(EveryonePrincipal.NAME);
        superuser.save();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            everyone.remove();
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    @Test
    public void testEveryoneGroup() throws RepositoryException, NotExecutableException {
        assertEquals(EveryonePrincipal.NAME, everyone.getPrincipal().getName());
    }

    @Test
    public void testPrincipal() throws RepositoryException {
        assertEquals(EveryonePrincipal.getInstance(), everyone.getPrincipal());
    }

    @Test
    public void testGroupPrincipal() throws Exception {
        Principal everyonePrincipal = everyone.getPrincipal();
        assertTrue(everyonePrincipal instanceof java.security.acl.Group);
        assertTrue(everyonePrincipal.equals(EveryonePrincipal.getInstance()));
        assertTrue(EveryonePrincipal.getInstance().equals(everyonePrincipal));

        java.security.acl.Group gr = (java.security.acl.Group) everyonePrincipal;
        assertFalse(gr.isMember(everyonePrincipal));
        assertTrue(gr.isMember(getTestUser(superuser).getPrincipal()));
        assertTrue(gr.isMember(new PrincipalImpl("test")));
    }

    @Test
    public void testMembers() throws RepositoryException, NotExecutableException {
        assertTrue(everyone.isDeclaredMember(getTestUser(superuser)));
        assertTrue(everyone.isMember(getTestUser(superuser)));

        Iterator<Authorizable> it = everyone.getDeclaredMembers();
        assertTrue(it.hasNext());
        Set<Authorizable> members = new HashSet<Authorizable>();
        while (it.hasNext()) {
            members.add(it.next());
        }

        it = everyone.getMembers();
        assertTrue(it.hasNext());
        while (it.hasNext()) {
            assertTrue(members.contains(it.next()));
        }
    }

    @Test
    public void testEditMembers() throws RepositoryException, NotExecutableException {
        assertFalse(everyone.addMember(getTestUser(superuser)));
        assertFalse(everyone.removeMember(getTestUser(superuser)));

        Group anotherGroup =  null;
        try {
            anotherGroup = userMgr.createGroup("testGroup");
            superuser.save();

            assertFalse(everyone.addMember(anotherGroup));
            assertFalse(everyone.removeMember(anotherGroup));

            assertFalse(anotherGroup.addMember(everyone));
            assertFalse(anotherGroup.removeMember(everyone));

        } finally {
            if (anotherGroup != null) {
                anotherGroup.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testUserMemberOfContainsEveryone() throws Exception {
        User testUser = getTestUser(superuser);

        boolean memberOfEveryone = false;
        Iterator<Group> groups = testUser.memberOf();
        while (groups.hasNext() && !memberOfEveryone) {
            Group g = groups.next();
            memberOfEveryone = (EveryonePrincipal.NAME.equals(g.getPrincipal().getName()));
        }

        assertTrue(memberOfEveryone);
    }
}