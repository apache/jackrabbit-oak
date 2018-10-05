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
package org.apache.jackrabbit.oak.security.user;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Testing special behavior of the everyone group.
 *
 * @since OAK 1.0
 */
public class EveryoneGroupTest extends AbstractSecurityTest {

    private Group everyoneGroup;
    private Set<Authorizable> authorizables;

    @Override
    public void before() throws Exception {
        super.before();

        UserManager userMgr = getUserManager(root);
        everyoneGroup = userMgr.createGroup(EveryonePrincipal.getInstance());

        authorizables = new HashSet<Authorizable>(2);
        authorizables.add(userMgr.createGroup("testGroup"));
        authorizables.add(userMgr.createUser("testUser", "pw"));
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            if (everyoneGroup != null) {
                everyoneGroup.remove();
            }
            for (Authorizable a : authorizables) {
                a.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testGetPrincipal() throws Exception {
        assertEquals(EveryonePrincipal.getInstance(), everyoneGroup.getPrincipal());
        assertEquals(EveryonePrincipal.NAME, everyoneGroup.getPrincipal().getName());
        assertTrue(everyoneGroup.getPrincipal() instanceof ItemBasedPrincipal);
    }

    @Test
    public void testEveryoneIsMember() throws Exception {
        assertFalse(everyoneGroup.isMember(everyoneGroup));
    }

    @Test
    public void testEveryoneIsDeclaredMember() throws Exception {
        assertFalse(everyoneGroup.isDeclaredMember(everyoneGroup));
    }

    @Test
    public void testIsMember() throws Exception {
        for (Authorizable a : authorizables) {
            assertTrue(everyoneGroup.isMember(a));
        }
    }

    @Test
    public void testIsDeclaredMember() throws Exception {
        for (Authorizable a : authorizables) {
            assertTrue(everyoneGroup.isDeclaredMember(a));
        }
    }

    @Test
    public void testGetMembers() throws Exception {
        Set<Authorizable> members = ImmutableSet.copyOf(everyoneGroup.getMembers());

        assertFalse(members.contains(everyoneGroup));
        for (Authorizable a : authorizables) {
            assertTrue(members.contains(a));
        }
    }

    @Test
    public void testGetDeclaredMembers() throws Exception {
        Set<Authorizable> members = ImmutableSet.copyOf(everyoneGroup.getDeclaredMembers());

        assertFalse(members.contains(everyoneGroup));
        for (Authorizable a : authorizables) {
            assertTrue(members.contains(a));
        }
    }

    @Test
    public void testAddEveryoneAsMember() throws Exception {
        assertFalse(everyoneGroup.addMember(everyoneGroup));
    }

    @Test
    public void testAddMember() throws Exception {
        for (Authorizable a : authorizables) {
            assertFalse(everyoneGroup.addMember(a));
        }
    }

    @Test
    public void testAddMembers() throws Exception {
        assertEquals(Sets.newHashSet(getTestUser().getID()), everyoneGroup.addMembers(getTestUser().getID()));
    }

    @Test
    public void testRemoveEveryoneFromMembers() throws Exception {
        assertFalse(everyoneGroup.removeMember(everyoneGroup));
    }

    @Test
    public void testRemoveMember() throws Exception {
        for (Authorizable a : authorizables) {
            assertFalse(everyoneGroup.removeMember(a));
        }
    }

    @Test
    public void testRemoveMembers() throws Exception {
        assertEquals(Sets.newHashSet(getTestUser().getID()), everyoneGroup.removeMembers(getTestUser().getID()));
    }

    @Test
    public void testEveryoneMemberOf() throws Exception {
        Iterator<Group> groups = everyoneGroup.memberOf();
        assertFalse(groups.hasNext());
    }

    @Test
    public void testEveryoneDeclaredMemberOf() throws Exception {
        Iterator<Group> groups = everyoneGroup.declaredMemberOf();
        assertFalse(groups.hasNext());
    }

    @Test
    public void testMemberOfIncludesEveryone() throws Exception {
        for (Authorizable a : authorizables) {
            Set<Group> groups = ImmutableSet.copyOf(a.memberOf());
            assertTrue(groups.contains(everyoneGroup));
        }
    }

    @Test
    public void testDeclaredMemberOfIncludesEveryone() throws Exception {
        for (Authorizable a : authorizables) {
            Set<Group> groups = ImmutableSet.copyOf(a.declaredMemberOf());
            assertTrue(groups.contains(everyoneGroup));
        }
    }
}