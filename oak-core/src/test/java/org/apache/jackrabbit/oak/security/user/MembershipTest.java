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

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Tests large group and user graphs.
 *
 * <ul>
 * <li>{@link #NUM_USERS} users</li>
 * <li>{@link #NUM_GROUPS} groups</li>
 * <li>1 group with all users</li>
 * <li>1 user with all groups</li>
  * </ul>
 *
 * @since OAK 1.0
 */
public class MembershipTest extends MembershipBaseTest {

    private static final int MANY_USERS = 1000;

    @Before
    public void before() throws Exception {
        super.before();
    }

    @Test
    public void testManyMemberships() throws Exception {
        Set<String> memberships = new HashSet<>();
        User usr = createUser();
        for (int i = 0; i < MANY_USERS; i++) {
            Group grp = createGroup();
            grp.addMember(usr);
            memberships.add(grp.getID());
        }
        root.commit();

        Iterator<Group> iter = usr.declaredMemberOf();
        while (iter.hasNext()) {
            Group group = iter.next();
            Assert.assertTrue(memberships.remove(group.getID()));
        }
        assertEquals(0, memberships.size());

        verify(monitor, times(1)).doneMemberOf(anyLong(), eq(true));
        verify(monitor, never()).doneMemberOf(anyLong(), eq(false));
    }

    @Test
    public void testNestedMembers() throws Exception {
        Set<String> members = new HashSet<>();
        Set<String> declaredMembers = new HashSet<>();
        Group grp = createGroup();
        for (int i = 0; i < 10; i++) {
            Group g1 = createGroup();
            grp.addMember(g1);
            members.add(g1.getID());
            declaredMembers.add(g1.getID());
            for (int j = 0; j < 10; j++) {
                Group g2 = createGroup();
                g1.addMember(g2);
                members.add(g2.getID());
                for (int k = 0; k < 10; k++) {
                    User usr = createUser();
                    g2.addMember(usr);
                    members.add(usr.getID());
                }
            }
        }
        root.commit();

        assertMembers(grp, members);

        Iterator<Authorizable> iter = grp.getDeclaredMembers();
        while (iter.hasNext()) {
            Authorizable member = iter.next();
            Assert.assertTrue(declaredMembers.remove(member.getID()));
        }
        assertEquals(0, declaredMembers.size());
    }

    @Test
    public void testNestedMemberships() throws Exception {
        Set<String> memberships = new HashSet<>();
        User user = createUser();
        Group grp = createGroup();
        memberships.add(grp.getID());
        for (int i = 0; i < 10; i++) {
            Group g1 = createGroup();
            grp.addMember(g1);
            memberships.add(g1.getID());
            for (int j = 0; j < 10; j++) {
                Group g2 = createGroup();
                g1.addMember(g2);
                memberships.add(g2.getID());
                g2.addMember(user);
            }
        }
        root.commit();

        Iterator<Group> iter = user.memberOf();
        while (iter.hasNext()) {
            Group group = iter.next();
            Assert.assertTrue(memberships.remove(group.getID()));
        }
        assertEquals(0, memberships.size());

        verify(monitor, times(210)).doneUpdateMembers(anyLong(), eq(1L), eq(0L), eq(false));
        verify(monitor, times(1)).doneMemberOf(anyLong(), eq(false));
        verify(monitor, never()).doneMemberOf(anyLong(), eq(true));
    }

    @Test
    public void testAddMembersAgain() throws Exception {
        Set<String> members = new HashSet<>();
        Group grp = createGroup();
        for (int i = 0; i < MANY_USERS; i++) {
            User usr = createUser();
            grp.addMember(usr);
            members.add(usr.getID());
        }
        root.commit();
        verify(monitor, times(MANY_USERS)).doneUpdateMembers(anyLong(), eq(1L), eq(0L), eq(false));

        for (String id : members) {
            assertFalse(grp.addMember(userMgr.getAuthorizable(id)));
        }
        verify(monitor, times(MANY_USERS)).doneUpdateMembers(anyLong(), eq(1L), eq(1L), eq(false));
        verifyNoMoreInteractions(monitor);
    }
}