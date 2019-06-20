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
package org.apache.jackrabbit.oak.security.user.query;

import javax.jcr.RepositoryException;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GroupPredicateTest extends AbstractSecurityTest {

    private UserManager userManager;

    private User testUser;
    private Group testMember;
    private Group testGroup;

    @Override
    public void before() throws Exception {
        super.before();

        userManager = getUserManager(root);

        testUser = getTestUser();
        testMember = userManager.createGroup("testMember");
        testMember.addMember(testUser);

        testGroup = userManager.createGroup("testGroup");
        testGroup.addMember(testMember);

        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            if (testMember != null) {
                testMember.remove();
                root.commit();
            }
            if (testGroup != null) {
                testGroup.remove();
                root.commit();
            }
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testUnknownGroupId() throws Exception {
        String id = "unknownGroupId";
        assertNull(userManager.getAuthorizable(id));

        GroupPredicate gp = new GroupPredicate(userManager, id, false);
        assertFalse(gp.apply(testUser));
        assertFalse(gp.apply(testGroup));
        assertFalse(gp.apply(null));
    }

    @Test
    public void testUserId() throws Exception {
        GroupPredicate gp = new GroupPredicate(userManager, testUser.getID(), false);
        assertFalse(gp.apply(testUser));
        assertFalse(gp.apply(testGroup));
        assertFalse(gp.apply(null));
    }

    @Test
    public void testDeclaredMembersOnly() throws Exception {
        GroupPredicate gp = new GroupPredicate(userManager, testGroup.getID(), true);
        assertTrue(gp.apply(testMember));

        assertFalse(gp.apply(testUser));
        assertFalse(gp.apply(testGroup));
        assertFalse(gp.apply(null));
    }

    @Test
    public void testInheritedMembers() throws Exception {
        GroupPredicate gp = new GroupPredicate(userManager, testGroup.getID(), false);
        assertTrue(gp.apply(testMember));
        assertTrue(gp.apply(testUser));

        assertFalse(gp.apply(testGroup));
        assertFalse(gp.apply(null));
    }

    @Test
    public void testApplyTwice() throws Exception {
        GroupPredicate gp = new GroupPredicate(userManager, testGroup.getID(), true);
        gp.apply(testMember);
        assertTrue(gp.apply(testMember));
    }

    @Test
    public void testApplyTwiceNotMember() throws Exception {
        GroupPredicate gp = new GroupPredicate(userManager, testGroup.getID(), true);
        gp.apply(testUser);
        assertFalse(gp.apply(testUser));
    }

    @Test
    public void testGetIdFails() throws Exception {
        GroupPredicate gp = new GroupPredicate(userManager, testGroup.getID(), true);

        Authorizable a = mock(Authorizable.class);
        when(a.getID()).thenThrow(new RepositoryException());
        assertFalse(gp.apply(a));
    }

    @Test
    public void testGetMemberIdFails() throws Exception {
        Authorizable member = when(mock(Authorizable.class).getID()).thenThrow(new RepositoryException()).getMock();
        Group g = when(mock(Group.class).getDeclaredMembers()).thenReturn(Iterators.singletonIterator(member)).getMock();
        when(g.isGroup()).thenReturn(true);
        UserManager uMgr = when(mock(UserManager.class).getAuthorizable("g")).thenReturn(g).getMock();
        Authorizable a = when(mock(Authorizable.class).getID()).thenReturn("a").getMock();

        GroupPredicate gp = new GroupPredicate(uMgr, "g", true);
        assertFalse(gp.apply(a));
    }
}