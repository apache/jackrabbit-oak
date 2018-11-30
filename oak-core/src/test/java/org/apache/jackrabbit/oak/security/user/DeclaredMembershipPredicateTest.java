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

import javax.jcr.RepositoryException;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class DeclaredMembershipPredicateTest extends AbstractSecurityTest {

    private UserManager userManager;

    private User testUser;
    private User inheritedMember;

    private Group testMember;
    private Group testGroup;

    @Override
    public void before() throws Exception {
        super.before();

        userManager = getUserManager(root);

        testUser = getTestUser();
        inheritedMember = userManager.createUser("inheritedMember", null);

        testMember = userManager.createGroup("testMember");
        testMember.addMember(inheritedMember);

        testGroup = userManager.createGroup("testGroup");
        testGroup.addMember(testMember);
        testGroup.addMember(testUser);

        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            if (testMember != null) {
                testMember.remove();
            }
            if (inheritedMember != null) {
                inheritedMember.remove();
            }
            if (testGroup != null) {
                testGroup.remove();
            }
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    private Predicate<Authorizable> getDeclaredMembershipPredicate(@NotNull String id) {
        return new DeclaredMembershipPredicate((UserManagerImpl) userManager, id);
    }

    @Test
    public void testUnknownGroupId() throws Exception {
        String id = "unknownGroupId";
        assertNull(userManager.getAuthorizable(id));

        Predicate<Authorizable> predicate = getDeclaredMembershipPredicate(id);
        assertFalse(predicate.apply(testUser));
        assertFalse(predicate.apply(testGroup));
        assertFalse(predicate.apply(inheritedMember));
        assertFalse(predicate.apply(null));
    }

    @Test
    public void testUserId() throws Exception {
        Predicate<Authorizable> predicate = getDeclaredMembershipPredicate(testUser.getID());
        assertFalse(predicate.apply(testUser));
        assertFalse(predicate.apply(testGroup));
        assertFalse(predicate.apply(inheritedMember));
        assertFalse(predicate.apply(null));
    }

    @Test
    public void testMembers() throws Exception {
        Predicate<Authorizable> predicate = getDeclaredMembershipPredicate(testGroup.getID());
        assertTrue(predicate.apply(testMember));
        assertTrue(predicate.apply(testUser));

        assertFalse(predicate.apply(testGroup));
        assertFalse(predicate.apply(inheritedMember));
        assertFalse(predicate.apply(null));
    }

    @Test
    public void testApplyTwice() throws Exception {
        Predicate<Authorizable> predicate = getDeclaredMembershipPredicate(testGroup.getID());
        predicate.apply(testMember);
        assertTrue(predicate.apply(testMember));
    }

    @Test
    public void testApplyTwiceNotMember() throws Exception {
        Predicate<Authorizable> predicate = getDeclaredMembershipPredicate(testGroup.getID());
        predicate.apply(inheritedMember);
        assertFalse(predicate.apply(inheritedMember));
    }

    @Test
    public void testGetIdFails() throws Exception {
        Predicate<Authorizable> predicate = getDeclaredMembershipPredicate(testGroup.getID());

        Authorizable a = Mockito.mock(Authorizable.class);
        when(a.getID()).thenThrow(new RepositoryException());
        assertFalse(predicate.apply(a));
    }
}