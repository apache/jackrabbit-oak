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
package org.apache.jackrabbit.oak.exercise.security.user;

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * <pre>
 * Module: User Management
 * =============================================================================
 *
 * Title: Authorizable Removal and Group Membership
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand how the removal of a given authorizable affects it's group membership.
 *
 * Exercises:
 *
 * - {@link #testMemberAfterRecreation()}
 *   Run the test and explain why after removal and re-creation the new user is
 *   automatically member of the test group.
 *
 * - {@link #testNotMemberAfterRecreation()}
 *   This is the same test as above but now the re-created test-user must not
 *   be member of 'administrators'. How do you need to do in order to make the
 *   test case pass?
 *
 *
 * Additional Exercise:
 * -----------------------------------------------------------------------------
 *
 * The following exercises can easily be performed in a Sling based repository
 * installation (e.g. Granite|CQ) with the same setup as in this test class:
 *
 * 1. Test Setup
 *
 * - Login as 'admin'
 * - Create a test-user and make it member of the 'administrators' group.
 * - Create a second user 'uadmin:uadmin' and make sure it is granted all privileges
 *   at the node of the test-user (Variant: make it member of 'user-administrators'.
 * - Verify that your changes have been persisted.
 *
 * 2. Automatic Membership Cleanup
 *
 * - Configure your system such that membership cleanup is performed upon
 *   removal of the test user.
 *   Explain what you need to do (Hint: authorizable actions).
 *
 * 3. Test Execution with 'uadmin'
 *
 * - Logout
 * - Login with the 'uadmin' user which can remove the test-user
 * - Remove the test-user and persist the changes
 * - Login as 'admin' again and test if the user has been removed; if yes, recreate
 *   it and verify and if members-list at 'administrators' has been adjust (test-user removed).
 *
 * - Explain the result both for the main and variant.
 *   > the removal failed: explain why?
 *   > the member-cleanup didn't succeed: explain why?
 *
 * 4. Test Execution with 'admin'
 *
 * - Perform the same test as 'admin' and compare the result with 3.
 * - Explain whats the difference and why it works.
 *
 *
 * Advanced Exercise:
 * -----------------------------------------------------------------------------
 *
 * - Discuss the possibilities Oak which could help to address the drawback you identified
 *   with the {@link org.apache.jackrabbit.oak.spi.security.user.action.AuthorizableAction}
 *   approach.
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L9_RemoveAuthorizableTest ()}
 *
 * </pre>
 *
 * @see Group#addMember(org.apache.jackrabbit.api.security.user.Authorizable)
 * @see Group#removeMember(org.apache.jackrabbit.api.security.user.Authorizable)
 * @see Group#isMember(org.apache.jackrabbit.api.security.user.Authorizable)
 * @see org.apache.jackrabbit.api.security.user.Authorizable#memberOf()
 * @see org.apache.jackrabbit.oak.spi.security.user.action.ClearMembershipAction
 */
public class L10_RemovalAndMembershipTest extends AbstractSecurityTest {

    private User user;
    private Group administrators;

    @Override
    public void before() throws Exception {
        super.before();

        user = getTestUser();

        administrators = getUserManager(root).createGroup("administrators");
        administrators.addMember(user);

        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            if (administrators != null) {
                administrators.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testMemberAfterRecreation() throws RepositoryException, CommitFailedException {
        // remove the user
        String id = user.getID();
        user.remove();
        root.commit();

        // create a new user with the same ID
        User u = getUserManager(root).createUser(id, ExerciseUtility.TEST_PW);
        root.commit();

        // EXERCISE: explain why this user cannot be added as member?
        assertFalse(administrators.addMember(u));

        // EXERCISE: explain why is this user is still member of the test group?
        assertTrue(administrators.isDeclaredMember(u));
    }

    @Test
    public void testNotMemberAfterRecreation() throws RepositoryException, CommitFailedException {

        // EXERCISE : adjust the test-case such that upon re-creation the user is not member of administrators.
        // HINT : do it right here

        // remove the user
        String id = user.getID();
        user.remove();
        root.commit();

        // create a new user with the same ID
        User u = getUserManager(root).createUser(id, ExerciseUtility.TEST_PW);
        root.commit();

        assertFalse(administrators.isDeclaredMember(u));
    }
}