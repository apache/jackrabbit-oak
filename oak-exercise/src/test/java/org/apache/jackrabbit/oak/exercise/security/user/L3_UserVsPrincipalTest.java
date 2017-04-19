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

import java.security.Principal;
import java.util.Map;
import java.util.UUID;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.exercise.security.authentication.L2_AuthInfoTest;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.apache.jackrabbit.oak.exercise.ExerciseUtility.TEST_USER_HINT;
import static org.apache.jackrabbit.oak.exercise.ExerciseUtility.TEST_GROUP_HINT;
import static org.apache.jackrabbit.oak.exercise.ExerciseUtility.TEST_PRINCIPAL_HINT;
import static org.apache.jackrabbit.oak.exercise.ExerciseUtility.TEST_GROUP_PRINCIPAL_HINT;

/**
 * <pre>
 * Module: User Management | Principal Management
 * =============================================================================
 *
 * Title: User vs. Principal
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the difference between {@link org.apache.jackrabbit.api.security.user.User}
 * and {@link java.security.Principal}.
 *
 * Exercises:
 *
 * - {@link #testLookup()}
 *   Test case illustrating lookup of principals and authorizables by principal
 *   name and ID as well as authorizable lookup by principal.
 *   Fix the test-case and making you familiar with the distiction between
 *   the ID and the principal name in the first place.
 *
 * - {@link #testCreateUserWithGroupPrincipalName()}
 *   This test attempts to create a new user with a principal that is already
 *   used by a Group. Complete the test-case such that it passes.
 *
 * - {@link #testCreateWithReverse()}
 *   Test case that attempts to create a user using the principal name of another
 *   user as ID and vice versa. Complete|Fix the test and explain the expected
 *   and the actually behavior.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link #testLoginWithPrincipalName()}
 *   Test case creating SimpleCredentials from principal name + password and create a
 *   new ContentSession from these credentials. Fix the case and make the
 *   appropriate assertions.
 *
 * - {@link #testAccessControlEntryWithId()}
 *   Test case attempting to create a new access control entry for a principal
 *   based from an authorizable ID. Fix the case and make the appropriate assertions.
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L2_AuthInfoTest}
 *   For tests related to exposure of principal and ID upon successful login.
 *
 * - {@link L6_AuthorizableContentTest}
 *   for tests related to the content structure of users/groups and how the
 *   ID and the principal name are represented there.
 *
 * </pre>
 */
public class L3_UserVsPrincipalTest extends AbstractSecurityTest {

    private String testId = ExerciseUtility.getTestId(TEST_USER_HINT);
    private Principal testPrincipal = ExerciseUtility.getTestPrincipal(TEST_PRINCIPAL_HINT);

    private String testGroupId = ExerciseUtility.getTestId(TEST_GROUP_HINT);
    private Principal testGroupPrincipal = ExerciseUtility.getTestPrincipal(TEST_GROUP_PRINCIPAL_HINT);

    private User testUser;
    private Group testGroup;

    private PrincipalManager principalManager;

    @Override
    public void before() throws Exception {
        super.before();

        UserManager userMgr = getUserManager(root);
        testUser = ExerciseUtility.createTestUser(userMgr);
        testGroup = userMgr.createGroup(testGroupId, testGroupPrincipal, null);

        testGroup.addMember(testUser);
        root.commit();

        principalManager = getPrincipalManager(root);
    }

    @Override
    public void after() throws Exception {
        try {
            testUser.remove();
            testGroup.remove();
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testLookup() throws RepositoryException {
        Map<String, Object[]> resultMap = ImmutableMap.of(
                testId, new Object[]{null, null, null},
                testPrincipal.getName(), new Object[]{null, null, null},
                testGroupId, new Object[]{null, null},
                testGroupPrincipal.getName(), new Object[]{null, null}
        );

        for (String key : resultMap.keySet()) {
            Object[] result = resultMap.get(key);

            // lookup principal by "ID"
            Principal expectedP = (Principal) result[0];
            Principal principal = principalManager.getPrincipal(key);
            assertEquals(expectedP, principal);

            // lookup authorizable by "principal"
            Principal p = new PrincipalImpl(key);
            Authorizable a = getUserManager(root).getAuthorizable(p);
            if (a != null) {
                assertEquals(expectedP, a.getPrincipal());
            }

            // lookup Authorizable by "ID"
            Authorizable expectedA = (Authorizable) result[1];
            a = getUserManager(root).getAuthorizable(key);
            assertEquals(expectedA, a);

        }
    }

    @Test
    public void testCreateUserWithGroupPrincipalName() throws RepositoryException, CommitFailedException {
        // EXERCISE: fix the test-case with the correct assertions and exception catching! And explain why...
        User user2 = null;
        try {
            user2 = getUserManager(root).createUser(UUID.randomUUID().toString(), ExerciseUtility.TEST_PW, testGroupPrincipal, null);
            root.commit();
        } finally {
            if (user2 != null) {
                user2.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testCreateWithReverse() throws RepositoryException, CommitFailedException {
        // EXERCISE: fix the test-case with the correct assertions and exception catching!
        // EXERCISE: if creating the user suceeds : verify if the testUser and user2 are equal. explain why!
        User user2 = null;
        try {
            user2 = getUserManager(root).createUser(testPrincipal.getName(), ExerciseUtility.TEST_PW, new PrincipalImpl(testId), null);
            root.commit();

            Boolean expectedEquals = null; // EXERCISE
            assertEquals(expectedEquals.booleanValue(), testUser.equals(user2));

        } finally {
            if (user2 != null) {
                user2.remove();
                root.commit();
            }
        }
    }

    @Test
    public void testLoginWithPrincipalName() throws Exception {
        // EXERCISE fix the test case and add proper verification
        login(ExerciseUtility.getTestCredentials(testPrincipal.getName())).close();
    }

    @Test
    public void testAccessControlEntryWithId() throws RepositoryException {
        AccessControlManager acMgr = getAccessControlManager(root);

        // EXERCISE fix the test case
        String[] ids = new String[] {testId, testGroupId};
        for (String id : ids) {
            AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, "/");
            acl.addAccessControlEntry(new PrincipalImpl(id), AccessControlUtils.privilegesFromNames(acMgr, PrivilegeConstants.JCR_READ));
        }
    }
}