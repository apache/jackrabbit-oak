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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * <pre>
 * Module: User Management
 * =============================================================================
 *
 * Title: Authorizable ID
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand what the ID of an authorizable stands for, how it is reflected
 * in the API. Furthermore this exercises aims to make you familiar with
 * some implementation details wrt the ID.
 *
 * Exercises:
 *
 * - {@link #getById()}
 *   Use this test again to become familiar with the authorizable lookup by ID
 *   and the nature of the authorizable ID.
 *
 * - {@link #testIdConflict()}
 *   This test illustrates how the user management implementation deals with
 *   conflicting IDs upon user/group creation.
 *   Fix the test without changing the params of the createUser/createGroup calls.
 *
 *
 * Advanced Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link #testIdWithManualCreation()}
 *   This exercise aims to create a new user manually using the Oak API (which
 *   doesn't apply the checks for protected items).
 *   Use this test to understand how authorizable uniqueness is enforced in the
 *   repository by fixing the test case
 *
 *   Question: Can you list all properties with a uniqueness constraint enforced upon?
 *   Question: Can you explain how this is enforced and what is the part that defines this uniqueness?
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L5_UuidTest}
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.api.security.user.UserManager
 * @see org.apache.jackrabbit.api.security.user.Authorizable#getID()
 */
public class L4_AuthorizableIdTest extends AbstractSecurityTest {

    private User testUser;
    private Group testGroup;

    @Override
    public void before() throws Exception {
        super.before();

        testUser = ExerciseUtility.createTestUser(getUserManager(root));
        testGroup = ExerciseUtility.createTestGroup(getUserManager(root));
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            if (testUser != null) {
                testUser.remove();
            }
            if (testGroup != null) {
                testGroup.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    @Test
    public void testGetByID() throws RepositoryException {
        // EXERCISE fix the test-case
        UserManager uMgr = getUserManager(root);
        Group g = uMgr.getAuthorizable(testUser.getID(), Group.class);

        assertEquals(g, uMgr.getAuthorizable(testGroup.getID()));
        assertEquals(g, uMgr.getAuthorizable(testGroup.getPrincipal().getName()));
    }

    @Test
    public void testIdConflict() throws RepositoryException, CommitFailedException {
        // EXERCISE: fix this test without changing the ID-parameter of the 2 create-calls.

        User conflictUser = getUserManager(root).createUser(testUser.getID(), null);
        root.refresh();

        Group conflictGroup = getUserManager(root).createGroup(testUser.getID());
        root.refresh();
    }

    @Test
    public void testIdWithManualCreation() throws RepositoryException, CommitFailedException {
        Tree userTree = root.getTree(testUser.getPath());
        Tree authorizableFolder = userTree.getParent();

        // EXERCISE: fix the test
        try {
            String id = TreeUtil.getString(userTree, UserConstants.REP_AUTHORIZABLE_ID);
            Tree anotherUser = authorizableFolder.addChild("nodeName");
            anotherUser.setProperty(userTree.getProperty(JcrConstants.JCR_PRIMARYTYPE));
            anotherUser.setProperty(userTree.getProperty(JcrConstants.JCR_UUID));
            anotherUser.setProperty(UserConstants.REP_PRINCIPAL_NAME, TreeUtil.getString(userTree, UserConstants.REP_PRINCIPAL_NAME));
            anotherUser.setProperty(UserConstants.REP_AUTHORIZABLE_ID, id);
            root.commit();

            User another = getUserManager(root).getAuthorizable(id, User.class);

            assertEquals(id, another.getID());
            assertFalse(another.equals(testUser));

        } finally {
            root.refresh();
            Tree t = authorizableFolder.getChild("nodeName");
            if (t.exists()) {
                t.remove();
                root.commit();
            }
        }
    }
}