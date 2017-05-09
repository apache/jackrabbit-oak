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
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: User Management
 * =============================================================================
 *
 * Title: User Management Basics
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Make yourself familiar with the basic user management functionality as present
 * in Jackrabbit API
 *
 * Exercises:
 *
 * - {@link #testCreateUser()}
 *   Use this test to create a new user. Play with the parameters.
 *   Question: What are valid values for the parameters?
 *   Question: Which parameters can be 'null'?
 *   Question: What's the effect if one/some parameters are 'null'?
 *
 * - {@link #testCreateGroup()}
 *   Use to method to create a new group. Play with the parameters.
 *   Question: What are valid values for the parameters?
 *   Question: Which parameters can be 'null'?
 *   Question: What's the effect if one/some parameters are 'null'?
 *
 * - {@link #testGetAuthorizable()}
 *   Play around wit the various methods defined on {@link org.apache.jackrabbit.api.security.user.UserManager}
 *   to retrieve an existing user or group.
 *
 * </pre>
 */
public class L2_CreateAndGetTest extends AbstractJCRTest {

    private UserManager userManager;

    private User testUser;
    private Group testGroup;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        userManager = ((JackrabbitSession) superuser).getUserManager();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (testUser != null) {
                testUser.remove();
            }
            if (testGroup != null) {
                testGroup.remove();
            }
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    public void testCreateUser() throws RepositoryException {
        // EXERCISE: use the following parameters (with suitable values) to create a new user.
        // EXERCISE: play with the values. what are valid values? which params can be null? what is the effect?
        String userID = null;
        String password = null;
        Principal principal = null;
        String intermediatePath = null;

        // EXERCISE: use both methods to create a new user. what's the effect?
        testUser = userManager.createUser(userID, password, principal, intermediatePath);
        //testUser = userManager.createUser(userID password);

        superuser.save();
    }

    public void testCreateGroup() throws RepositoryException {
        // EXERCISE: use the following parameters (with suitable values) to create a new group.
        // EXERCISE: play with the values. what are valid values? which params can be null? what is the effect?
        String groupID = null;
        Principal principal = null;
        String intermediatePath = null;

        // EXERCISE: use both methods to create a new group. what's the effect?
        testGroup = userManager.createGroup(groupID, principal, intermediatePath);
//        testGroup = userManager.createGroup(groupID);
//        testGroup = userManager.createGroup(principal);
//        testGroup = userManager.createGroup(principal, intermediatePath);

        superuser.save();
    }

    public void testGetAuthorizable() throws RepositoryException {
        testUser = userManager.createUser("testUser", null, new PrincipalImpl("testPrincipal"), null);
        testGroup = userManager.createGroup("testGroup", new PrincipalImpl("testGroupPrincipal"), null);
        superuser.save();

        // EXERCISE: use all methods provided on UserManager interface to retrieve a given user/group.
        // - lookup by id
        // - lookup by path
        // - lookup by principal
        // - lookup by id + class
    }
}