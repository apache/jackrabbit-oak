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

import java.util.List;
import javax.jcr.Node;
import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: User Management
 * =============================================================================
 *
 * Title: Representation of User|Group Content in the Repository
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand how the default implementation stores user/group information.
 * After having completed this test you should be familiar with the node
 * type definitions for rep:Authorizable, rep:User and rep:Group and be
 * able to map some basic API calls to the individual properties.
 *
 * Exercises:
 *
 * - Overview
 *   Look {@code org/apache/jackrabbit/oak/plugins/nodetype/write/builtin_nodetypes.cnd}
 *   and try to identify the built in node types used to store user and group
 *   content.
 *
 *   Question: Can explain the meaning of all types?
 *   Question: Why are most item definitions protected?
 *   Question: Can you explain which child item definitions are not protected and why?
 *
 * - {@link #testUserNode()}
 *   Test case for user nodes.
 *
 * - {@link #testUserNodeType()}
 *   What is the primary type of a user node?
 *   Which mixin types are present in addition?
 *
 * - {@link #testGroupNode()}
 *   Test case for group nodes.
 *
 * - {@link #testGroupNodeType()}
 *   What is the primary type of a group node?
 *   Which mixin types are present in addition?
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Discuss why {@link #testUserNode()} doesn't include the password?
 * - Discuss why {@link #testGroupNode()} doesn't include the 'disabled' flag.
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L5_UuidTest ()}
 * - {@link L11_PasswordTest}
 * - {@link L12_PasswordExpiryTest}
 * - {@link L8_MembershipTest}
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.api.security.user.User
 * @see org.apache.jackrabbit.api.security.user.Group
 * @see org.apache.jackrabbit.oak.spi.security.user.UserConstants
 */
public class L6_AuthorizableContentTest extends AbstractJCRTest {

    private UserManager userManager;

    private User testUser;
    private Group testGroup;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        userManager = ((JackrabbitSession) superuser).getUserManager();
        testUser = ExerciseUtility.createTestUser(userManager);
        testUser.disable("no longer active");

        testGroup = ExerciseUtility.createTestGroup(userManager);

        superuser.save();
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

    private Node getAuthorizableNode(Authorizable authorizable) throws RepositoryException {
        String path = authorizable.getPath();
        return superuser.getNode(path);
    }

    public void testUserNode() throws RepositoryException {
        Node node = getAuthorizableNode(testUser);

        String idPropertyName = null; // EXERCISE
        assertEquals(testUser.getID(), node.getProperty(idPropertyName).getString());

        String principalPropertyName = null; // EXERCISE
        assertEquals(testUser.getPrincipal().getName(), node.getProperty(principalPropertyName).getString());

        String disabledPropertyName = null; // EXERCISE
        assertEquals(testUser.getDisabledReason(), node.getProperty(disabledPropertyName));
    }

    public void testUserNodeType() throws RepositoryException {
        Node node = getAuthorizableNode(testUser);

        String expectedNodeTypeName = null; // EXERCISE
        assertEquals(expectedNodeTypeName, node.getPrimaryNodeType().getName());

        List<String> mixinTypes = ImmutableList.of(); // EXERCISE : fill the list
        for (String mixin : mixinTypes) {
            assertTrue(node.isNodeType(mixin));
        }
    }

    public void testGroupNode() throws RepositoryException {
        Node node = getAuthorizableNode(testGroup);

        String idPropertyName = null; // EXERCISE
        assertEquals(testGroup.getID(), node.getProperty(idPropertyName).getString());

        String principalPropertyName = null; // EXERCISE
        assertEquals(testGroup.getPrincipal().getName(), node.getProperty(principalPropertyName).getString());

        String expectedNodeTypeName = null; // EXERCISE
        assertEquals(expectedNodeTypeName, node.getPrimaryNodeType().getName());
    }

    public void testGroupNodeType() throws RepositoryException {
        Node node = getAuthorizableNode(testGroup);

        String expectedNodeTypeName = null; // EXERCISE
        assertEquals(expectedNodeTypeName, node.getPrimaryNodeType().getName());

        List<String> mixinTypes = ImmutableList.of(); // EXERCISE : fill the list
        for (String mixin : mixinTypes) {
            assertTrue(node.isNodeType(mixin));
        }
    }
}