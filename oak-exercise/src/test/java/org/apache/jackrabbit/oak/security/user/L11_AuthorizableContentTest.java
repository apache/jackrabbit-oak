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

import java.util.List;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.security.ExerciseUtility;
import org.apache.jackrabbit.test.AbstractJCRTest;

import static org.junit.Assert.assertArrayEquals;

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
 * - {@link #testUserProperties()}
 *   Become familiar with the API to read arbitrary properties with an user
 *   or group. Also fill in a list of properties defined by JCR and the
 *   authorizable node types that cannot be obtained using the methods define
 *   on {@link org.apache.jackrabbit.api.security.user.Authorizable}.
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
 * - {@link L12_UuidTest ()}
 * - {@link L4_PasswordTest}
 * - {@link L10_PasswordExpiryTest}
 * - {@link L5_MembershipTest}
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.api.security.user.User
 * @see org.apache.jackrabbit.api.security.user.Group
 * @see org.apache.jackrabbit.oak.spi.security.user.UserConstants
 */
public class L11_AuthorizableContentTest extends AbstractJCRTest {

    private final static String EMAIL_REL_PATH = "properties/email";
    private final static String PETS_REL_PATH = "pets";

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

        String idPropertyName = null; // TODO
        assertEquals(testUser.getID(), node.getProperty(idPropertyName).getString());

        String principalPropertyName = null; // TODO
        assertEquals(testUser.getPrincipal().getName(), node.getProperty(principalPropertyName).getString());

        String disabledPropertyName = null; // TODO
        assertEquals(testUser.getDisabledReason(), node.getProperty(disabledPropertyName));
    }

    public void testUserNodeType() throws RepositoryException {
        Node node = getAuthorizableNode(testUser);

        String expectedNodeTypeName = null; // TODO
        assertEquals(expectedNodeTypeName, node.getPrimaryNodeType().getName());

        List<String> mixinTypes = ImmutableList.of(); // TODO : fill the list
        for (String mixin : mixinTypes) {
            assertTrue(node.isNodeType(mixin));
        }
    }

    public void testGroupNode() throws RepositoryException {
        Node node = getAuthorizableNode(testGroup);

        String idPropertyName = null; // TODO
        assertEquals(testGroup.getID(), node.getProperty(idPropertyName).getString());

        String principalPropertyName = null; // TODO
        assertEquals(testGroup.getPrincipal().getName(), node.getProperty(principalPropertyName).getString());

        String emailPropertyPath = null; // TODO
        assertEquals(testUser.getProperty(EMAIL_REL_PATH)[0], node.getProperty(emailPropertyPath).getValue());

        String petsPropertyPath = null; // TODO
        assertEquals(testUser.getProperty(PETS_REL_PATH), node.getProperty(petsPropertyPath).getValues());

        String expectedNodeTypeName = null; // TODO
        assertEquals(expectedNodeTypeName, node.getPrimaryNodeType().getName());
    }

    public void testGroupNodeType() throws RepositoryException {
        Node node = getAuthorizableNode(testGroup);

        String expectedNodeTypeName = null; // TODO
        assertEquals(expectedNodeTypeName, node.getPrimaryNodeType().getName());

        List<String> mixinTypes = ImmutableList.of(); // TODO : fill the list
        for (String mixin : mixinTypes) {
            assertTrue(node.isNodeType(mixin));
        }
    }

    public void testUserProperties() throws RepositoryException {
        // set 2 different properties (single and multivalued)
        testUser.setProperty(EMAIL_REL_PATH, superuser.getValueFactory().createValue("testUser@oak.apache.org"));
        testUser.setProperty(PETS_REL_PATH, new Value[]{
                superuser.getValueFactory().createValue("cat"), superuser.getValueFactory().createValue("rabbit")
        });
        superuser.save();


        Node node = getAuthorizableNode(testUser);

        // TODO: build the list of existing user properties rel paths
        List<String> userPropertiesPath = ImmutableList.of();
        for (String relPath : userPropertiesPath) {
            assertTrue(testUser.hasProperty(relPath));
        }

        Value[] emailsExpected = testUser.getProperty(EMAIL_REL_PATH);
        String expectedRelPath = null; // TODO
        assertEquals(emailsExpected[0], node.getProperty(expectedRelPath).getValue());

        Value[] petsExpected = testUser.getProperty(PETS_REL_PATH);
        expectedRelPath = null; // TODO
        assertArrayEquals(petsExpected, node.getProperty(expectedRelPath).getValues());

        // TODO: build a list of protected JCR properties that cannot be accessed using the Authorizable interface
        List<String> protectedJcrPropertyNames = ImmutableList.of();
        for (String relPath : protectedJcrPropertyNames) {
            assertFalse(testUser.hasProperty(relPath));
        }

        // TODO: build a list of protected properties defined by the authorizable or user node type definitions that cannot be accessed using the Authorizable interface
        List<String> protectedAuthorizablePropertyNames = ImmutableList.of();
        for (String relPath : protectedAuthorizablePropertyNames) {
            assertFalse(testUser.hasProperty(relPath));
        }

        // remove the properties again
        testUser.removeProperty(EMAIL_REL_PATH);
        testUser.removeProperty(PETS_REL_PATH);
        superuser.save();
    }
}