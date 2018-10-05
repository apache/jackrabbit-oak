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

import java.util.Iterator;
import java.util.List;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

import static org.junit.Assert.assertArrayEquals;

/**
 * <pre>
 * Module: User Management
 * =============================================================================
 *
 * Title: Authorizable Properties
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * The aim of this exercise is to be aware of the API calls to set arbitrary
 * (non-protected) properties on authorizables.
 *
 * Exercises:
 *
 * - Overview
 *   List all methods defined on {@link org.apache.jackrabbit.api.security.user.Authorizable}
 *   that allow to read and write arbitrary properties on a given user or group.
 *
 * - {@link #testArbitraryProperties()}
 *   Become familiar with the API to read arbitrary properties with an user
 *   or group. Also fill in a list of properties defined by JCR and the
 *   authorizable node types that cannot be obtained using the methods define
 *   on {@link org.apache.jackrabbit.api.security.user.Authorizable}.
 *
 * - {@link #testSpecialProperties()}
 *   This tests uses the user properties API to retrieve protected JCR and
 *   user management internal properties.
 *   Fix the tests according to your expectations and your understanding of
 *   the API contract and the implementation.
 * </pre>
 *
 * @see org.apache.jackrabbit.api.security.user.Authorizable
 */
public class L7_AuthorizablePropertiesTest extends AbstractJCRTest {

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
        testGroup.addMember(testUser);
        testGroup.setProperty("Name", superuser.getValueFactory().createValue("Test Group"));

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

    public void testArbitraryProperties() throws RepositoryException {
        // set 2 different properties (single and multivalued)
        testUser.setProperty(EMAIL_REL_PATH, superuser.getValueFactory().createValue("testUser@oak.apache.org"));
        testUser.setProperty(PETS_REL_PATH, new Value[]{
                superuser.getValueFactory().createValue("cat"), superuser.getValueFactory().createValue("rabbit")
        });
        superuser.save();


        Node node = getAuthorizableNode(testUser);

        // EXERCISE: build the list of existing user properties rel paths
        List<String> userPropertiesPath = ImmutableList.of();
        for (String relPath : userPropertiesPath) {
            assertTrue(testUser.hasProperty(relPath));
        }

        Value[] emailsExpected = testUser.getProperty(EMAIL_REL_PATH);
        String expectedRelPath = null; // EXERCISE
        assertEquals(emailsExpected[0], node.getProperty(expectedRelPath).getValue());

        Value[] petsExpected = testUser.getProperty(PETS_REL_PATH);
        expectedRelPath = null; // EXERCISE
        assertArrayEquals(petsExpected, node.getProperty(expectedRelPath).getValues());

        // EXERCISE: build a list of protected JCR properties that cannot be accessed using the Authorizable interface
        List<String> protectedJcrPropertyNames = ImmutableList.of();
        for (String relPath : protectedJcrPropertyNames) {
            assertFalse(testUser.hasProperty(relPath));
        }

        // EXERCISE: build a list of protected properties defined by the authorizable or user node type definitions that cannot be accessed using the Authorizable interface
        List<String> protectedAuthorizablePropertyNames = ImmutableList.of();
        for (String relPath : protectedAuthorizablePropertyNames) {
            assertFalse(testUser.hasProperty(relPath));
        }

        // remove the properties again
        testUser.removeProperty(EMAIL_REL_PATH);
        testUser.removeProperty(PETS_REL_PATH);
        superuser.save();
    }

    public void testSpecialProperties() throws RepositoryException {
        List<String> expectedGroupPropNames = null; // EXERCISE
        Iterator<String> propNames = testGroup.getPropertyNames();

        while (propNames.hasNext()) {
            assertTrue(expectedGroupPropNames.remove(propNames.next()));
        }
        assertTrue(expectedGroupPropNames.isEmpty());

        Boolean hasPrimaryType = null; // EXERCISE
        assertEquals(hasPrimaryType.booleanValue(), testGroup.hasProperty(JcrConstants.JCR_PRIMARYTYPE));

        Value[] expectedMembers = null; // EXERCISE
        Value[] members = testGroup.getProperty(UserConstants.REP_MEMBERS);
    }
}