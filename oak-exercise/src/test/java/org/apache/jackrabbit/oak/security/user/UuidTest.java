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

import javax.jcr.Node;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 * Module: User Management
 * =============================================================================
 *
 * Title: Authorizable Uuid
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the implementation specific content structure for users and groups.
 * This particular exercise aims to illustrate the role of jcr:uuid.
 *
 * Exercises:
 *
 * - Authorizable Node Type Definition:
 *   Look at the node type definition of {@code rep:Authorizable} in {@code builtin_nodetypes.cnd}
 *   and implementation and answer the following questions:
 *
 *   - Why does a group or user node have a jcr:uuid property?
 *   - What are the constraints JCR mandates for jcr:uuid? Also recap what JCR
 *     states wrt {@link javax.jcr.Item#isSame(javax.jcr.Item)}. What are the
 *     implications for the authorizable implementation as it is today?
 *   - How is the jcr:uuid set in this default implementation?
 *   - What is the jcr:uuid use for in this default implementation?
 *   Answers: TODO
 *
 * - {@link #testIdAndUuidAndIdentifier()}
 *   Use the answers provided above to fix the test. The goal is that you learn
 *   to understand the difference between the ID as exposed by the user management
 *   API and the internal JCR specific node identifiers.
 *
 * - {@link #testUuidUponCreation()}
 *   Based on the answers above you should be able to fix the test case.
 *
 * </pre>
 */
public class UuidTest extends AbstractJCRTest {

    private static final String USER_ID = "testUser";

    private UserManager userManager;
    private User testUser;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        userManager = ((JackrabbitSession) superuser).getUserManager();
        testUser = userManager.createUser(USER_ID, "pw");
        superuser.save();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            testUser.remove();
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    private Node getUserNode(User user) throws RepositoryException {
        String userPath = user.getPath();
        return superuser.getNode(userPath);
    }

    public void testIdAndUuidAndIdentifier() throws RepositoryException {
        Node userNode = getUserNode(testUser);

        assertTrue(userNode.isNodeType(JcrConstants.MIX_REFERENCEABLE));

        String identifier = userNode.getIdentifier();
        String uuid = userNode.getUUID();
        String authorizableId = userNode.getProperty(UserConstants.REP_AUTHORIZABLE_ID).getString();

        // TODO: explain why identifier and uuid are expected to be the equal
        assertEquals(identifier, uuid);

        // TODO: explain why neither uuid nor identifier are expected to be equal to the rep:authoriableId property
        assertFalse(identifier.equals(authorizableId));
        assertFalse(uuid.equals(authorizableId));

        String userId = testUser.getID();
        String expectedUserId = null; // TODO: what is the expected userID ?

        assertEquals(expectedUserId, userId);

        // TODO: what id do you have to use for the lookup on the user manager?
        String idForLookup = null;
        User user = userManager.getAuthorizable(idForLookup, User.class);
    }

    public void testUuidUponCreation() throws RepositoryException {
        Node userNode = getUserNode(testUser);
        String uuid = userNode.getUUID();

        // remove the test user
        testUser.remove();
        superuser.save();

        // recreate the same user again
        testUser = userManager.createUser(USER_ID, "pw");

        // TODO: fill the expected identifier.
        // Q: can you predict the expected identifier?
        // Q: if yes, why?
        String expectedUuid = null;
        assertEquals(expectedUuid, getUserNode(testUser).getUUID());
    }
}