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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.jcr.Credentials;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.exercise.security.authorization.permission.L3_PrecedenceRulesTest;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * <pre>
 * Module: User Management
 * =============================================================================
 *
 * Title: System Users
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the difference between system users and regular users and why we
 * decided to introduce a dedicated API for system users in Jackrabbit API 2.10.
 *
 * Exercises:
 *
 * - Overview
 *   Walk through the system user creation in the setup and also take a closer
 *   look at the node type definition in the builtin_nodetypes.cnd
 *
 *   Question: What can you say about the {@code rep:SystemUser} node type definition?
 *   Question: Can you explain how this is reflected in the default user management implementation?
 *   Question: How is a system user different from a regular user? Can you list all differences?
 *
 * - {@link #testSystemUser()}
 *   This test retrieves the system user created in the setup. Complete the
 *   test to become familiar with the handing of system users in the user
 *   management API.
 *
 * - {@link #testSystemUserNode()}
 *   This test illustrates some of the implementation details on how system users
 *   are being represented in the repository. Complete the test to get it pass.
 *
 *   Question: Can you explain what the 'memberOf' method will return if there
 *             exists an everyone authorizable group?
 *
 * - {@link #testSystemUserPrincipal()}
 *   Look at the principal associated with the system user created in the setup.
 *   Verify your expectations wrt principal name and type of principal and the
 *   group membership and fix the test accordingly.
 *
 *   Question: Can you elaborate about the impact of the test results when it comes to
 *             permission evaluation for system users?
 *             Use {@link L3_PrecedenceRulesTest}
 *             to verify your expectations or to get some more insight.
 *
 * - {@link #testSetPassword()}
 *   This test attempts to set a password to the system user created in the
 *   setup. Fix the test and the assertion and explain the behavior.
 *
 *   Question: How is setting passwords different for system users compared to regular users?
 *   Question: Look at the node type definition again. What can you state wrt rep:password in the effective node type?
 *   Question: Walk through the test again. Can you identify the exact location(s) for special handling?
 *
 * - {@link #testGetCredentials()}
 *   Look at the credentials object exposed by the system user and compare it
 *   with the result as return in {@link L11_PasswordTest#testGetCredentials()}
 *
 * </pre>
 *
 * @see <a href="https://issues.apache.org/jira/browse/JCR-3802">JCR-3802</a>
 */
public class L13_SystemUserTest extends AbstractSecurityTest {

    private User systemUser;
    private Group testGroup;

    @Override
    public void before() throws Exception {
        super.before();

        systemUser = getUserManager(root).createSystemUser(ExerciseUtility.getTestId("testSystemUser"), null);
        testGroup = ExerciseUtility.createTestGroup(getUserManager(root));
        testGroup.addMember(systemUser);
        root.commit();
    }

    @Override
    public void after() throws Exception {
        try {
            if (systemUser != null) {
                systemUser.remove();
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
    public void testSystemUser() throws RepositoryException {
        UserManager userMgr = getUserManager(root);
        Authorizable authorizable = userMgr.getAuthorizable(systemUser.getID());

        Boolean isGroup = null; // EXERCISE
        assertEquals(isGroup.booleanValue(), authorizable.isGroup());

        Boolean isAdmin = null; // EXERCISE
        assertEquals(isAdmin.booleanValue(), systemUser.isAdmin());

        // EXERCISE: retrieve the authorizable by class: what is the correct authorizble-class to use?
        Class cls = null;
        Authorizable sUser = userMgr.getAuthorizable(systemUser.getID(), cls);

        Iterator<Group> memberOf = sUser.memberOf();
        Set<Group> expectedGroups = null; // EXERCISE
        while (memberOf.hasNext()) {
            assertTrue(expectedGroups.remove(memberOf.next()));
        }
        assertTrue(expectedGroups.isEmpty());
    }

    @Test
    public void testSystemUserNode() throws RepositoryException {
        Tree systemUserTree = root.getTree(systemUser.getPath());
        assertTrue(systemUserTree.exists());

        String expectedPrimaryTypeName = null; // EXERCISE
        assertEquals(expectedPrimaryTypeName, TreeUtil.getPrimaryTypeName(systemUserTree));

        String expectedId = null; // EXERCISE
        assertEquals(expectedId, TreeUtil.getString(systemUserTree, UserConstants.REP_AUTHORIZABLE_ID));

        String expectedPw = null; // EXERCISE
        assertEquals(expectedPw, TreeUtil.getString(systemUserTree, UserConstants.REP_PASSWORD));
    }

    @Test
    public void testSystemUserPrincipal() throws RepositoryException {
        Authorizable authorizable = getUserManager(root).getAuthorizable(systemUser.getID());

        // EXERCISE: what is the nature of the principal of the system user? Assert your expectedation.
        Principal principal = authorizable.getPrincipal();

        PrincipalManager principalManager = getPrincipalManager(root);
        PrincipalIterator pIter = principalManager.getGroupMembership(principal);
        int expectedSize = -1; // EXERCISE
        assertEquals(expectedSize, pIter.getSize());

        List<Principal> expectedGroupPrincipals = null; // EXERCISE
        while (pIter.hasNext()) {
            Principal group = pIter.nextPrincipal();
            assertTrue(expectedGroupPrincipals.remove(group));
        }
        assertTrue(expectedGroupPrincipals.isEmpty());
    }

    @Test
    public void testSetPassword() throws RepositoryException, CommitFailedException {
        systemUser.changePassword(ExerciseUtility.TEST_PW);

        Tree systemUserTree = root.getTree(systemUser.getPath());
        String expectedPw = null; // EXERCISE
        assertEquals(expectedPw, TreeUtil.getString(systemUserTree, UserConstants.REP_PASSWORD));

        systemUserTree.setProperty(UserConstants.REP_PASSWORD, "anotherPw");
        root.commit();
    }

    @Test
    public void testGetCredentials() throws RepositoryException {
        // EXERCISE look at the Credentials object returned from the system user and compare it with the result from PasswordTest#getCredentials()

        Credentials creds = systemUser.getCredentials();

        // EXERCISE fix the expectation
        Credentials expected = null;
        assertEquals(expected, creds);
    }
}