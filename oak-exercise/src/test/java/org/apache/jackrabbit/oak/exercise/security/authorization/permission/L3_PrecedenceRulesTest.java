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
package org.apache.jackrabbit.oak.exercise.security.authorization.permission;

import java.security.Principal;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: Authorization (Permission Evaluation)
 * =============================================================================
 *
 * Title: Basic Precedence Rules in Permission Evaluation
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * The aim of this exercise is to make you familiar with some implementations
 * details of the default permission evaluation.
 *
 * Exercises:
 *
 * - Overview
 *   Read the on the Oak documentation about the default permission evaluation
 *   implementation such that the following test cases are easy to solve.
 *
 *
 * - {@link #testGroupMembership()}
 *   Test illustrating that permissions granted/denied to groups are inherited
 *   to the group members.
 *
 * - {@link #testHierarchy()}
 *   Test illustrating that in the default implemenation permissions are inherited
 *   though the item hierarchy.
 *   Create the correct permission setup to verify this.
 *
 * - {@link #testAceOrder()}
 *   This case shows how the order of ACEs within a given ACL affect the resulting
 *   permissions. Fix the test case without dropping either of the two ACEs such
 *   that the test passes and look at the ACEs present on the list before and
 *   after the fix.
 *
 *   Question: How many ways to you find to fix the test?
 *
 * - {@link #testPrecedenceOfUserPrincipals()}
 *   The goal of this test is to make you aware of the precendence of user principals
 *   during permission evaluation.
 *   Fix the test according to the instructions.
 *
 *   Question: How many ways to you find to fix the test?
 *
 * - {@link #testCombination()} and {@link #testCombination2()}
 *   Additional tests combining the different rules testes above.
 *   Fill in the correct values and explain the behaviour.
 *
 *
 * Additional Exercise
 * -----------------------------------------------------------------------------
 *
 * So far the test-cases only modify read permissions.
 *
 * - Write additional test-cases playing with different privileges
 *
 * - Once you feel comfortable with the basics include restrictions in your
 *   tests and verify your expectations.
 *
 * - Create a test setting up permission at the 'null' path and describe the
 *   result.
 *   Question: What can you say about the inheritance rules you learned so far
 *             when it comes to repository level permissions?
 *
 * HINT: there are plenty of test-cases present with oak-jcr and oak-core. Use
 *       the tests already present to invent new exercises.
 *
 *
 * </pre>
 *
 * @see <a href="http://jackrabbit.apache.org/oak/docs/security/permission/evaluation.html">Permission Evaluation in the Oak Docu</a>
 */
public class L3_PrecedenceRulesTest extends AbstractJCRTest {

    private Principal testPrincipal;
    private Principal testGroupPrincipal;
    private Session testSession;

    private String childPath;
    private String propertyPath;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Property p = testRootNode.setProperty(propertyName1, "val");
        propertyPath = p.getPath();

        Node child = testRootNode.addNode(nodeName1);
        childPath = child.getPath();

        User testUser = ExerciseUtility.createTestUser(((JackrabbitSession) superuser).getUserManager());
        Group testGroup = ExerciseUtility.createTestGroup(((JackrabbitSession) superuser).getUserManager());
        testGroup.addMember(testUser);
        superuser.save();

        testPrincipal = testUser.getPrincipal();
        testGroupPrincipal = testGroup.getPrincipal();

        AccessControlUtils.addAccessControlEntry(superuser, testRoot, EveryonePrincipal.getInstance(), AccessControlUtils.privilegesFromNames(superuser, Privilege.JCR_ALL), false);

        testSession = superuser.getRepository().login(ExerciseUtility.getTestCredentials(testUser.getID()));
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (testSession != null && testSession.isLive()) {
                testSession.logout();
            }
            UserManager uMgr = ((JackrabbitSession) superuser).getUserManager();
            Authorizable testUser = uMgr.getAuthorizable(testPrincipal);
            if (testUser != null) {
                testUser.remove();
            }
            Authorizable testGroup = uMgr.getAuthorizable(testGroupPrincipal);
            if (testGroup != null) {
                testGroup.remove();
            }
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    public void testGroupMembership() throws RepositoryException {
        assertFalse(testSession.nodeExists(testRoot));

        assertTrue(((java.security.acl.Group) testGroupPrincipal).isMember(testPrincipal));

        AccessControlUtils.addAccessControlEntry(superuser, testRoot, testGroupPrincipal, AccessControlUtils.privilegesFromNames(superuser, Privilege.JCR_READ), true);
        superuser.save();

        testSession.refresh(false);
        boolean expected = false; // EXERCISE
        assertEquals(expected, testSession.nodeExists(testRoot));
    }

    public void testHierarchy() throws RepositoryException {
        assertFalse(testSession.nodeExists(testRoot));
        assertFalse(testSession.nodeExists(childPath));
        assertFalse(testSession.propertyExists(propertyPath));

        Principal principal = testPrincipal;
        // EXERCISE : create the correct permission setup such that the test session can read all items below.
        // EXERCISE : how many entries do you need to create?
        superuser.save();

        testSession.refresh(false);
        assertTrue(testSession.nodeExists(testRoot));
        assertTrue(testSession.nodeExists(childPath));
        assertTrue(testSession.propertyExists(propertyPath));
    }

    public void testAceOrder() throws RepositoryException {
        assertFalse(testSession.nodeExists(testRoot));

        Privilege[] readPrivs = AccessControlUtils.privilegesFromNames(superuser, Privilege.JCR_READ);

        // EXERCISE: fix the permission setup such that the test success without dropping either ACE
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(superuser, testRoot);
        acl.addEntry(testGroupPrincipal, readPrivs, true);
        acl.addEntry(EveryonePrincipal.getInstance(), readPrivs, false);
        superuser.getAccessControlManager().setPolicy(acl.getPath(), acl);
        superuser.save();

        testSession.refresh(false);
        assertTrue(testSession.nodeExists(testRoot));
        assertTrue(testSession.propertyExists(propertyPath));

    }

    public void testPrecedenceOfUserPrincipals() throws RepositoryException {
        Privilege[] readPrivs = AccessControlUtils.privilegesFromNames(superuser, Privilege.JCR_READ);

        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(superuser, testRoot);
        acl.addEntry(testPrincipal, readPrivs, false);
        acl.addEntry(testGroupPrincipal, readPrivs, true);
        superuser.getAccessControlManager().setPolicy(acl.getPath(), acl);
        superuser.save();

        // EXERCISE what is the expected result?
        testSession.refresh(false);
        Boolean canRead = null; // EXERCISE
        assertEquals(canRead.booleanValue(), testSession.nodeExists(testRoot));
        assertEquals(canRead.booleanValue(), testSession.nodeExists(childPath));

        // EXERCISE: now change the permission setup such that the testSession has read access
        // EXERCISE: how many ways to you find to achieve this?
    }

    public void testCombination() throws RepositoryException {
        Privilege[] readPrivs = AccessControlUtils.privilegesFromNames(superuser, Privilege.JCR_READ);

        AccessControlUtils.addAccessControlEntry(superuser, testRoot, testPrincipal, readPrivs, false);
        AccessControlUtils.addAccessControlEntry(superuser, childPath, testGroupPrincipal, readPrivs, true);
        superuser.save();

        // EXERCISE what is the expected result?
        testSession.refresh(false);
        Boolean canRead = null; // EXERCISE
        assertEquals(canRead.booleanValue(), testSession.nodeExists(testRoot));
        assertEquals(canRead.booleanValue(), testSession.propertyExists(propertyPath));
        assertEquals(canRead.booleanValue(), testSession.nodeExists(childPath));
    }

    public void testCombination2() throws RepositoryException {
        Privilege[] readPrivs = AccessControlUtils.privilegesFromNames(superuser, Privilege.JCR_READ);

        AccessControlUtils.addAccessControlEntry(superuser, testRoot, testPrincipal, readPrivs, false);
        AccessControlUtils.addAccessControlEntry(superuser, testRoot, testPrincipal, new String[] {PrivilegeConstants.REP_READ_PROPERTIES}, true);
        AccessControlUtils.addAccessControlEntry(superuser, childPath, testGroupPrincipal, readPrivs, false);
        superuser.save();

        // EXERCISE what is the expected result?
        testSession.refresh(false);
        Boolean canRead = null; // EXERCISE
        assertEquals(canRead.booleanValue(), testSession.nodeExists(testRoot));
        canRead = null; // EXERCISE
        assertEquals(canRead.booleanValue(), testSession.propertyExists(propertyPath));
        canRead = null; // EXERCISE
        assertEquals(canRead.booleanValue(), testSession.nodeExists(childPath));
        canRead = null; // EXERCISE
        assertEquals(canRead.booleanValue(), testSession.propertyExists(childPath+"/jcr:primaryType"));
    }
}