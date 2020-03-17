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
package org.apache.jackrabbit.oak.exercise.security.authorization.accesscontrol;

import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;

/**
 * <pre>
 * Module: Authorization (Access Control Management)
 * =============================================================================
 *
 * Title: Effective Policies
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Undestand the meaning and nature of retrieving the effective policies for
 * a given path or set of principals.
 *
 * Exercises:
 *
 * - {@link #testGetEffectivePolicies()}
 *   This test create policies at the test root and its child node.
 *   Fix the test such that the expected number of effective policies is correct.
 *
 * - {@link #testGetEffectivePoliciesAtNodeTypeRoot()}
 *   Implementation specific test retrieve the effective policies for the
 *   node type root node. Fix the test such that it passes.
 *
 *   Question: What is the expected result?
 *   Question: If there are effective policies, can you explain why?
 *   Question: Can you also describe the nature of the effective policies?
 *
 * - {@link #testGetEffectivePoliciesNewPolicy()}
 *   Test case illustrating the nature of the effective policies.
 *   Fix the case such that the assertion is correct.
 *
 * - {@link #testGetEffectivePoliciesByPrincipal()}
 *   Test case illustrating the usage of
 *   {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getEffectivePolicies(java.util.Set)}
 *   Fill in the expected number of effective policies and explain your expectations.
 *
 *
 * Additional Exercises
 * -----------------------------------------------------------------------------
 *
 * The following exercises use a test session with limited access rights to
 * retrieve the effective policies.
 *
 * - {@link #testSessionGetEffectivePolicies()}
 *   In this case the effective policies are retrieved with a test session that
 *   has limited access. Insert the expected number of effective policies and
 *   explain the result.
 *
 * - {@link #testSessionGetEffectivePoliciesWithoutPrivilege()}
 *   Again the test session with limited access rights is used to retrieve the
 *   effective policies. Fix the test-case and explain the results based on
 *   the implementation you can find in {@link org.apache.jackrabbit.oak.security.authorization.accesscontrol.AccessControlManagerImpl}.
 *
 * - {@link #testSessionGetEffectivePoliciesByPrincipal()}
 *   The test session with limited access is used to retrieve effective policies
 *   by principal. Fix the test case and explain the expected result.
 *
 * - {@link #testSessionGetEffectivePoliciesByPrincipalWithoutPrivileges()}
 *   The same test case again but the test session is not granted jcr:readAccessControl
 *   privilege. Complete the test-case and explain the result.
 *
 * - For these additional tests:
 *   Compare the results with what is exposed when using an admin session with
 *   full access everywhere.
 *
 *   Question: What are the implications for usage/usability of effective policies in a productive environment?
 *
 *
 * Advanced Exercise
 * -----------------------------------------------------------------------------
 *
 * The JCR specification declares the methods to retrieve effective policies as
 * 'besteffort'. Discuss the meaning of this and try to imagine implementations
 * where fullfilling this (vague) API contract might not be feasible or not
 * even be sensible.
 *
 * </pre>
 *
 * @see javax.jcr.security.AccessControlManager#getEffectivePolicies(String)
 * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlManager#getEffectivePolicies(java.util.Set)
 */
public class L4_EffectivePoliciesTest extends AbstractJCRTest {

    private String childPath;

    private JackrabbitAccessControlManager acMgr;
    private JackrabbitAccessControlList acl;

    private User testUser;
    private Principal testPrincipal;
    private Privilege[] testPrivileges;

    private Session testSession;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Node child = testRootNode.addNode(nodeName1);
        childPath = child.getPath();

        testUser = ExerciseUtility.createTestUser(((JackrabbitSession) superuser).getUserManager());
        testPrincipal = testUser.getPrincipal();
        superuser.save();

        acMgr = (JackrabbitAccessControlManager) superuser.getAccessControlManager();
        acl = AccessControlUtils.getAccessControlList(superuser, testRoot);
        if (acl == null) {
            throw new NotExecutableException();
        }

        testPrivileges = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ, Privilege.JCR_WRITE);
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (testSession != null && testSession.isLive()) {
                testSession.logout();
            }
            if (testUser != null) {
                testUser.remove();
                superuser.save();
            }
        } finally {
            super.tearDown();
        }
    }

    private JackrabbitAccessControlList setupPolicy(String path, Privilege[] privileges, Principal principal) throws RepositoryException, NotExecutableException {
        JackrabbitAccessControlList policy = AccessControlUtils.getAccessControlList(acMgr, path);
        if (policy != null) {
            policy.addEntry(principal, privileges, true);
            acMgr.setPolicy(path, policy);
        } else {
            throw new NotExecutableException();
        }
        return policy;
    }

    private Session getTestSession() throws RepositoryException {
        return superuser.getRepository().login(ExerciseUtility.getTestCredentials(testUser.getID()));
    }

    public void testGetEffectivePolicies() throws Exception {
        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(testRoot);
        int expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, policies.length);

        setupPolicy(testRoot, testPrivileges, testPrincipal);
        superuser.save();

        policies = acMgr.getEffectivePolicies(testRoot);
        expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, policies.length);

        policies = acMgr.getEffectivePolicies(childPath);
        expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, policies.length);

        setupPolicy(childPath, testPrivileges, testPrincipal);
        superuser.save();

        policies = acMgr.getEffectivePolicies(childPath);
        expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, policies.length);
    }

    public void testGetEffectivePoliciesAtNodeTypeRoot() throws Exception {
        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(NodeTypeConstants.NODE_TYPES_PATH);

        int expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, policies.length);

        // EXERCISE : if there are effective policies at this path, what type of policies to do you expect
        // EXERCISE : verify your expectation with an assertion
    }

    public void testGetEffectivePoliciesNewPolicy() throws Exception {
        setupPolicy(testRoot, testPrivileges, testPrincipal);

        // EXERCISE fix the test such that the assert below passes. explain why this is needed.

        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(testRoot);
        assertEquals(1, policies.length);
    }

    public void testGetEffectivePoliciesByPrincipal() throws Exception {

        Set<Principal> principalSet = Collections.singleton(testPrincipal);
        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(principalSet);

        int expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, policies.length);

        setupPolicy(testRoot, testPrivileges, testPrincipal);
        setupPolicy(childPath, testPrivileges, testPrincipal);

        expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, acMgr.getEffectivePolicies(principalSet).length);

        superuser.save();

        expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, acMgr.getEffectivePolicies(principalSet).length);
    }

    public void testSessionGetEffectivePolicies() throws Exception {
        // grant 'testUser' READ + WRITE privileges at the test root
        setupPolicy(testRoot, testPrivileges, testPrincipal);

        // grant 'testUser' READ + READ_AC privileges at child path
        Privilege[] privileges = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ, Privilege.JCR_READ_ACCESS_CONTROL);
        setupPolicy(childPath, privileges, testPrincipal);
        superuser.save();

        testSession = getTestSession();
        AccessControlManager testAcMgr = testSession.getAccessControlManager();

        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(childPath);
        int expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, effective.length);
    }

    public void testSessionGetEffectivePoliciesWithoutPrivilege() throws Exception {
        // grant 'testUser' READ + WRITE privileges at the test path
        setupPolicy(testRoot, testPrivileges, testPrincipal);
        superuser.save();

        testSession = getTestSession();
        AccessControlManager testAcMgr = testSession.getAccessControlManager();

        List<String> paths = ImmutableList.of(testRoot, NodeTypeConstants.NODE_TYPES_PATH);
        for (String path : paths) {
            // EXERCISE : complete or fix the test case
            AccessControlPolicy[] effectivePolicies = testAcMgr.getEffectivePolicies(path);
        }
    }

    public void testSessionGetEffectivePoliciesByPrincipal() throws Exception {
        Privilege[] privileges = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ, Privilege.JCR_READ_ACCESS_CONTROL);
        setupPolicy(testRoot, privileges, testPrincipal);
        setupPolicy(childPath, testPrivileges, EveryonePrincipal.getInstance());
        superuser.save();

        testSession = getTestSession();
        JackrabbitAccessControlManager testAcMgr = (JackrabbitAccessControlManager) testSession.getAccessControlManager();

        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(Collections.singleton(testPrincipal));
        int expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, effective.length);

        // EXERCISE : explain the result
    }

    public void testSessionGetEffectivePoliciesByPrincipalWithoutPrivileges() throws Exception {
        setupPolicy(testRoot, testPrivileges, testPrincipal);
        setupPolicy(childPath, testPrivileges, EveryonePrincipal.getInstance());
        superuser.save();

        testSession = getTestSession();
        JackrabbitAccessControlManager testAcMgr = (JackrabbitAccessControlManager) testSession.getAccessControlManager();

        AccessControlPolicy[] effective = testAcMgr.getEffectivePolicies(Collections.singleton(testPrincipal));
        int expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, effective.length);

        // EXERCISE : explain the result
    }
}