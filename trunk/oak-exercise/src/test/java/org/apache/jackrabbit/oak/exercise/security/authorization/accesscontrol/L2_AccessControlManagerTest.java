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
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: Authorization (Access Control Management)
 * =============================================================================
 *
 * Title: Access Control Manager and Policies
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Learn about the details of the {@link javax.jcr.security.AccessControlManager}
 * and the differences to {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager}.
 * Become familiar with getting, modifying and applying access control policies.
 * Finally you will be able to make use of the access control related utilities
 * as provided by the jackrabbit-jcr-commons package.
 *
 * Exercises:
 *
 * - {@link #testGetAccessControlManager()}
 *   Complete the test by retrieving the regular JCR and the Jackrabbit access
 *   control manager.
 *
 *   Question: What kind of test could/should you perform in order not to risk any runtime exceptions
 *   Question: Does JCR API provide means to avoid {@link javax.jcr.UnsupportedRepositoryOperationException}?
 *
 * - {@link #testRetrievePoliciesAtTestRoot()}
 *   Understand the difference between {@code getApplicablePolicies} and {@code getPolicies}.
 *   Fix the test such that it passes.
 *
 * - {@link #testRetrievePoliciesAtNamespaceRoot()}
 *   Same as before for {@link NamespaceConstants#NAMESPACES_PATH}.
 *
 *   Question: What are the differences?
 *   Question: Can you explain it?
 *   Question: What would you need to do in order to get the same result for the test root?
 *
 * - {@link #testModifyPolicy()}
 *   This test modifies the access control list exposed by the default implementation at the test root.
 *   Fix the test such that it passes.
 *
 *   Question: What is the expected return value of {@code getApplicablePolicies} and {@code getPolicies}
 *             after modifying the ACL?
 *   Question: What is the nature of the ACE created by the test case? Deny? Allow? Why?
 *
 * - {@link #testAddAceWithUtility()}
 *   Similar to {@link #testModifyPolicy()} this test modifies the policy at the
 *   test root however using the one variant of the utility methods provided by
 *   jackrabbit-jcr-commons. Fix the test such that it passes.
 *
 *   Question: Can you summarize the difference to the test above?
 *   Question: Can you explain the behavior?
 *
 * - {@link #testSetPolicy()}
 *   Same as {@link #testModifyPolicy()}. Fix the test case such that it passes.
 *   Hint: The title of the test indicates the expected fix :-)
 *
 * - {@link #testRemovePolicy()}
 *   Test illustrating how to remove access control policies. Explain why the
 *   initial call to remove the policy fails.
 *
 * - {@link #testTransientNature()}
 *   Test case illustrating the transient nature of access control modifications.
 *   Fix the test case such that it passes.
 *
 * - {@link #testRetrievePoliciesAsReadOnlySession()}
 *   This case illustrates the fact the reading access control management requires
 *   additional permissions. Explaing why the read-only session cannot call
 *   either of the methods to retrieve policies at the test root and fix the
 *   test case accordingly.
 *
 *   Question: How many variants to do see to fix the test?
 *
 * - {@link #testWritePoliciesAsReadOnlySession()}
 *   This case illustrates the fact the writing access control content requires
 *   additional permissions. Explaing why the read-only session cannot set the
 *   policy at the test root and fix the test case accordingly.
 *
 *   Question: How many variants to do see to fix the test?
 *
 *
 * Additional Exercises
 * -----------------------------------------------------------------------------
 *
 * While the default implementation currently mostly exposes {@link AccessControlList}
 * policies, the specification defines the nature of access control policies an
 * implementation detail.
 *
 * - {@link #testPoliciesAtNullPath()}
 *   All test above use a regular, absolute path pointing to a Node to read and
 *   write access control policies.
 *   As of JSR 283 it is also allowed to use 'null' instead. Use this exercise
 *   to recap the meaning 'null' in this context by looking at the JCR Javadoc.
 *   Fix the test by providing the correct set of privileges that can be granted here.
 *
 *   Question: What privileges can only be granted/revoked at the 'null' path?
 *   Question: Can you extend the test to verify your expectations?
 *
 * - Look for other implementations of the {@link javax.jcr.security.AccessControlPolicy}
 *   interface in Oak and list your findings.
 *
 *   Question: Can you find other policies?
 *   Question: Can you describe the nature of these policies?
 *   Question: Can you identify the impact this may have on API consumers that make assumptions about the type of policies?
 *
 * - Take a second look at the {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlManager}.
 *
 *   Question: What additional methods exist with respect to retrieving access control policies?
 *   Question: Can you explain how to use these methods?
 *   Question: Explore how the default implementation handles these calls.
 *
 *
 * Related Exercises
 * -----------------------------------------------------------------------------
 *
 * - {@link L3_AccessControlListTest}
 * - {@link L4_EffectivePoliciesTest}
 * - {@link L7_RestrictionsTest}
 *
 * </pre>
 *
 * @see javax.jcr.security.AccessControlManager
 * @see javax.jcr.security.AccessControlPolicy
 */
public class L2_AccessControlManagerTest extends AbstractJCRTest {

    private AccessControlManager acMgr;
    private Principal testPrincipal;
    private String testID;

    private Session testSession;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        acMgr = superuser.getAccessControlManager();

        User testUser = ExerciseUtility.createTestUser(((JackrabbitSession) superuser).getUserManager());
        testPrincipal = testUser.getPrincipal();
        testID = testUser.getID();
        superuser.save();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (testSession != null && testSession.isLive()) {
                testSession.logout();
            }

            Authorizable testUser = ((JackrabbitSession) superuser).getUserManager().getAuthorizable(testPrincipal);
            if (testUser != null) {
                testUser.remove();
                superuser.save();
            }
        } finally {
            super.tearDown();
        }
    }

    public void testGetAccessControlManager() throws RepositoryException {

        // EXERCISE retrieve the access control manager using standard JCR API
        AccessControlManager acMgr = null;
        assertNotNull(acMgr);

        // EXERCISE retrieve the jackrabbit access control manager using standard API, without risking a class-cast exception.
        JackrabbitAccessControlManager jackrabbitAcMgr = null;
        assertNotNull(jackrabbitAcMgr);
    }

    public void testRetrievePoliciesAtTestRoot() throws RepositoryException {
        AccessControlPolicy[] policies = acMgr.getPolicies(testRoot);
        int expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, policies.length);


        AccessControlPolicyIterator policyIterator = acMgr.getApplicablePolicies(testRoot);
        int expectedSize = -1; // EXERCISE
        assertEquals(expectedSize, policyIterator.getSize());

        // EXERCISE: look at the utility methods and explain the expected return value
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testRoot);
        JackrabbitAccessControlList acl2 = AccessControlUtils.getAccessControlList(superuser, testRoot);
        assertEquals(acl, acl2); // EXERCISE: is this correct?
    }

    public void testRetrievePoliciesAtNamespaceRoot() throws RepositoryException {
        AccessControlPolicy[] policies = acMgr.getPolicies(NamespaceConstants.NAMESPACES_PATH);
        int expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, policies.length);


        AccessControlPolicyIterator policyIterator = acMgr.getApplicablePolicies(NamespaceConstants.NAMESPACES_PATH);
        int expectedSize = -1; // EXERCISE
        assertEquals(expectedSize, policyIterator.getSize());

        // EXERCISE: look at the utility methods and explain the expected return value
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, NamespaceConstants.NAMESPACES_PATH);
        JackrabbitAccessControlList acl2 = AccessControlUtils.getAccessControlList(superuser, NamespaceConstants.NAMESPACES_PATH);
        assertEquals(acl, acl2); // EXERCISE: is this correct?
    }

    public void testModifyPolicy() throws RepositoryException {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testRoot);

        assertNotNull(acl);
        assertEquals(0, acl.getAccessControlEntries().length);

        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), new Privilege[] {acMgr.privilegeFromName(Privilege.JCR_READ)});
        int expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, acl.getAccessControlEntries().length);

        int expectedSize = -1; // EXERCISE
        assertEquals(expectedSize, acMgr.getApplicablePolicies(testRoot).getSize());

        expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, acMgr.getPolicies(testRoot).length);
    }

    public void testAddAceWithUtility() throws RepositoryException {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testRoot);

        boolean success = AccessControlUtils.addAccessControlEntry(superuser, testRoot, testPrincipal, new String[] {Privilege.JCR_READ}, false);
        boolean expectedSuccess = false; // EXERCISE
        assertEquals(expectedSuccess, success);

        int expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, acl.getAccessControlEntries().length);

        int expectedSize = -1; // EXERCISE
        assertEquals(expectedSize, acMgr.getApplicablePolicies(testRoot).getSize());

        AccessControlPolicy[] policies = acMgr.getPolicies(testRoot);
        expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, policies.length);

        acl = null;
        for (AccessControlPolicy policy : policies) {
            if (policy instanceof JackrabbitAccessControlList) {
                acl = (JackrabbitAccessControlList) policy;
            }
        }
        JackrabbitAccessControlList acl2 = AccessControlUtils.getAccessControlList(acMgr, testRoot);

        // EXERCISE: is the following expected to succeed?
        assertEquals(acl, acl2);

        int expectedAceLength = -1; // EXERCISE
        assertEquals(expectedAceLength, acl.getAccessControlEntries().length);
    }

    public void testSetPolicy() throws RepositoryException {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testRoot);

        assertTrue(acl.addEntry(testPrincipal, AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ), false));

        // EXERCISE: fix the test.

        assertFalse(acMgr.getApplicablePolicies(testRoot).hasNext());

        AccessControlPolicy[] policies = acMgr.getPolicies(testRoot);
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof JackrabbitAccessControlList);

        JackrabbitAccessControlList acl2 = (JackrabbitAccessControlList) policies[0];

        assertFalse(acl2.isEmpty());
        assertEquals(1, acl2.size());
        AccessControlEntry ace = acl2.getAccessControlEntries()[0];

        assertTrue(ace instanceof JackrabbitAccessControlEntry);
        assertEquals(testPrincipal, ace.getPrincipal());
        assertFalse(((JackrabbitAccessControlEntry) ace).isAllow());
    }

    public void testRemovePolicy() throws RepositoryException {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testRoot);

        // EXERCISE: explain why
        try {
            acMgr.removePolicy(testRoot, acl);
            fail("EXERCISE");
        } catch (AccessControlException e) {
            // success
        }

        AccessControlUtils.addAccessControlEntry(superuser, testRoot, testPrincipal, new String[] {Privilege.JCR_READ}, false);

        acl = AccessControlUtils.getAccessControlList(acMgr, testRoot);
        acMgr.removePolicy(testRoot, acl);
    }

    public void testTransientNature() throws RepositoryException {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testRoot);
        assertTrue(acl.addEntry(testPrincipal, AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ), false));
        acMgr.setPolicy(testRoot, acl);

        assertEquals(acl, AccessControlUtils.getAccessControlList(acMgr, testRoot));

        // EXERCISE: fix the test case

        Session s = getHelper().getSuperuserSession();
        try {
            AccessControlPolicy[] policies = s.getAccessControlManager().getPolicies(testRoot);
            assertEquals(1, policies.length);
            assertEquals(acl, policies[0]);
        } finally {
            if (s.isLive()) {
                s.logout();
            }
        }
    }

    public void testRetrievePoliciesAsReadOnlySession() throws RepositoryException {
        testSession = superuser.getRepository().login(ExerciseUtility.getTestCredentials(testID));

        // EXERCISE: Fix the test and explain your fix.
        AccessControlPolicyIterator policyIterator = testSession.getAccessControlManager().getApplicablePolicies(testRoot);
        AccessControlPolicy[] policies = testSession.getAccessControlManager().getPolicies(testRoot);
    }

    public void testWritePoliciesAsReadOnlySession() throws RepositoryException {
        testSession = superuser.getRepository().login(ExerciseUtility.getTestCredentials(testID));

        // EXERCISE: Fix the test and explain your fix.
        // NOTE: that obviously is prone to cause troubles as the policies is retrieved with a different session!
        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testRoot);

        testSession.getAccessControlManager().setPolicy(testRoot, acl);
    }

    public void testPoliciesAtNullPath() throws RepositoryException {
        String testPath = null;
        Privilege[] privileges = null; // EXERCISE define the set of privs that can/must be granted at the 'null' path.

        AccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addAccessControlEntry(testPrincipal, privileges);
        acMgr.setPolicy(testPath, acl);

        superuser.save();

        // EXERCISE explain (or even verify) the expected result
    }
}