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
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.exercise.security.authorization.permission.L4_PrivilegesAndPermissionsTest;
import org.apache.jackrabbit.oak.exercise.security.privilege.L3_BuiltInPrivilegesTest;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;

/**
 * <pre>
 * Module: Authorization (Access Control Management)
 * =============================================================================
 *
 * Title: Access Control List in Detail
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Learn to use additional ways to modify an JCR or Jackrabbit ACL.
 *
 * Exercises:
 *
 * - {@link #testEmptyList()}
 *   Get familiar with the concept of an empty ACL and what methods the Jackrabbit
 *   API extensions provides. Fix the test case.
 *
 *   Question: Can you set an empty ACL?
 *   Question: If that works, what is the nature of the ACL if you retrieve it using AccessControlManager#getPolicies()?
 *
 * - {@link #testAddEntries()}
 *   Use this test to play with the different ways of adding one (or multiple) ACE
 *   to the list. Verify your expectations and keep an eye on the size of the ACL.
 *
 * - {@link #testRemoveEntry()}
 *   Remove one ACE that has been added to the policy before and verify the result.
 *
 * - {@link #testReorderEntries()}
 *   Reorder the ACEs created for this test according to the instructions in the
 *   test. Make sure the test passes.
 *
 * - {@link #testGetPath()}
 *   Test illustrating the {@link org.apache.jackrabbit.api.security.JackrabbitAccessControlList#getPath()}.
 *   Fill in the expected path and explain the meaning of the path
 *
 *   Question: Can you use the path exposed by the ACL to set the policy? If not fix the test accordingly
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L5_AccessControlListImplTest}
 * - {@link L7_RestrictionsTest}
 * - {@link L3_BuiltInPrivilegesTest}
 * - {@link L4_PrivilegesAndPermissionsTest}
 *
 *
 * </pre>
 *
 * @see javax.jcr.security.AccessControlList
 * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlList
 * @see javax.jcr.security.AccessControlEntry
 * @see org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry
 */
public class L3_AccessControlListTest extends AbstractJCRTest {

    private AccessControlManager acMgr;
    private JackrabbitAccessControlList acl;
    private Principal testPrincipal;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        acMgr = superuser.getAccessControlManager();

        testPrincipal = ExerciseUtility.createTestGroup(((JackrabbitSession) superuser).getUserManager()).getPrincipal();
        superuser.save();

        acl = AccessControlUtils.getAccessControlList(superuser, testRoot);
        if (acl == null) {
            throw new NotExecutableException();
        }
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            Authorizable testGroup = ((JackrabbitSession) superuser).getUserManager().getAuthorizable(testPrincipal);
            if (testGroup != null) {
                testGroup.remove();
                superuser.save();
            }
        } finally {
            super.tearDown();
        }
    }

    public void testEmptyList() throws RepositoryException {
        AccessControlEntry[] entries = acl.getAccessControlEntries();
        int expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, entries.length);

        int expectedSize = -1; // EXERCISE
        assertEquals(expectedSize, acl.size());

        boolean expectedIsEmpty = false; // EXERCISE
        assertEquals(expectedIsEmpty, acl.isEmpty());

        // EXERCISE: can you set an empty ACL? if not fix the test accordingly.
        acMgr.setPolicy(testRoot, acl);

        // EXERCISE: retrieve the policy with acMgr.getPolicies(). what will the ACL look like?
    }

    public void testAddEntries() throws RepositoryException {
        Privilege[] privileges1 = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ, Privilege.JCR_WRITE);
        Privilege[] privileges2 = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_ALL);

        Principal principal1 = testPrincipal;
        Principal principal2 = EveryonePrincipal.getInstance();

        // EXERCISE : make use of the different variants provided by JCR and Jackrabbit API to set ACEs to the list.
        // EXERCISE : verify the expected result and test the size/nature of the ACL afterwards
        // HINT the test AccessControlListImpl test will make you familiar with some implementation details
    }

    public void testRemoveEntry() throws RepositoryException {
        assertTrue(AccessControlUtils.addAccessControlEntry(superuser, testRoot, testPrincipal, new String[]{Privilege.JCR_READ}, true));
        assertTrue(AccessControlUtils.addAccessControlEntry(superuser, testRoot, EveryonePrincipal.getInstance(), new String[]{Privilege.JCR_READ}, false));

        // EXERCISE remove the Everyone-ACE from the list and verify that the list still contains the entry for testPrincipal.

        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(superuser, testRoot);
        assertNotNull(acl);
        assertEquals(1, acl.size());
    }

    public void testReorderEntries() throws Exception {
        Privilege[] read = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ, Privilege.JCR_READ_ACCESS_CONTROL);
        Privilege[] write = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_WRITE);

        acl.addAccessControlEntry(testPrincipal, read);
        acl.addEntry(testPrincipal, write, false);
        acl.addAccessControlEntry(EveryonePrincipal.getInstance(), write);

        AccessControlEntry[] entries = acl.getAccessControlEntries();

        assertEquals(3, entries.length);
        AccessControlEntry first = entries[0];
        AccessControlEntry second = entries[1];
        AccessControlEntry third = entries[2];

        // EXERCISE: reorder 'second' to the first position

        entries = acl.getAccessControlEntries();
        assertEquals(second, entries[0]);
        assertEquals(first, entries[1]);
        assertEquals(third, entries[2]);

        // EXERCISE reorder 'third' before 'first'

        entries = acl.getAccessControlEntries();
        assertEquals(second, entries[0]);
        assertEquals(third, entries[1]);
        assertEquals(first, entries[2]);

        // EXERCISE reorder 'second' to the end of the list

        entries = acl.getAccessControlEntries();
        assertEquals(third, entries[0]);
        assertEquals(first, entries[1]);
        assertEquals(second, entries[2]);
    }

    public void testGetPath() throws RepositoryException {
        String expectedPath = null; // EXERCISE
        assertEquals(expectedPath, acl.getPath());

        // EXERCISE: does that the following code work? why? if not fix the code
        acMgr.setPolicy(acl.getPath(), acl);
    }
}