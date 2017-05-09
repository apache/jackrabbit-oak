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
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.oak.security.authorization.accesscontrol.InvalidTestPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;

import static org.junit.Assert.assertArrayEquals;

/**
 * <pre>
 * Module: Authorization (Access Control Management)
 * =============================================================================
 *
 * Title: AccessControlList Implementation Details
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand some of the implementation details applied by the default
 * access control list provided by the Oak access control management.
 *
 * Exercises:
 *
 * - {@link #testAddEntryTwice()}
 *   Adding the same ACE twice does not work.
 *   Verify the expectation by looking at the ACEs exposed by the list.
 *
 * - {@link #testUpdateAndComplementary()}
 *   The default implementation of the JackrabbitAccessControlList interface
 *   performs some optimization upon ACE-addition.
 *   Walk through the setup and complete the test case such that it passes.
 *
 * - {@link #testAddEntryWithInvalidPrincipals()}
 *   This tests creates a list of invalid principals for which adding an ACE
 *   will fail.
 *
 *   Question: Can you explain for each of these principals why?
 *
 * - {@link #testAddEntriesWithCustomKnownPrincipal()}
 *   Here we use a custom principal implementation as well but a principal
 *   with the given name is actually known.
 *   Walk through the test and complete it such that it passes.
 *
 * - {@link #testAddEntryWithInvalidPrivilege()}
 *   Walk through the test and explain why creating ACE for the given list of
 *   privilege arrays must fail.
 *
 * - {@link #testRemoveInvalidEntry()}
 *   Walk through the removal and explain why removing an ACE with the same
 *   characteristics is expected to fail.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * The JCR specification mandates the that the principal used to create an ACE
 * is known to the system.
 *
 * - Investigate how the Oak repository can be configured such that creating
 *   ACEs with unknown principals would still succeed.
 *
 *   Question: Can you name the configuration option and list the allowed values? What are the differences?
 *   Question: Can you find other places in the access control management code
 *             base where this is being used?
 *   Question: Can you imagine the use cases for such a different or relaxed behaviour?
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L6_AccessControlContentTest}
 *
 * </pre>
 */
public class L5_AccessControlListImplTest extends AbstractJCRTest {

    private AccessControlManager acMgr;
    private JackrabbitAccessControlList acl;

    private Principal testPrincipal;
    private Privilege[] testPrivileges;

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

        testPrivileges = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ, Privilege.JCR_WRITE);
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

    public void testAddEntryTwice() throws Exception {
        acl.addEntry(testPrincipal, testPrivileges, true, Collections.<String, Value>emptyMap());

        boolean expectedResult = false; // EXERCISE
        assertEquals(expectedResult, acl.addEntry(testPrincipal, testPrivileges, true, Collections.<String, Value>emptyMap()));

        // EXERCISE : verify the size of the ACL.
    }

    public void testUpdateAndComplementary() throws Exception {
        Privilege[] readPriv = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ);
        Privilege[] writePriv = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_WRITE);
        Privilege[] acReadPriv = AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ_ACCESS_CONTROL);

        assertTrue(acl.addEntry(testPrincipal, readPriv, true));
        assertTrue(acl.addEntry(testPrincipal, writePriv, true));
        assertTrue(acl.addEntry(testPrincipal, acReadPriv, true));

        int expectedSize = -1; // EXERCISE
        assertEquals(expectedSize, acl.size());

        assertTrue(acl.addEntry(testPrincipal, readPriv, false));

        expectedSize = -1; // EXERCISE
        assertEquals(expectedSize, acl.size());


        AccessControlEntry[] entries = acl.getAccessControlEntries();

        Privilege[] expectedPrivileges = null; // EXERCISE
        assertArrayEquals(expectedPrivileges, entries[0].getPrivileges());

        Privilege[] expectedPrivileges1 = null; // EXERCISE
        assertArrayEquals(expectedPrivileges1, entries[1].getPrivileges());
    }

    public void testAddEntryWithInvalidPrincipals() throws Exception {
        // EXERCISE: explain for each principal in the list why using it for an ACE fails
        List<Principal> invalidPrincipals = ImmutableList.of(
                new InvalidTestPrincipal("unknown"),
                null,
                new PrincipalImpl(""), new Principal() {
            @Override
            public String getName() {
                return "unknown";
            }
        });

        for (Principal principal : invalidPrincipals) {
            try {
                acl.addAccessControlEntry(principal, testPrivileges);
                fail("Adding an ACE with an invalid principal should fail");
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    public void testAddEntriesWithCustomKnownPrincipal()  throws Exception {
        Principal oakPrincipal = new PrincipalImpl(testPrincipal.getName());
        Principal principal = new Principal() {
            @Override
            public String getName() {
                return testPrincipal.getName();
            }
        };

        assertTrue(acl.addAccessControlEntry(oakPrincipal, AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ)));
        assertTrue(acl.addAccessControlEntry(principal, AccessControlUtils.privilegesFromNames(acMgr, Privilege.JCR_READ_ACCESS_CONTROL)));

        int expectedLength = -1; // EXERCISE
        assertEquals(expectedLength, acl.getAccessControlEntries().length);
    }

    public void testAddEntryWithInvalidPrivilege() throws Exception {
        String privilegeName = "AccessControlListImplTestPrivilege";
        Privilege customPriv = ((JackrabbitWorkspace) superuser.getWorkspace()).getPrivilegeManager().registerPrivilege(privilegeName, true, new String[0]);

        // EXERCISE : walks through this test and explain why adding those ACEs fails.
        List<Privilege[]> invalidPrivileges = ImmutableList.of(
                new Privilege[0],
                null,
                new Privilege[] {customPriv}
        );

        for (Privilege[] privs : invalidPrivileges) {
            try {
                acl.addAccessControlEntry(testPrincipal, privs);
                fail("Adding an ACE with invalid privilege array should fail.");
            } catch (AccessControlException e) {
                // success
            }
        }
    }

    public void testRemoveInvalidEntry() throws RepositoryException {
        assertTrue(AccessControlUtils.addAccessControlEntry(superuser, testRoot, testPrincipal, testPrivileges, true));

        // EXERCISE : walk through the removal and explain the expected behaviour.
        try {
            acl.removeAccessControlEntry(new JackrabbitAccessControlEntry() {
                public boolean isAllow() {
                    return false;
                }

                public String[] getRestrictionNames() {
                    return new String[0];
                }

                public Value getRestriction(String restrictionName) {
                    return null;
                }

                public Value[] getRestrictions(String restrictionName) {
                    return null;
                }

                public Principal getPrincipal() {
                    return testPrincipal;
                }

                public Privilege[] getPrivileges() {
                    return testPrivileges;
                }
            });
            fail("Passing an unknown ACE should fail");
        } catch (AccessControlException e) {
            // success
        }
    }
}