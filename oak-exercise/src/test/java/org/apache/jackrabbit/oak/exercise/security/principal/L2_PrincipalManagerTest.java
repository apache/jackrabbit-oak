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
package org.apache.jackrabbit.oak.exercise.security.principal;

import java.security.Principal;
import java.util.Set;
import javax.jcr.RepositoryException;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.apache.jackrabbit.test.NotExecutableException;

/**
 * <pre>
 * Module: Principal Management
 * =============================================================================
 *
 * Title: Principal Manager
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Get familiar with the {@link org.apache.jackrabbit.api.security.principal.PrincipalManager}
 * interface.
 *
 * Exercises:
 *
 * - {@link #testHasPrincipal()}
 *   Walk through the test for an existing principal name to become familiar
 *   with the principal lookup.
 *   Complete the method {@link #getNonExistingPrincipalName()} such that the test passes.
 *
 *
 * - {@link #testGetPrincipal()}
 *   Same as above for {@link PrincipalManager#getPrincipal(String)}
 *
 * - {@link #testFindPrincipals()}
 *   Become familiar with the principal query: Look at the API contract and
 *   try different variants of the {@link PrincipalManager#findPrincipals(String, int)} call.
 *
 * - {@link #testGetGroupMembership()}
 *   Retrive the principal groups our test principal is member of.
 *   Question: What is the expected result?
 *   Question: Is there a minimal set of principals? If yes, why?
 *
 *
 * Additional Exercises
 * -----------------------------------------------------------------------------
 *
 * - {@link #testMyPrincipal()}
 *   Make use of the characteristics of the default principal management
 *   implementation and adjust the test case such that it passes.
 *
 *
 * Related Exercises
 * -----------------------------------------------------------------------------
 *
 * - {@link L3_EveryoneTest}
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.api.security.principal.PrincipalManager
 */
public class L2_PrincipalManagerTest extends AbstractJCRTest {

    private PrincipalManager principalManager;

    private String testPrincipalName;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        principalManager = ((JackrabbitSession) superuser).getPrincipalManager();

        // NOTE: this method call doesn't make to much sense outside of a
        // simple test with a very limited number of principals (!!)
        PrincipalIterator principalIterator = principalManager.getPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        if (principalIterator.hasNext()) {
            testPrincipalName = principalIterator.nextPrincipal().getName();
        }

        if (testPrincipalName == null) {
            throw new NotExecutableException();
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private String getNonExistingPrincipalName() {
        // EXERCISE: return a non existing principal name (Q: what could you do instead of guessing?)
        return null;
    }

    public void testHasPrincipal() throws RepositoryException {
        assertTrue(principalManager.hasPrincipal(testPrincipalName));

        assertFalse(principalManager.hasPrincipal(getNonExistingPrincipalName()));
    }

    public void testGetPrincipal() throws RepositoryException {
        Principal principal = principalManager.getPrincipal(testPrincipalName);
        assertNotNull(principal);

        assertNull(principalManager.getPrincipal(getNonExistingPrincipalName()));
    }

    public void testFindPrincipals() throws RepositoryException {
        String searchHint = testPrincipalName;             // EXERCISE: play with the search hint
        int searchType = PrincipalManager.SEARCH_TYPE_ALL; // EXERCISE: modify the type flag

        PrincipalIterator principalIterator = principalManager.findPrincipals(testPrincipalName, searchType);

        // EXERCISE: what is the expected query result depending on the search hint and the type-flag?
    }

    public void testGetGroupMembership() throws RepositoryException {
        Principal principal = principalManager.getPrincipal(testPrincipalName);

        PrincipalIterator groups = principalManager.getGroupMembership(principal);
        // EXERCISE: what group principals do you expect here?
    }

    public void testMyPrincipal() throws RepositoryException {
        String principalName = "myPrincipal";

        // EXERCISE : fix the test case
        // HINT : take advantage of the default implemenation (i.e. it's relation to user management)

        try {
            Principal testPrincipal = principalManager.getPrincipal(principalName);
            assertNotNull(testPrincipal);

            Set<String> groupNames = Sets.newHashSet("myGroup", EveryonePrincipal.NAME);
            PrincipalIterator groups = principalManager.getGroupMembership(testPrincipal);
            while (groups.hasNext()) {
                groupNames.remove(groups.nextPrincipal().getName());
            }

            assertTrue(groupNames.isEmpty());
        } finally {
            // EXERCISE: cleanup
        }
    }


}