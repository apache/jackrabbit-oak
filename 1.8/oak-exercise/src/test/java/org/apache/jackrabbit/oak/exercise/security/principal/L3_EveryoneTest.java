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
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.oak.exercise.security.user.L3_UserVsPrincipalTest;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: Principal Management
 * =============================================================================
 *
 * Title: Everyone Test
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the role of the {@link org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal}
 *
 * Exercises:
 *
 * - {@link #testEveryoneExists()}
 *   Test to illustrate the that everyone principal always exists and always is
 *   an instanceof {@link java.security.acl.Group} even if there is no corresponding
 *   authorizable.
 *   Discuss the meaning of the everyone principal and why having a corresponding authorizable is optional.
 *   Note the difference between java.security.acl.Group and org.apache.jackrabbit.api.security.user.Group.
 *
 * - {@link #testEveryoneName()}
 *   Test to illustrate that the name of the everyone principal is constant.
 *   Complete the test case by typing the expected name.
 *
 * - {@link #testAccessByName()}
 *   Even though there exists a dedicated method to retrieve the everyone principal
 *   you can equally access it by name.
 *   Use the principal management API to retrieve the everyone principal by name.
 *   Discuss the drawback of this approach in an environment where you don't have access to the Oak constants.
 *
 * - {@link #testEveryoneIsMemberofEveryone()}
 *   Test case illustrating the dynamic nature of the everyone principal.
 *   Walk through the test
 *   Try to extend the test such that the default principal management exposes additional user|group principals.
 *
 * - {@link #testEveryoneAsAuthorizableGroup()}
 *   Additional test illustrating that the dynamic nature of the everyone principal
 *   does not change if there exists a corresponding authorizable group.
 *   > Create a new authorizable that corresponds to the everyone principal (Q: what parameters are constants?)
 *   > Verify that principal exposed by the authorizable corresponds to the everyone principal.
 *   > Assert that the dynamic nature of the principal has not changed.
 *   > Test if the dynamic nature also applies to the authorizable
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * The following exercises can easily be performed in a Sling based repository
 * installation (e.g. Granite|CQ) with the same setup as in this test class.
 *
 * - Test if there exists an everyone authorizable group.
 *   If this is the case try what happens if you remove that authorizable and discuss the consequences.
 *   Question: Can you explain why it exists?
 *
 * </pre>
 *
 * @see L3_UserVsPrincipalTest
 */
public class L3_EveryoneTest extends AbstractJCRTest {

    private PrincipalManager principalManager;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        principalManager = ((JackrabbitSession) superuser).getPrincipalManager();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testEveryoneExists() throws RepositoryException {
        Principal everyone = principalManager.getEveryone();

        assertNotNull(everyone);
        assertTrue(everyone instanceof java.security.acl.Group);

        Authorizable everyoneAuthorizable = ((JackrabbitSession) superuser).getUserManager().getAuthorizable(everyone);
        assertNull(everyoneAuthorizable);
    }

    public void testEveryoneName() throws RepositoryException {
        Principal everyone = principalManager.getEveryone();

        String expectedName = null; // EXERCISE type the expected authorizable name using constants defined by oak.
        assertEquals(expectedName, everyone.getName());
    }

    public void testAccessByName() throws RepositoryException {
        Principal everyone = principalManager.getEveryone();

        assertTrue(principalManager.hasPrincipal(everyone.getName()));

        Principal everyoneByName = null; // EXERCISE: retrieve the everyone principal by name
        assertEquals(everyone, everyoneByName);
    }

    public void testEveryoneIsMemberofEveryone() throws RepositoryException {
        java.security.acl.Group everyone = (java.security.acl.Group) principalManager.getEveryone();
        PrincipalIterator it = principalManager.getPrincipals(PrincipalManager.SEARCH_TYPE_ALL);

        // EXERCISE: discuss the dynamic nature of the everyone group principal
        while (it.hasNext()) {
            Principal principal = it.nextPrincipal();
            if (everyone.equals(principal)) {
                assertFalse(everyone.isMember(principal));
            } else {
                assertTrue(everyone.isMember(principal));
            }
        }
    }

    public void testEveryoneAsAuthorizableGroup() throws RepositoryException {
        // EXERCISE: create an authorizable that corresponds to the everyone principal.
        org.apache.jackrabbit.api.security.user.Group everyoneAuthorizable = null;
        superuser.save();

        try {
            java.security.acl.Group everyone = (java.security.acl.Group) principalManager.getEveryone();

            assertEquals(everyone, everyoneAuthorizable.getPrincipal());

            // EXERCISE: verify that the everyone principal is still a dynamic group
            // EXERCISE: test if the dyanmic nature also applies to the authorizable
        } finally {
            if (everyoneAuthorizable != null) {
                everyoneAuthorizable.remove();
                superuser.save();
            }
        }
    }
}