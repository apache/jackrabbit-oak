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
package org.apache.jackrabbit.oak.exercise.security.authentication;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import javax.jcr.LoginException;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.exercise.ExerciseUtility;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: Authentication
 * =============================================================================
 *
 * Title: Impersonation
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Become familiar with {@link javax.jcr.Session#impersonate(javax.jcr.Credentials)}
 * and how this is implemented in Oak.
 * Please note that this exercise mixes authentication with user management
 * functionality.
 *
 * Exercises:
 *
 *
 * - {@link #testImpersonateTestUser()}
 *   This test illustrates how a given session can be allowed to impersonate
 *   another user. Use this test to walk through the impersonation step by step
 *   and identify which principal must be granted impersonation in order to
 *   get the test pass without exception.
 *
 *   Question: Can you explain where the impersonation information is being stored in the repository?
 *   Question: Can you explain where the impersonation is being evaluated.
 *   Question: Why is impersonation being granted to a Principal and not a userID?
 *
 * - {@link #testImpersonateOneSelf()}
 *   Walk through {@link Session#impersonate(javax.jcr.Credentials)} in this
 *   test case and test if the test user can impersonate himself; fix the test
 *   accordingly.
 *
 *   Question: Can you identify the location of the code that makes this pass/fail?
 *   Question: Can you identify how this behavior could be changed by a different repository configuration?
 *
 * - {@link #testAdminCanImpersonateEveryone()}
 *   Walk through {@link Session#impersonate(javax.jcr.Credentials)} in this
 *   test case and explain why the admin user is allowed to impersonate the
 *   test user although impersonation is not explicitly granted.
 *
 *   Question: What kind of security concerns can you identify with this shortcut?
 *             Discuss and explain your findings.
 *
 *
 * Advanced Exercise:
 * -----------------------------------------------------------------------------
 *
 * Once you feel familiar with the various security modules (including access
 * control and user management), you may want to come back to this advanced
 * exercise that requires an understanding of all areas.
 *
 * - Impersonation and pluggable authentication
 *   Once you feel comfortable with the pluggable nature of the authentication
 *   module, discuss how replacing the default login module chain will affect
 *   how {@link Session#impersonate(javax.jcr.Credentials)} works.
 *
 *   Question: Can you think of a different mechanism on how to validate if a given
 *             impersonation requestion should succeed?
 *   Question: What are the classes present with Oak that you need to deal with
 *             in your custom implementation?
 *
 * - {@link #testAdvancedImpersonationTest()}
 *   This advanced tests mixes all three security areas involved in the impersonation:
 *   1. Authorization: the required permission to write the list of impersonators
 *   2. User Management: API to grant (and revoke) impersonation.
 *   3. Authentication: The impersonation itself.
 *
 * </pre>
 *
 * @see javax.jcr.Session#impersonate(javax.jcr.Credentials)
 * @see org.apache.jackrabbit.api.security.user.Impersonation
 * @see org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials
 */
public class L7_ImpersonationTest extends AbstractJCRTest {

    private UserManager userManager;

    private User testUser;
    private User anotherUser;

    private List<Session> sessionList = new ArrayList<Session>();

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        userManager = ((JackrabbitSession) superuser).getUserManager();
        testUser = ExerciseUtility.createTestUser(userManager);
        anotherUser = ExerciseUtility.createTestUser(userManager);
        superuser.save();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            for (Session s : sessionList) {
                if (s.isLive()) {
                    s.logout();
                }
            }
            if (testUser != null) {
                testUser.remove();
            }
            if (anotherUser != null) {
                anotherUser.remove();
            }
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    public void testImpersonateTestUser() throws RepositoryException {
        Principal principal = null; // EXERCISE: fill in the correct principal such that the test passes.

        Impersonation impersonation = anotherUser.getImpersonation();
        impersonation.grantImpersonation(principal);
        superuser.save();

        Session testSession = superuser.getRepository().login(ExerciseUtility.getTestCredentials(testUser.getID()));
        sessionList.add(testSession);

        Session impersonated = testSession.impersonate(new SimpleCredentials(anotherUser.getID(), new char[0]));
        sessionList.add(impersonated);

        assertEquals(anotherUser.getID(), impersonated.getUserID());
    }

    public void testImpersonateOneSelf() throws RepositoryException {
        // EXERCISE: walk through this impersonation. does it work? if it does: why?

        Session testSession = superuser.getRepository().login(ExerciseUtility.getTestCredentials(testUser.getID()));
        sessionList.add(testSession);

        Session impersonated = testSession.impersonate(new SimpleCredentials(testUser.getID(), new char[0]));
        sessionList.add(impersonated);

        assertEquals(testUser.getID(), impersonated.getUserID());
    }

    public void testAdminCanImpersonateEveryone() throws RepositoryException {
        // EXERCISE: walk through this impersonation. does it work? if it does: why?

        Session impersonated = superuser.impersonate(new SimpleCredentials(anotherUser.getID(), new char[0]));
        sessionList.add(impersonated);

        assertEquals(anotherUser.getID(), impersonated.getUserID());
    }

    public void testAdvancedImpersonationTest() throws RepositoryException {
        // EXERCISE: change the permission setup such that the test-user is allowed to make himself an impersonator of 'another' user.

        Session testSession = superuser.getRepository().login(ExerciseUtility.getTestCredentials(testUser.getID()));
        sessionList.add(testSession);

        UserManager uMgr = ((JackrabbitSession) testSession).getUserManager();
        User another = uMgr.getAuthorizable(anotherUser.getID(), User.class);
        assertNotNull(another);

        Principal princ = uMgr.getAuthorizable(testUser.getID()).getPrincipal();
        another.getImpersonation().grantImpersonation(princ);
        testSession.save();

        testSession.impersonate(new SimpleCredentials(anotherUser.getID(), new char[0])).logout();

        // EXERCISE: change the impersonation of 'anotherUser' again such that the impersonate call fails
        // EXERCISE: withouth changing the permission setup. what API calls do you have at hand?

        try {
            Session s = testSession.impersonate(new SimpleCredentials(anotherUser.getID(), new char[0]));
            sessionList.add(s);
            fail("Test user must no longer be able to edit the impersonation of the test user");
        } catch (LoginException e) {
            // success
        }
    }
}