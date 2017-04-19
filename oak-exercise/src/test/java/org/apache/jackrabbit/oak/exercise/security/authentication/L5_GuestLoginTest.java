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

import javax.jcr.GuestCredentials;
import javax.jcr.LoginException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.exercise.security.user.L15_RepositoryWithoutAnonymousTest;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: Authentication
 * =============================================================================
 *
 * Title: Guest Login (aka Anonymous Login)
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand how to login as guest, the meaning of the {@link javax.jcr.GuestCredentials}
 * and how this is linked to the 'anonymous' user.
 *
 * Exercises:
 *
 * - {@link #testAnonymousGuestLogin()}
 *   Walk through the anonymous login {@link #testAnonymousGuestLogin()}
 *   Question: Can you identify ares in the default authentication setup that apply special handling for anonymous?
 *
 * - {@link #testAnonymousSimpleCredentialsLogin()}
 *   Try to login as anonymous with SimpleCredentials
 *   Question: Why can't you login as anonymous with {@link javax.jcr.SimpleCredentials}?
 *
 * - {@link #testAnonymousSimpleCredentialsLoginSuccess}
 *   In order to understand what makes the guest-login special compared to a
 *   regular user-login, modify the test-case such that a regular login with
 *   SimpleCredentials succeeds.
 *
 * - {@link #testDisableGuestLogin()}
 *   Use this test to prevent anonymous access in an existing/running oak repository.
 *   Modify the test such that it success. How many variants do you find?
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Run a Sling (Granite|Cq) application and identify how the Sling Authentication
 *   deals with guest login.
 *   Question: What is the Sling way to disable anonymous (guest) access?
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L6_AnonymousIdTest ()}
 * - {@link L15_RepositoryWithoutAnonymousTest ()}
 * - {@link L9_NullLoginTest ()}
 *
 * </pre>
 *
 * @see javax.jcr.GuestCredentials
 * @see org.apache.jackrabbit.api.security.user.User#getCredentials()
 * @see org.apache.jackrabbit.api.security.user.User#changePassword(String, String)
 * @see org.apache.jackrabbit.api.security.user.User#disable(String)
 */
public class L5_GuestLoginTest extends AbstractJCRTest {

    private Repository repository;
    private Session testSession;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        repository = getHelper().getRepository();
        superuser.save();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (testSession != null && testSession.isLive()) {
                testSession.logout();
            }
        } finally {
            super.tearDown();
        }
    }

    public void testAnonymousGuestLogin() throws RepositoryException {
        testSession = repository.login(new GuestCredentials());
    }

    public void testAnonymousSimpleCredentialsLogin() throws RepositoryException {
        testSession = repository.login(new GuestCredentials());

        String anonymousID = testSession.getUserID();
        testSession.logout();

        try {
            testSession = repository.login(new SimpleCredentials(anonymousID, new char[0]));
            fail("Anonymous cannot login with simple credentials.");
        } catch (LoginException e) {
            // success
            // EXERCISE: explain why
        }
    }

    public void testAnonymousSimpleCredentialsLoginSuccess() throws RepositoryException {
        testSession = repository.login(new GuestCredentials());

        String anonymousID = testSession.getUserID();

        // EXERCISE: how to you need to modify the test-case that this would work?

        Session anonymousUserSession = repository.login(new SimpleCredentials(anonymousID, new char[0]));
        assertEquals(UserConstants.DEFAULT_ANONYMOUS_ID, testSession.getUserID());
    }

    public void testDisableGuestLogin() throws RepositoryException {

        // EXERCISE : identify ways to prevent anonymous login with GuestCredentials in an existing repository
        //            extend the test here such that the login below fails
        // 1:
        // 2:

        try {
            testSession = repository.login(new GuestCredentials());
            fail("Anonymous login must fail.");
        } catch (LoginException e) {
            // success
        }
    }
}