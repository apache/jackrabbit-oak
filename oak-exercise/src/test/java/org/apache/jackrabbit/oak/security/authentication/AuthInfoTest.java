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
package org.apache.jackrabbit.oak.security.authentication;

import java.security.Principal;
import java.util.Set;
import javax.jcr.GuestCredentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * <pre>
 * Module: Authentication
 * =============================================================================
 *
 * Title: Oak AuthInfo
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the {@link AuthInfo} interface, how it relates to the
 * {@link javax.security.auth.Subject} and how it is populated during login.
 *
 * Exercises:
 *
 * - {@link #testGetAuthInfo()}
 *   Walk through the {@link org.apache.jackrabbit.oak.api.ContentSession#getAuthInfo()}
 *   call in order to get in insight to the implementation and how the {@link org.apache.jackrabbit.oak.api.AuthInfo}
 *   relates to the {@link javax.security.auth.Subject}
 *
 * - {@link #testGuestAuthInfo()}
 *   Walk though the login call on the Oak API and identify where the {@link AuthInfo}
 *   is being created.
 *   TODO: Fix the test case by providing the expected id and set of principals for the guest content session.
 *   TODO: Can you identify the similarities between the subject and the AuthInfo? What is missing in AuthInfo?
 *
 * - {@link #testUserAuthInfo()}
 *   Same as {@link #testGuestAuthInfo()} for a newly created user.
 *   TODO: Fix the test case by providing the expected id and set of principals for the content session.
 *   TODO: Pay attention to the way the test user has been created.
 *   TODO: What is the principal name?
 *   TODO: What is the difference between the userID and the principal name? and how is that reflected in the AuthInfo?
 *
 * - {@link #testUserAuthInfoWithGroupMembership()}
 *   Same as {@link #testUserAuthInfo()} but with the subtle difference that the
 *   test user is member of a group.
 *   TODO: Fix the test case by providing the expected set of principals for the content session.
 *   TODO: Identify how the group membership is being exposed in the AuthInfo
 *   TODO: Can you spot the 'groupID' in the AuthInfo? Or in the underlying Subject?
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.oak.api.AuthInfo
 * @see javax.security.auth.Subject
 * @see org.apache.jackrabbit.oak.api.ContentRepository#login(javax.jcr.Credentials, String)
 */
public class AuthInfoTest extends AbstractSecurityTest {

    private UserManager userManager;
    private User testUser;
    private Group testGroup;

    private ContentSession contentSession;

    @Override
    public void before() throws Exception {
        super.before();

        userManager = getUserManager(root);
    }

    @Override
    public void after() throws Exception {
        try {
            if (contentSession != null) {
                contentSession.close();
            }
            if (testUser != null) {
                testUser.remove();
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
    public void testGetAuthInfo() {
        // TODO: inspect the method
        AuthInfo authInfo = adminSession.getAuthInfo();
    }

    @Test
    public void testGuestAuthInfo() throws LoginException, NoSuchWorkspaceException {
        contentSession = login(new GuestCredentials());

        AuthInfo authInfo = contentSession.getAuthInfo();

        String expectedId = null; // TODO : fill in the expected id
        assertEquals(expectedId, authInfo.getUserID());


        // TODO: create the set of expected principals.
        // TODO: what are the variants you have at hand when using the Jackrabbit API
        // TODO: what are the variants you have at hand when using public Oak SPI interfaces?
        Set<Principal> expectedPrincipals = null; // FIXME
        assertEquals(expectedPrincipals, authInfo.getPrincipals());
    }

    @Test
    public void testUserAuthInfo() throws LoginException, RepositoryException, CommitFailedException {
        testUser = userManager.createUser("testUser", "pw", new PrincipalImpl("testPrincipal"), null);
        root.commit();

        contentSession = login(new SimpleCredentials("testUser", "pw".toCharArray()));

        AuthInfo authInfo = contentSession.getAuthInfo();

        String expectedId = null; // TODO : fill in the expected id
        assertEquals(expectedId, authInfo.getUserID());


        // TODO: create the set of expected principals.
        // TODO: what are the variants you have at hand when using the Jackrabbit API
        // TODO: what are the variants you have at hand when using public Oak SPI interfaces?
        Set<Principal> expectedPrincipals = null; // FIXME
        assertEquals(expectedPrincipals, authInfo.getPrincipals());
    }

    @Test
    public void testUserAuthInfoWithGroupMembership() throws LoginException, RepositoryException, CommitFailedException {
        testUser = userManager.createUser("testUser", "pw", new PrincipalImpl("testPrincipal"), null);
        testGroup = userManager.createGroup("testGroup", new PrincipalImpl("testGroupPrincipal"), null);
        testGroup.addMember(testUser);
        root.commit();

        contentSession = login(new SimpleCredentials("testUser", "pw".toCharArray()));

        AuthInfo authInfo = contentSession.getAuthInfo();

        // TODO: create the set of expected principals.
        // TODO: what are the variants you have at hand when using the Jackrabbit API
        // TODO: what are the variants you have at hand when using public Oak SPI interfaces?
        Set<Principal> expectedPrincipals = null; // FIXME
        assertEquals(expectedPrincipals, authInfo.getPrincipals());
    }
}