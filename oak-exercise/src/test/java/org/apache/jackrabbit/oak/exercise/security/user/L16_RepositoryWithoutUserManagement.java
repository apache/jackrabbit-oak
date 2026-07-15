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
package org.apache.jackrabbit.oak.exercise.security.user;

import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.RepositoryException;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;

/**
 * <pre>
 * Module: User Management & Authentication
 * =============================================================================
 *
 * Title: Repository without User Management
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * This advanced exercise aims to help you understand that user management
 * is in fact an optional part of the Oak repository implementation.
 * While existing application may rely on the user management to be present
 * by default it isn't too hard to come up with a setup that omits this
 * module altogether (e.g. if user/group information is being held outside
 * of the repository content).
 *
 * - Define an repository setup that doesn't require a user management implementation.
 *
 *   Question: Which parts need to be modified?
 *   Question: What are the consequences for Jcr/Jackrabbit API consumers?
 *
 * This exercise mainly consists of defined a
 * - SecurityProvider implementation
 * - JAAS configuration
 * that provides the desired repository setup without user management.
 *
 * It comes with the following test cases:
 *
 * - {@link #testUserManagementDescriptor()}
 *   Verifies that the repository descriptiors don't list {@link org.apache.jackrabbit.api.JackrabbitRepository#OPTION_USER_MANAGEMENT_SUPPORTED}
 *   This test passes if your setup is correct.
 *
 * - {@link #testNoUserManagementSupported()}
 *   This test verifies that no {@link org.apache.jackrabbit.oak.spi.security.user.UserConfiguration}
 *   can be obtained from the specified SecurityProvider.
 *
 * - {@link #testLogin()}
 *   Finally, a test should prove that a user with the some Credentials (valid
 *   for your custom setup) can actually login to the repository and is associated
 *   with a valid {@link org.apache.jackrabbit.oak.api.AuthInfo}.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * You may want to write additional test-cases that verify that the
 * {@link org.apache.jackrabbit.api.security.principal.PrincipalManager} allows
 * you to retrieve valid principals, which can be used to setup access control
 * content for your repository (which for simplicity might use the built-in
 * authorization functionality).
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.oak.spi.security.SecurityProvider
 * @see org.apache.jackrabbit.oak.spi.security.user.UserConfiguration
 */
public class L16_RepositoryWithoutUserManagement extends AbstractSecurityTest {

    @Override
    protected SecurityProvider getSecurityProvider() {
        // EXERCISE: define a security provider that doesn't support user management
        // EXERCISE: and therefore doesn't expose a UserConfiguration implementation.
        // hint: you need a custom way to look up principals
        // hint: make use of CustomPrincipalConfiguration and CustomPrincipalProvider
        // and adjust it according to your needs!
        return super.getSecurityProvider();
    }

    @Override
    protected Configuration getConfiguration() {
        // EXERCISE: define a suitable jaas configuration that doesn't tries to
        // EXERCISE: validate credentials against the user information stored in the repository
        // hint: define a configuration with CustomLoginModule (and adjust the latter for your needs)
        return super.getConfiguration();
    }

    @Test
    public void testUserManagementDescriptor() throws RepositoryException {
        Oak oak = new Oak()
                .with(new InitialContent())
                .with(getSecurityProvider());
        ContentRepository contentRepository = oak.createContentRepository();

        assertFalse(contentRepository.getDescriptors().getValue(JackrabbitRepository.OPTION_USER_MANAGEMENT_SUPPORTED).getBoolean());

    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNoUserManagementSupported() {
        getSecurityProvider().getConfiguration(UserConfiguration.class);
    }

    @Test
    public void testLogin() throws LoginException, NoSuchWorkspaceException {
        String expectedId = null; // EXERCISE define the userID for the login
        Credentials creds = null; // EXERCISE define credentials that work in your setup.
        ContentSession s = login(creds);
        AuthInfo authInfo = s.getAuthInfo();

        assertNotSame(AuthInfo.EMPTY, authInfo);
        assertEquals(expectedId, authInfo.getUserID());
    }
}