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
package org.apache.jackrabbit.oak.security.user;

import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: User Management
 * =============================================================================
 *
 * Title: Password Test
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Become familiar with password related parts of the user management API and
 * get to know some implementation details.
 *
 * Exercises:
 *
 * - {@link #testPasswordInContent()}
 *   Creates a new user with a valid password. Inspect how the password is being
 *   store in the repository (Note: implementation detail!) and fill in the
 *   right property name to get the test-case pass.
 *   Explain why the password property doesn't contain the password string.
 *
 * - {@link #testCreateUserAndLogin()}
 *   Same as {@link #testPasswordInContent()} but additional aims to login as
 *   the new user.
 *   Fix the test by creating the correct {@link javax.jcr.Credentials}.
 *
 * - {@link #testCreateUserWithoutPassword()}
 *   This test creates a new user with a 'null' password. Inspect the user node
 *   created by this method and add the correct assertion wrt password.
 *
 * - {@link #testCreateUserWithoutPasswordAndLogin()}
 *   Same as {@link #testCreateUserWithoutPassword()}. This time fix the test
 *   case to properly reflect the expected behavior upon login for that new user.
 *
 * - {@link #testChangePassword()}
 *   Change the password of an existing user. Use both variants and get familiar
 *   with the implementation specific constraints.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * In a OSGI-based Oak installation (Sling|Granite|CQ) you can easily perform the
 * following additional test.
 * Note: You can also do that in Java by building a new Jcr/Oak repository with
 * the corresponding configuration parameters set.
 *
 * - Go to the system console and change the default configuration parameters
 *   in the 'Apache Jackrabbit Oak UserConfiguration' and play with the following
 *   configuration parameters:
 *   - {@link org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_PASSWORD_HASH_ALGORITHM}
 *   - {@link org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_PASSWORD_HASH_ITERATIONS}
 *   - {@link org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_PASSWORD_SALT_SIZE}
 *   Change the password of a test user and observe the changes.
 *
 * - Go to the system console and look for the 'Apache Jackrabbit Oak AuthorizableActionProvider'.
 *   Enable the password validation action and then change the password of
 *   an existing test user.
 *
 *
 * Advanced Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Write a custom password validation action and plug it into your repository.
 *   See Oak documentation for some hints.
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L10_PasswordExpiryTest ()}
 * - {@link org.apache.jackrabbit.oak.security.user.action.L2_AuthorizableActionTest ()}
 *
 * </pre>
 *
 * @see User#changePassword(String, String)
 * @see User#changePassword(String)
 * @see org.apache.jackrabbit.oak.spi.security.user.action.PasswordValidationAction
 * @see org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil
 */
public class L4_PasswordTest extends AbstractJCRTest {

    private UserManager userManager;

    private User testUser;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        userManager = ((JackrabbitSession) superuser).getUserManager();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (testUser != null) {
                testUser.remove();
            }
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    public void testPasswordInContent() throws RepositoryException {
        testUser = userManager.createUser("testUser", "pw");
        superuser.save();

        Node userNode = superuser.getNode(testUser.getPath());
        String pwPropertyName = null; // TODO: fill in

        Property pwProperty = userNode.getProperty(pwPropertyName);

        // TODO: explain why the password property doesn't contain the 'pw' string
        assertFalse("pw".equals(pwProperty.getString()));
    }

    public void testCreateUserAndLogin() throws RepositoryException {
        testUser = userManager.createUser("testUser", "pw");
        superuser.save();

        Credentials creds = null; // TODO build the credentials
        getHelper().getRepository().login(creds).logout();
    }

    public void testCreateUserWithoutPassword() throws RepositoryException {
        testUser = userManager.createUser("testUser", null);
        superuser.save();

        // TODO: look at the user node. does it have a password property set?
        // TODO: add the correct assertion
        Node userNode = superuser.getNode(testUser.getPath());
    }

    public void testCreateUserWithoutPasswordAndLogin() throws RepositoryException {
        testUser = userManager.createUser("testUser", null);
        superuser.save();

        // TODO: build the credentials and fix the test-case such that it no longer fails
        Credentials creds = null;
        getHelper().getRepository().login(creds).logout();
    }

    public void testChangePassword() throws RepositoryException {
        testUser = userManager.createUser("testUser", null);
        superuser.save();

        String newPassword = null; // TODO : define valid value(s)
        testUser.changePassword(newPassword);

        String oldPassword = null; // TODO : fill in the correct value
        newPassword = null;        // TODO : fill in a valid value; Q: can you use null?
        testUser.changePassword(newPassword, oldPassword);

        superuser.save();
    }
}