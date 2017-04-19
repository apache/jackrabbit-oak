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

import java.security.PrivilegedExceptionAction;
import javax.jcr.GuestCredentials;
import javax.jcr.LoginException;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: Authentication
 * =============================================================================
 *
 * Title: Null Login with PrePopulated Subject (Pre-Authentication without LoginModule)
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the meaning and usage of the pre-authenticated login with
 * {@link javax.jcr.Repository#login()} (i.e. {@code null} credentials).
 *
 * Exercises:
 *
 * - {@link #testNullLogin()}
 *   Step through a regular JCR login without credentials and explain why this
 *   is expected to fail.
 *
 * - {@link #testSuccessfulNullLogin()}
 *   This test-case illustrates a usage of the 'null' login. Complete the test
 *   by creating/populating a valid {@link Subject} and verify your expectations
 *   after a successful login.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * In Jackrabbit 2.x the null-credentials login was treated as login of the
 * anonymous guest user.
 *
 * - Use {@link #testJr2CompatibleLoginConfiguration} to configure the
 *   {@link javax.security.auth.login.LoginContext} such that the repository behaves
 *   like Jackrabbit 2.x and treats {@link javax.jcr.Repository#login()}
 *   (null-login) as anonymous login.
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L3_LoginModuleTest}
 * - {@link L8_PreAuthTest}
 *
 * </pre>
 *
 * @see <a href="http://jackrabbit.apache.org/oak/docs/security/authentication/preauthentication.html">Pre-Authentication Documentation</a>
 */
public class L9_NullLoginTest extends AbstractJCRTest {

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

    public void testNullLogin() throws RepositoryException {
        try {
            testSession = repository.login();
            fail();
        } catch (LoginException e) {
            // success
            // EXERCISE: explain what is going on and why it is expected to fail.
        }
    }

    public void testSuccessfulNullLogin() throws Exception {
        // EXERCISE: populate a subject that results in successful null-login
        Subject subject = null;
        String expectedId = null;

        testSession = Subject.doAs(subject, new PrivilegedExceptionAction<Session>() {
            @Override
            public Session run() throws RepositoryException {
                return repository.login(null, null);
            }
        });

        assertEquals(expectedId, testSession.getUserID());
    }

    public void testJr2CompatibleLoginConfiguration() throws RepositoryException {
        // EXERCISE: define the JAAS configuration that allows you to have null-login treated as anonymous login.
        Configuration configuration = null;

        Configuration.setConfiguration(configuration);
        try {
            testSession = repository.login();

            Session guest = repository.login(new GuestCredentials());
            String expectedId = guest.getUserID();
            guest.logout();

            assertEquals(expectedId, testSession.getUserID());
        } finally {
            Configuration.setConfiguration(null);
        }
    }
}