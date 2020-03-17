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

import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.test.AbstractJCRTest;

import static org.apache.jackrabbit.oak.exercise.ExerciseUtility.createTestUser;
import static org.apache.jackrabbit.oak.exercise.ExerciseUtility.getTestCredentials;

/**
 * <pre>
 * Module: Authentication
 * =============================================================================
 *
 * Title: Introduction - Login Step by Step
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Make yourself familiar with the authentication as present in JCR and in Oak.
 *
 * Exercise:
 *
 * Walk though repository login starting from the JCR repository login and
 * make yourself familiar with the authentication.
 *
 * - {@link #testUserLogin()}
 * - {@link #testAdminLogin()}
 *
 * Questions:
 *
 * - What is the Oak API correspondent of {@link Repository#login(javax.jcr.Credentials)}?
 *
 * - Identify those parts/classes/configurations in the repository authentication
 *   that can be customized
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Modify the test to use the other variants of {@link javax.jcr.Repository#login}
 * - Modify the test to use {@link org.apache.jackrabbit.api.JackrabbitRepository#login(javax.jcr.Credentials, String, java.util.Map)}
 *
 * Questions:
 *
 * - Explain the difference between the different login flavors
 *
 * - Explain the difference of the {@code JackrabbitRepository} login extension
 *   wrt regular JCR login and explain what it is (could be) used for.
 *   Hint: Look at Sling (Granite|CQ), search in the Apache JIRA
 *
 * </pre>
 *
 * @see javax.jcr.Repository#login
 * @see org.apache.jackrabbit.api.JackrabbitRepository#login
 * @see org.apache.jackrabbit.oak.api.ContentRepository#login(javax.jcr.Credentials, String)
 * @see javax.jcr.Credentials
 * @see javax.jcr.SimpleCredentials
 */
public class L1_IntroductionTest extends AbstractJCRTest {

    private Repository repository;
    private User user;
    private Session testSession;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        repository = getHelper().getRepository();
        user = createTestUser(((JackrabbitSession) superuser).getUserManager());;
        superuser.save();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            if (testSession != null && testSession.isLive()) {
                testSession.logout();
            }
            user.remove();
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    public void testUserLogin() throws RepositoryException {
        testSession = repository.login(getTestCredentials(user.getID()));
    }

    public void testAdminLogin() throws RepositoryException {
        Credentials adminCredentials = getHelper().getSuperuserCredentials();
        testSession = repository.login(adminCredentials);
    }
}