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

import java.util.Collections;
import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.test.AbstractJCRTest;

/**
 * <pre>
 * Module: Authentication
 * =============================================================================
 *
 * Title: Session.getUserID()
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the semantics of {@link javax.jcr.Session#getUserID()} and the
 * difference to {@link org.apache.jackrabbit.api.security.user.User#getID()}.
 * Understand, why this test is located in the 'Authentication' section instead
 * of the 'UserManagement' section.
 *
 * Exercises:
 *
 * - Read JSR 283 and the JavaDoc of {@link javax.jcr.Session#getUserID()}
 *   Question: What is the defined return value of this method? How does that
 *   relate to {@link org.apache.jackrabbit.api.security.user.User#getID()}?
 *
 * - {@link #testGetUserIDReturnsNull()}
 *   Run the test and explain why {@link javax.jcr.Session#getUserID()} returns
 *   {@code null} after login with the admin credentials.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Identify what the nature of the API contract means for round trips between
 *   {@link javax.jcr.Session#getUserID()} and {@link org.apache.jackrabbit.api.security.user.UserManager#getAuthorizable(String)}.
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L2_AuthInfoTest}
 *
 * </pre>
 *
 * @see javax.jcr.Session#getUserID()
 * @see org.apache.jackrabbit.api.security.user.User#getID()
 * @see org.apache.jackrabbit.oak.api.AuthInfo#getUserID()
 */
public class L4_UserIDTest extends AbstractJCRTest {

    private Repository repository;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        repository = getHelper().getRepository();
    }


    public void testGetUserIDReturnsNull() throws RepositoryException, LoginException {

        // verify first that the admin-ID is not null in the default setup.
        String adminId = superuser.getUserID();
        assertNotNull(adminId);

        // verify userID in SimpleCredentials is not null
        Credentials adminCredentials = getHelper().getSuperuserCredentials();
        assertNotNull(((SimpleCredentials) adminCredentials).getUserID());

        Session adminSession = null;
        try {
            // change the JAAS configuration
            Configuration.setConfiguration(getConfiguration());
            // login again
            adminSession = repository.login(adminCredentials);

            // EXERCISE : explain why the userID of the admin-session is now 'null'
            assertNull(adminSession.getUserID());

        } finally {
            if (adminSession != null && adminSession.isLive()) {
                adminSession.logout();
            }
            Configuration.setConfiguration(null);
        }
    }

    private static Configuration getConfiguration() {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String applicationName) {
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(UserIDTestLoginModule.class.getName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, Collections.EMPTY_MAP)
                };
            }
        };
    }
}