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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.junit.Test;

/**
 * <pre>
 * Module: Authentication
 * =============================================================================
 *
 * Title: Pre-Authentication with LoginModule Chain
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand how a pre-authentication can be used in combination with the
 * {@link LoginModule} chain according to the description provided in
 * http://jackrabbit.apache.org/oak/docs/security/authentication/preauthentication.html
 *
 * Exercises:
 *
 * - {@link #testPreAuthenticatedLogin()}
 *   Modify the {@link CustomLoginModule}
 *   such that the simplified pre-auth in the test-case passes.
 *
 * - With the same setup at hand explain why the {@code CustomCredentials} must
 *   be package protected. Come up with a vulnerability/exploit if this credentials
 *   implemenation was exposed to the public.
 *
 *
 * Additional Exercises:
 * -----------------------------------------------------------------------------
 *
 * In a Sling base repository installation (Granite|CQ) make use of your
 * understanding of pre-authentication with LoginModule chain involvement
 * and defined a dedicated bundle that comes with a package that contains the
 * following classes
 *
 * - A Credentials implemenation that is package private and cannot be abused
 *   outside of the scope of this bundle.
 * - Sling AuthenticationHandler implementation that performs the pre-auth and
 *   passes the package private Credentials to the repository login
 * - LoginModule implementation (that receives the package private Credentials
 *   and updates the shared state accordingly).
 *
 *
 * Related Exercises:
 * -----------------------------------------------------------------------------
 *
 * - {@link L3_LoginModuleTest}
 * - {@link L9_NullLoginTest}
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin
 * @see <a href="http://jackrabbit.apache.org/oak/docs/security/authentication/preauthentication.html">Pre-Authentication Documentation</a>
 */
public class L8_PreAuthTest extends AbstractSecurityTest {

    @Override
    protected Configuration getConfiguration() {
        final ConfigurationParameters config = getSecurityConfigParameters();
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String applicationName) {
                Map<String, ?> options = getSecurityConfigParameters().getConfigValue(applicationName, Collections.<String, Object>emptyMap());
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry(CustomLoginModule.class.getName(), AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL, options),
                        new AppConfigurationEntry(LoginModuleImpl.class.getName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)};
            }
        };
    }

    @Test
    public void testPreAuthenticatedLogin() throws IOException, LoginException, NoSuchWorkspaceException {
        // EXERCISE: adjust the CustomLoginModule such that the following test passes, the jaas configuration has already been adjusted for you above.

        // login as admin with CustomCredentials and without a password
        // -> no password verification in the module required as this is expected
        //    to have already happened during the pre-auth setp (which is missing here)
        String loginID = getUserConfiguration().getParameters().getConfigValue(UserConstants.DEFAULT_ADMIN_ID, UserConstants.DEFAULT_ADMIN_ID);
        ContentSession contentSession = login(new CustomCredentials(loginID, null, Collections.EMPTY_MAP));

        // EXERCISE: add verification of the AuthInfo according to your implementation of the custom login module.

        contentSession.close();
    }
}