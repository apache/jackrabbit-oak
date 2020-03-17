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
import javax.jcr.GuestCredentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.RepositoryException;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.junit.Test;

/**
 * <pre>
 * Module: Authentication
 * =============================================================================
 *
 * Title: LoginModule
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand the role of {@link javax.security.auth.spi.LoginModule}s in the Oak
 * authentication setup, the way multiple login modules can be configured (both
 * in OSGi and Java based setups) and how they interact.
 *
 * Exercises:
 *
 * - Overview
 *   Search the Oak code base for implementations of {@link javax.security.auth.spi.LoginModule}
 *   and describe their behaviour|intention and the interactions they may have
 *   when combined in a certain order.
 *
 * - {@link #testLogin()}
 *   Learn how different login modules interact by modifing the JAAS setup.
 *   In this test-scenario this can easily be achieved by overriding the
 *   {@link #getConfiguration()} method.
 *   Change the JAAS configuration and use the {@link #testLogin()} method to
 *   walk through the login. For example
 *   > look at ConfigurationUtil for various options
 *   > manually create a different configuration with different control flags
 *   > create a configuration that also includes the {@link CustomLoginModule}
 *   Discuss your findings
 *
 *
 * Additional Exercises
 * -----------------------------------------------------------------------------
 *
 * In an OSGi base setup like Sling (i.e. Granite|CQ) you can perform the
 * following exercises to deepen your understanding of the {@code LoginModule}
 * mechanism.
 *
 * - Instead of modifying the JAAS configuration in the Java code (or a jaas
 *   configuration file) use the system console to change the order and control
 *   flag of the various login modules.
 *   Same as {@link #testLogin()} but with configuration changed in OSGi.
 *
 *
 * Advanced Exercises
 * -----------------------------------------------------------------------------
 *
 * Use the {@link CustomLoginModule}
 * stub to make advanced exercises wrt {@link javax.security.auth.spi.LoginModule}:
 *
 * - {@link #testCustomCredentialsLogin}
 *   Adjust the JAAS configuration and complete the
 *   {@link CustomLoginModule}
 *   such that the test passes; i.e. that you can perform a successful login with
 *   {@link CustomCredentials}.
 *
 *   Play with the {@link javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag}
 *   in the configuration such that a successful login with the custom module succeeds.
 *
 *   Alternatively you could for example map the a given loginID to a particular
 *   user in the repository and use the shared state of the login modules to pass
 *   around credentials, login name etc.
 *
 *
 * Related Exercises
 * -----------------------------------------------------------------------------
 *
 * - {@link L8_PreAuthTest}
 * - {@link L9_NullLoginTest}
 *
 * </pre>
 *
 * @see javax.security.auth.spi.LoginModule
 * @see javax.security.auth.login.Configuration
 */
public class L3_LoginModuleTest extends AbstractSecurityTest {


    @Override
    protected Configuration getConfiguration() {
        // EXERCISE: modify the JAAS configuration
        // EXERCISE: - look at ConfigurationUtil for various options
        // EXERCISE: - manually create a different configuration with different control flags
        // EXERCISE: - create a configuration that also includes the {@link CustomLoginModule}
        return super.getConfiguration();
    }

    @Test
    public void testLogin() throws LoginException, NoSuchWorkspaceException, IOException {
        ContentSession contentSession = login(new GuestCredentials());
        contentSession.close();
    }

    @Test
    public void testCustomCredentialsLogin() throws LoginException, RepositoryException, IOException {
        String loginID = null; // EXERCISE
        String pw = null;      // EXERCISE
        ContentSession contentSession = login(new CustomCredentials(loginID, pw, Collections.EMPTY_MAP));

        // EXERCISE: add verification of the AuthInfo according to your implementation of the custom login module.

        contentSession.close();
    }
}