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

import javax.jcr.GuestCredentials;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * <pre>
 * Module: User Management (Authentication)
 * =============================================================================
 *
 * Title: Repository without Anonymous
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand how the repository can be setup without having an anonymous user
 * and how this affects the guest login.
 *
 * Exercises:
 *
 * - {@link #testAnonymousLogin()}
 *   Use the test to setup a new Oak repository that doesn't allow any anonymous access
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.oak.security.user.UserInitializer
 * @see org.apache.jackrabbit.oak.spi.security.user.UserConstants#PARAM_ANONYMOUS_ID
 */
public class L15_RepositoryWithoutAnonymousTest extends AbstractSecurityTest {

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        // EXERCISE : use the built-in oak configuration settings to prevent anonymous access altogether
        ConfigurationParameters userConfig = ConfigurationParameters.EMPTY;

        return ConfigurationParameters.of(UserConfiguration.NAME, userConfig);
    }

    @Test
    public void testAnonymousLogin() throws Exception {
        ContentSession oakSession = null;
        try {
            oakSession = login(new GuestCredentials());
            fail("Anonymous login must fail.");
        } catch (javax.security.auth.login.LoginException e) {
            // success
        } finally {
            if (oakSession != null) {
                oakSession.close();
            }
        }
    }

}