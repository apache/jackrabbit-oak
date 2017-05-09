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
import javax.jcr.RepositoryException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * <pre>
 * Module: Authentication | User Management
 * =============================================================================
 *
 * Title: Anonymous Id
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Understand that the ID of the anonymous user in the system is configurable
 * and thus must not be treated as constant (hardcoding) in an application (Sling|Granite|CQ).
 *
 * Exercises:
 *
 * - {@link #testAnonymousID()}
 *   Login as anonymous again and test the resulting userID
 *   Question: What is the expected value of {@link javax.jcr.Session#getUserID()}
 *   upon guest login? Explain why.
 *
 * - {@link #testDifferentAnonymousID()}
 *   Define the configuration settings that will create you an Oak repository instance
 *   that has a different anonymous ID.
 *
 *
 * Additional Exercise
 * -----------------------------------------------------------------------------
 *
 * In Adobe Granite exists an Osgi service that allows you to retrive the ID
 * of the 'anonymous' user without hardcoding.
 *
 * - Find the service and test how you can obtain the anonymous ID.
 *
 * </pre>
 *
 * @see javax.jcr.GuestCredentials
 */
public class L6_AnonymousIdTest extends AbstractSecurityTest {

    private ContentSession testSession;

    @Override
    public void before() throws Exception {
        super.before();
    }

    @Override
    public void after() throws Exception {
        try {
            if (testSession != null) {
                testSession.close();
            }
        } finally {
            super.after();
        }
    }

    @Test
    public void testAnonymousID() throws RepositoryException, LoginException {
        testSession = login(new GuestCredentials());

        String anonymousID = testSession.getAuthInfo().getUserID();

        // EXERCISE: what value do you expect for 'anonymousID'? explain why. is there a solution without hardcoding?
        String expectedID = null;

        assertEquals(expectedID, anonymousID);
    }

    // NOTE: chang this test-configuration for testDifferentAnonymousID()
    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        // EXERCISE: un-comment for 'testDifferentAnonymousID'
//        ConfigurationParameters userConfig = ConfigurationParameters.of(UserConstants.PARAM_ANONYMOUS_ID, "differentAnonymousId");
//        return ConfigurationParameters.of(UserConfiguration.NAME, userConfig);
        return ConfigurationParameters.EMPTY;
    }

    @Test
    public void testDifferentAnonymousID() throws Exception {
        // EXERCISE : use built-in oak configuration settings to have a different anonymous ID -> uncomment the configuration parameters in 'getSecurityConfigParameters' above

        testSession = login(new GuestCredentials());

        String expectedId = null; // EXERCISE: write the expected ID
        assertEquals(expectedId, testSession.getAuthInfo().getUserID());
    }
}