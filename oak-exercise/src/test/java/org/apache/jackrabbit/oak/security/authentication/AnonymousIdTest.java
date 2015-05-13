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

import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.AbstractJCRTest;

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
 *   Define the configuration settings that will create you a JCR repository instance
 *   that has a different anonymous ID.
 *
 * </pre>
 *
 * @see javax.jcr.GuestCredentials
 */
public class AnonymousIdTest extends AbstractJCRTest {

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

    public void testAnonymousID() throws RepositoryException {
        testSession = repository.login(new GuestCredentials());

        String anonymousID = testSession.getUserID();

        // TODO: what value do you expect for 'anonymousID'? explain why.
        String expectedID = null; //FIXME : fill-in value without hardcoding
        assertEquals(expectedID, anonymousID);
    }

    public void testDifferentAnonymousID() throws Exception {
        String anonymousId = "differentAnonymousId";

        // TODO : use built-in oak configuration settings to have a different anonymous ID.
        ConfigurationParameters userConfigParams = ConfigurationParameters.EMPTY; // FIXME : define the configuration

        // create a new JCR repository with the given setup
        Jcr jcr = new Jcr().with(new SecurityProviderImpl(ConfigurationParameters.of(UserConfiguration.NAME, userConfigParams)));

        Repository jcrRepository = jcr.createRepository();
        Session guestSession = jcrRepository.login(new GuestCredentials(), null);

        assertEquals(anonymousId, guestSession.getUserID());
        guestSession.logout();
    }
}