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
package org.apache.jackrabbit.oak.exercise.security.principal;

import java.security.Principal;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.exercise.security.principal.CustomPrincipalConfiguration;
import org.apache.jackrabbit.oak.exercise.security.principal.CustomPrincipalProvider;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Test;

/**
 * <pre>
 * Module: Principal Management
 * =============================================================================
 *
 * Title: Principal Provider
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Get familiar with the {@link org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider} interface.
 *
 * Exercises:
 *
 * - {@link #testCorrespondance()}
 *   List the corresponding calls between {@link PrincipalManager} and {@link PrincipalProvider}.
 *   List also those methods that have no correspondance in either interface.
 *   Try to identify the reason for having a JCR-level manager and an Oak-level provider interface.
 *
 *
 * Additional Exercises
 * -----------------------------------------------------------------------------
 *
 * - Take a closer look at the {@link org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration}
 *   and the available implementations.
 *
 *   Question: Can you identify how they are used and how principal management can
 *             be extended both in an OSGi-based and regular java setup?
 *
 *
 * Advanced Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Complete the {@link CustomPrincipalProvider}
 *   stub and deploy the exercise bundle in a Sling base repository installation
 *   (e.g. Cq|Granite).
 *   > Try to identify the tools that allow you to explore your custom principals
 *   > Play with the dynamic group membership as you define it in the principal provider and verify that the subjects calculated upon login are correct
 *   > Play with the authorization part of the principal management granting/revoking access for one of your custom principals
 * </pre>
 *
 * @see org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider
 * @see CustomPrincipalProvider
 * @see CustomPrincipalConfiguration
 */
public class L4_PrincipalProviderTest extends AbstractSecurityTest {

    private PrincipalProvider principalProvider;
    private PrincipalManager principalManager;

    private String testPrincipalName;

    @Override
    public void before() throws Exception {
        super.before();

        principalProvider = getConfig(PrincipalConfiguration.class).getPrincipalProvider(root, NamePathMapper.DEFAULT);
        principalManager = getConfig(PrincipalConfiguration.class).getPrincipalManager(root, NamePathMapper.DEFAULT);

        // NOTE: this method call doesn't make to much sense outside of a
        // simple test with a very limited number of principals (!!)
        PrincipalIterator principalIterator = principalManager.getPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        if (principalIterator.hasNext()) {
            testPrincipalName = principalIterator.nextPrincipal().getName();
        }

        if (testPrincipalName == null) {
            throw new NotExecutableException();
        }
    }

    @Override
    public void after() throws Exception {
        super.after();
    }

    @Test
    public void testCorrespondance() {
        boolean exists = principalManager.hasPrincipal(testPrincipalName);
        Principal principal = principalManager.getPrincipal(testPrincipalName);
        PrincipalIterator principalIterator = principalManager.findPrincipals(testPrincipalName, PrincipalManager.SEARCH_TYPE_ALL);
        PrincipalIterator groups = principalManager.getGroupMembership(principal);
        PrincipalIterator all = principalManager.getPrincipals(PrincipalManager.SEARCH_TYPE_ALL);

        // EXERCISE: write the corresponding calls for the principal provider and verify the expected result
        // EXERCISE: which methods have nor corresponding call in the other interface?
    }

}