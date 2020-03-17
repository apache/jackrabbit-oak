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

import javax.jcr.RepositoryException;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.security.user.RandomAuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.test.api.util.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * <pre>
 * Module: User Management
 * =============================================================================
 *
 * Title: Authorizable Node Name
 * -----------------------------------------------------------------------------
 *
 * Goal:
 * Become familiar with the {@link org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName}
 * interface and how it is used in the default implementation. After having
 * completed this test you should also be able to write and configure a custom
 * implemenation understanding the impact it will have on the default user
 * management.
 *
 * Exercises:
 *
 * - {@link #testAuthorizableNodeName()}
 *   Test the fallback behaviour as implemented in Oak if no
 *   {@link org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName}
 *   interface is configured.
 *
 * - {@link #testRandomAuthorizableNodeName()}
 *   Same as above but with an {@link org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName}
 *   configured.
 *
 *
 * Advanced Exercises:
 * -----------------------------------------------------------------------------
 *
 * - Create a custom implementation of the {@link org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName}
 *   interface.
 *
 * - Make your implementation a OSGi service as described in the documentation
 *   and deploy it in your Sling (Granite|CQ) repository.
 *   Verify that creating a new user or group actually makes use of your
 *   implementation.
 *
 * </pre>
 *
 * @see org.apache.jackrabbit.oak.spi.security.user.AuthorizableNodeName
 * @see org.apache.jackrabbit.oak.security.user.RandomAuthorizableNodeName
 */
public class L14_AuthorizableNodeNameTest extends AbstractSecurityTest {

    private UserManager userManager;
    private User testUser;

    private AuthorizableNodeName nameGenerator = new RandomAuthorizableNodeName();

    @Override
    public void before() throws Exception {
        super.before();

        userManager = getUserManager(root);
    }

    @Override
    public void after() throws Exception {
        try {
            if (testUser != null) {
                testUser.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        // EXERCISE: un-comment for 'testRandomAuthorizableNodeName'
//        ConfigurationParameters userConfig = ConfigurationParameters.of(UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, nameGenerator);
//        return ConfigurationParameters.of(UserConfiguration.NAME, userConfig);
        return ConfigurationParameters.EMPTY;
    }

    @Test
    public void testAuthorizableNodeName() throws RepositoryException {
        testUser = userManager.createUser("test/:User", null);

        String nodeName = Text.getName(testUser.getPath());
        String expectedNodeName = null; // EXERCISE : fill in the expected value

        assertEquals(expectedNodeName, nodeName);
    }

    @Test
    public void testRandomAuthorizableNodeName() throws RepositoryException {
        // EXERCISE: uncomment the setup in 'getSecurityConfigParameters' before running this test.

        // verify that the configuration is correct:
        AuthorizableNodeName configured = getUserConfiguration().getParameters().getConfigValue(UserConstants.PARAM_AUTHORIZABLE_NODE_NAME, AuthorizableNodeName.DEFAULT);
        assertNotSame(AuthorizableNodeName.DEFAULT, configured);
        assertTrue(configured instanceof RandomAuthorizableNodeName);

        testUser = userManager.createUser("test/:User", null);

        String nodeName = Text.getName(testUser.getPath());

        // EXERCISE: write the correct assertion wrt the generated node name.
    }
}