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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import java.util.Set;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.NamedAccessControlPolicy;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the special {@code ReadPolicy} exposed at specified paths.
 *
 * @since OAK 1.0
 */
public class ReadPolicyTest extends AbstractSecurityTest {

    private Set<String> readPaths;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        ConfigurationParameters options = getConfig(AuthorizationConfiguration.class).getParameters();
        readPaths = options.getConfigValue(PermissionConstants.PARAM_READ_PATHS, PermissionConstants.DEFAULT_READ_PATHS);
    }

    @Test
    public void testGetPolicies() throws Exception {
        for (String path : readPaths) {
            AccessControlPolicy[] policies = getAccessControlManager(root).getPolicies(path);
            assertTrue(policies.length > 0);
            boolean found = false;
            for (AccessControlPolicy policy : policies) {
                if ("org.apache.jackrabbit.oak.security.authorization.accesscontrol.AccessControlManagerImpl$ReadPolicy".equals(policy.getClass().getName())) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        for (String path : readPaths) {
            AccessControlPolicy[] policies = getAccessControlManager(root).getEffectivePolicies(path);
            assertTrue(policies.length > 0);
            boolean found = false;
            for (AccessControlPolicy policy : policies) {
                if ("org.apache.jackrabbit.oak.security.authorization.accesscontrol.AccessControlManagerImpl$ReadPolicy".equals(policy.getClass().getName())) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }

    @Test
    public void testGetName() throws Exception {
        AccessControlPolicy[] policies = getAccessControlManager(root).getPolicies(readPaths.iterator().next());
        assertEquals(1, policies.length);
        assertTrue(policies[0] instanceof NamedAccessControlPolicy);
        assertNotNull(((NamedAccessControlPolicy) policies[0]).getName());
    }

}