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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.NamedAccessControlPolicy;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ReadPolicy;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.jackrabbit.oak.security.authorization.accesscontrol.AbstractAccessControlTest.assertPolicies;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the special {@code ReadPolicy} exposed at specified paths.
 *
 * @since OAK 1.0
 */
@RunWith(Parameterized.class)
public class ReadPolicyTest extends AbstractSecurityTest {

    private final Set<String> configuredPaths;

    private Set<String> readPaths;
    private Set<String> subTreePaths = new HashSet<>();

    @Parameterized.Parameters(name = "Configured Readable Paths = {1}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] {null , "null => default set"},
                new Object[] {Collections.emptySet(), "empty set"},
                new Object[] {Collections.emptySet(), "/"+JcrConstants.JCR_SYSTEM}
        );
    }

    public ReadPolicyTest(@Nullable Set<String> configuredPaths, @NotNull String name) {
        this.configuredPaths = configuredPaths;
    }
    
    @Override
    @Before
    public void before() throws Exception {
        super.before();

        ConfigurationParameters options = getConfig(AuthorizationConfiguration.class).getParameters();
        readPaths = options.getConfigValue(PermissionConstants.PARAM_READ_PATHS, PermissionConstants.DEFAULT_READ_PATHS);

        for (String p : readPaths) {
            Tree t = root.getTree(p);
            Iterator<Tree> children = t.getChildren().iterator();
            if (children.hasNext()) {
                subTreePaths.add(children.next().getPath());
            }
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        if (configuredPaths != null) {
            ConfigurationParameters params = ConfigurationParameters.of(PermissionConstants.PARAM_READ_PATHS, configuredPaths);
            return ConfigurationParameters.of(AuthorizationConfiguration.NAME, params);
        } else {
            return super.getSecurityConfigParameters();
        }
    }

    @Test
    public void testGetPolicies() throws Exception {
        for (String path : readPaths) {
            AccessControlPolicy[] policies = getAccessControlManager(root).getPolicies(path);
            assertTrue(policies.length > 0);
            boolean found = false;
            for (AccessControlPolicy policy : policies) {
                if (policy instanceof ReadPolicy) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }

    @Test
    public void tsetGetPoliciesInSubtrees() throws Exception {
        for (String path : subTreePaths) {
            for (AccessControlPolicy policy : getAccessControlManager(root).getPolicies(path)) {
                if (policy instanceof ReadPolicy) {
                    fail("ReadPolicy must only be bound to configured path.");
                }
            }
        }
    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        for (String path : readPaths) {
            AccessControlPolicy[] policies = getAccessControlManager(root).getEffectivePolicies(path);
            boolean found = false;
            for (AccessControlPolicy policy : policies) {
                if (policy instanceof ReadPolicy) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }

    @Test
    public void testGetEffectivePoliciesInSubTrees() throws Exception {
        for (String path : subTreePaths) {
            AccessControlPolicy[] policies = getAccessControlManager(root).getEffectivePolicies(path);
            boolean found = false;
            for (AccessControlPolicy policy : policies) {
                if (policy instanceof ReadPolicy) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
    }
    
    @Test
    public void testGetEffectivePoliciesPrincipalSet() throws Exception {
        AccessControlPolicy[] policies = getAccessControlManager(root).getEffectivePolicies(Collections.singleton(EveryonePrincipal.getInstance()));
        long expSize = (readPaths.isEmpty()) ? 0 : 1;
        assertPolicies(policies, expSize, true);
    }

    @Test
    public void testGetName() throws Exception {
        if (!readPaths.isEmpty()) {
            AccessControlPolicy[] policies = getAccessControlManager(root).getPolicies(readPaths.iterator().next());
            assertEquals(1, policies.length);
            assertTrue(policies[0] instanceof NamedAccessControlPolicy);
            assertNotNull(((NamedAccessControlPolicy) policies[0]).getName());
        }
    }

}