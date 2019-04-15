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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.SystemUserPrincipal;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.security.AccessControlPolicy;

import static org.junit.Assert.assertEquals;

public class UnknownPrincipalIgnoreTest extends AbstractPrincipalBasedTest {

    private PrincipalBasedAccessControlManager acMgr;

    @Before
    public void before() throws Exception {
        super.before();
        acMgr = createAccessControlManager(root);
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(AuthorizationConfiguration.NAME,
                ConfigurationParameters.of(
                        ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, ImportBehavior.NAME_IGNORE)
        );
    }

    @Test
    public void testGetApplicablePolicies() throws Exception {
        AccessControlPolicy[] applicable = acMgr.getApplicablePolicies((SystemUserPrincipal) () -> "unknown");
        assertEquals(0, applicable.length);
    }

    @Test
    public void testGetPolicies() throws Exception {
        AccessControlPolicy[] policies = acMgr.getPolicies((AdminPrincipal) () -> "unknown");
        assertEquals(0, policies.length);
    }

    @Test
    public void testGetEffectivePolicies() throws Exception {
        AccessControlPolicy[] policies = acMgr.getEffectivePolicies(ImmutableSet.of(getTestSystemUser().getPrincipal(), new PrincipalImpl("unknown")));
        assertEquals(0, policies.length);
    }
}