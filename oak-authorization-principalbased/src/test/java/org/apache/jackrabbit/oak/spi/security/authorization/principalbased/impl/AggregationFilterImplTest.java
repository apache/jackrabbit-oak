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

import org.apache.jackrabbit.api.security.JackrabbitAccessControlManager;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.security.AccessControlManager;
import java.security.Principal;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class AggregationFilterImplTest extends AbstractPrincipalBasedTest{

    private AggregationFilterImpl aggregationFilter;
    private Set<Principal> systemUserPrincipals;
    private Set<Principal> testUserPrincipals;

    @Before
    public void before() throws Exception {
        super.before();

        aggregationFilter = new AggregationFilterImpl();
        systemUserPrincipals = Set.of(getTestSystemUser().getPrincipal());
        testUserPrincipals = Set.of(getTestUser().getPrincipal(), EveryonePrincipal.getInstance());
    }

    @Test
    public void testStopPermissionProviderTrue() {
        assertTrue(aggregationFilter.stop(createPermissionProvider(root, systemUserPrincipals.toArray(new Principal[0])), systemUserPrincipals));
    }

    @Test
    public void testStopPermissionProviderFalse() {
        assertFalse(aggregationFilter.stop(mock(AggregatedPermissionProvider.class), systemUserPrincipals));
        assertFalse(aggregationFilter.stop(mock(AggregatedPermissionProvider.class), testUserPrincipals));

        PermissionProvider pp = getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, adminSession.getWorkspaceName(), systemUserPrincipals);
        if (pp instanceof AggregatedPermissionProvider) {
            assertFalse(aggregationFilter.stop((AggregatedPermissionProvider) pp, systemUserPrincipals));
        }

        pp = getConfig(AuthorizationConfiguration.class).getPermissionProvider(root, adminSession.getWorkspaceName(), testUserPrincipals);
        if (pp instanceof AggregatedPermissionProvider) {
            assertFalse(aggregationFilter.stop((AggregatedPermissionProvider) pp, testUserPrincipals));
        }
    }

    @Test
    public void testStopAcMgrPrincipalsTrue() {
        assertTrue(aggregationFilter.stop(createAccessControlManager(root), systemUserPrincipals));
    }

    @Test
    public void testStopAcMgrPrincipalsFalse() {
        assertFalse(aggregationFilter.stop(mock(JackrabbitAccessControlManager.class), systemUserPrincipals));
        assertFalse(aggregationFilter.stop(mock(JackrabbitAccessControlManager.class), testUserPrincipals));

        assertFalse(aggregationFilter.stop(createAccessControlManager(root), testUserPrincipals));

        AccessControlManager acMgr = getConfig(AuthorizationConfiguration.class).getAccessControlManager(root, getNamePathMapper());
        if (acMgr instanceof JackrabbitAccessControlManager) {
            assertFalse(aggregationFilter.stop((JackrabbitAccessControlManager) acMgr, systemUserPrincipals));
        }
    }

    @Test
    public void testStopAcMgrPrincipalsInvalid() {
        assertFalse(aggregationFilter.stop(createAccessControlManager(root), Set.of(new PrincipalImpl("invalid"))));
    }

    @Test
    public void testStopAcMgrPath() {
        assertFalse(aggregationFilter.stop(createAccessControlManager(root), PathUtils.ROOT_PATH));
        assertFalse(aggregationFilter.stop(createAccessControlManager(root), SUPPORTED_PATH));
        assertFalse(aggregationFilter.stop(getConfig(AuthorizationConfiguration.class).getAccessControlManager(root, getNamePathMapper()), SUPPORTED_PATH));
    }
}