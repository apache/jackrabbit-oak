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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_GROUP_DYNAMIC_GROUPS;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_IDP_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal.AbstractAutoMembershipTest.IDP_VALID_AM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DynamicGroupMembershipServiceTest extends AbstractExternalAuthTest {

    private SyncHandlerMappingTracker mappingTracker;
    private SyncConfigTracker scTracker;
    private DynamicGroupMembershipService service;

    @Before
    public void before() throws Exception {
        super.before();

        mappingTracker = new SyncHandlerMappingTracker(context.bundleContext());
        mappingTracker.open();

        scTracker = new SyncConfigTracker(context.bundleContext(), mappingTracker);
        scTracker.open();

        service = new DynamicGroupMembershipService(scTracker);

        assertFalse(scTracker.isEnabled());
    }

    @After
    public void after() throws Exception {
        try {
            mappingTracker.close();
            scTracker.close();
        } finally {
            super.after();
        }
    }
    
    private static Map<String, String> getMappingParams() {
        return Map.of(PARAM_IDP_NAME, IDP_VALID_AM, PARAM_SYNC_HANDLER_NAME, "sh");
    }

    private static Map<String, Object> getSyncHandlerParams(boolean enableDynamicGroups) {
        return Map.of(
                PARAM_USER_DYNAMIC_MEMBERSHIP, true,
                PARAM_NAME, "sh",
                PARAM_GROUP_DYNAMIC_GROUPS, enableDynamicGroups);
    }
    
    @Test
    public void testNotEnabled() {
        assertSame(DynamicMembershipProvider.EMPTY, service.getDynamicMembershipProvider(root, getUserManager(root), getNamePathMapper()));
    }

    @Test
    public void testDynamicMembershipNoDynamicGroups() {
        context.registerService(SyncHandler.class, new DefaultSyncHandler(), getSyncHandlerParams(false));
        assertTrue(scTracker.isEnabled());

        context.registerService(SyncHandlerMapping.class, new SyncHandlerMapping() {}, getMappingParams());

        assertFalse(scTracker.hasDynamicGroupsEnabled());
        assertTrue(scTracker.getIdpNamesWithDynamicGroups().isEmpty());
        
        assertSame(DynamicMembershipProvider.EMPTY, service.getDynamicMembershipProvider(root, getUserManager(root), getNamePathMapper()));
    }

    @Test
    public void testDynamicMembershipAndDynamicGroups() {
        context.registerService(SyncHandler.class, new DefaultSyncHandler(), getSyncHandlerParams(true));
        assertTrue(scTracker.isEnabled());

        context.registerService(SyncHandlerMapping.class, new SyncHandlerMapping() {}, getMappingParams());

        assertTrue(scTracker.hasDynamicGroupsEnabled());
        assertEquals(Collections.singleton(IDP_VALID_AM), scTracker.getIdpNamesWithDynamicGroups());
        
        DynamicMembershipProvider dmp = service.getDynamicMembershipProvider(root, getUserManager(root), getNamePathMapper());
        assertNotSame(DynamicMembershipProvider.EMPTY, dmp);
        assertTrue(dmp instanceof ExternalGroupPrincipalProvider);
    }
}