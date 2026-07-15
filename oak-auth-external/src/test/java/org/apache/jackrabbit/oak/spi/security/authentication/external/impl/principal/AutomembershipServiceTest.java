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

import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_AUTO_MEMBERSHIP;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_IDP_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AutomembershipServiceTest extends AbstractAutoMembershipTest {
    
    private SyncHandlerMappingTracker mappingTracker;
    private AutomembershipService service;
    private SyncConfigTracker scTracker;
    
    @Before
    public void before() throws Exception {
        super.before();

        mappingTracker = new SyncHandlerMappingTracker(context.bundleContext());
        mappingTracker.open();
        
        scTracker = new SyncConfigTracker(context.bundleContext(), mappingTracker);
        scTracker.open();
        
        service = new AutomembershipService(scTracker);
        
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
    
    private static Map<String, Object> getSyncHandlerParams() {
        return Map.of(
                PARAM_USER_DYNAMIC_MEMBERSHIP, true,
                PARAM_NAME, "sh",
                PARAM_USER_AUTO_MEMBERSHIP, new String[] {AUTOMEMBERSHIP_GROUP_ID_1});
    }
    
    @Test
    public void testMissingDynamicMembership() {
        assertSame(DynamicMembershipProvider.EMPTY, service.getDynamicMembershipProvider(root, userManager, getNamePathMapper()));
    }

    @Test
    public void testDynamicMembership() {
        context.registerService(SyncHandler.class, new DefaultSyncHandler(), getSyncHandlerParams());
        assertTrue(scTracker.isEnabled());
        
        context.registerService(SyncHandlerMapping.class, new SyncHandlerMapping() {}, getMappingParams());
        assertTrue(service.getDynamicMembershipProvider(root, userManager, getNamePathMapper()) instanceof AutoMembershipProvider);
    }
    
    @Test
    public void testRegistered() {
        ExternalPrincipalConfiguration pc = new ExternalPrincipalConfiguration();
        context.registerInjectActivateService(pc);

        DynamicMembershipService s = context.getService(DynamicMembershipService.class);
        assertNotNull(s);
        assertTrue(s instanceof AutomembershipService);

        assertSame(DynamicMembershipProvider.EMPTY, service.getDynamicMembershipProvider(root, userManager, getNamePathMapper()));
        
        context.registerService(SyncHandlerMapping.class, new SyncHandlerMapping() {}, getMappingParams());
        assertSame(DynamicMembershipProvider.EMPTY, service.getDynamicMembershipProvider(root, userManager, getNamePathMapper()));

        context.registerService(SyncHandler.class, new DefaultSyncHandler(), getSyncHandlerParams());
        assertTrue(service.getDynamicMembershipProvider(root, userManager, getNamePathMapper()) instanceof AutoMembershipProvider);
    }
}