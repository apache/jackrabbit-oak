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

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.ObjectArrays;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipAware;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping;
import org.apache.sling.testing.mock.osgi.MapUtil;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_GROUP_AUTO_MEMBERSHIP;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_AUTO_MEMBERSHIP;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_IDP_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class SyncConfigTrackerTest {

    @Rule
    public final OsgiContext context = new OsgiContext();
    
    private final SyncHandler service = mock(SyncHandler.class);
    private final SyncHandlerMapping mapping = mock(SyncHandlerMapping.class);

    private final String[] gam = new String[] {"g1", "g2"};
    private final String[] uam = new String[] {"g3", "g4"};
    
    private SyncHandlerMappingTracker mappingTracker;
    private SyncConfigTracker tracker;

    @Before
    public void before() {
        mappingTracker = new SyncHandlerMappingTracker(context.bundleContext());
        mappingTracker.open();
        
        tracker = new SyncConfigTracker(context.bundleContext(), mappingTracker);
        tracker.open();
        assertFalse(tracker.isEnabled());
    }
    
    @After
    public void after() {
        mappingTracker.close();
        tracker.close();
    }

    private void registerSyncHandlerMapping(@NotNull String idpName, @NotNull String syncHandlerName) {
        context.registerService(SyncHandlerMapping.class, mapping, Map.of(PARAM_IDP_NAME, idpName, PARAM_SYNC_HANDLER_NAME, syncHandlerName));
    }

    private void registerSyncHandlerWithAutoMembership(@NotNull String syncHandlerName, boolean dynamicMembership, @NotNull String[] userAutoMembership, @NotNull String[] groupAutoMembership) {
        context.registerService(SyncHandler.class, service, Map.of(
                PARAM_NAME, syncHandlerName,
                PARAM_USER_DYNAMIC_MEMBERSHIP, dynamicMembership,
                PARAM_GROUP_AUTO_MEMBERSHIP, userAutoMembership,
                PARAM_USER_AUTO_MEMBERSHIP, groupAutoMembership));
    }

    @Test
    public void testAddingServiceWithoutProps() {
        context.registerService(SyncHandler.class, service, Collections.emptyMap());
        assertFalse(tracker.isEnabled());
    }

    @Test
    public void testAddingServiceWithProperties() {
        context.registerService(SyncHandler.class, service, Collections.singletonMap(PARAM_USER_DYNAMIC_MEMBERSHIP, false));
        assertFalse(tracker.isEnabled());

        context.registerService(SyncHandler.class, service, Collections.singletonMap(PARAM_USER_DYNAMIC_MEMBERSHIP, true));
        assertTrue(tracker.isEnabled());
    }

    @Test
    public void testAddingMultipleServices() {
        context.registerService(SyncHandler.class, service, Collections.singletonMap(PARAM_USER_DYNAMIC_MEMBERSHIP, true));
        assertTrue(tracker.isEnabled());

        context.registerService(SyncHandler.class, mock(SyncHandler.class), Collections.singletonMap(PARAM_USER_DYNAMIC_MEMBERSHIP, false));
        assertTrue(tracker.isEnabled());
    }

    @Test
    public void testModifiedServiceWithoutProps() {
        context.registerService(SyncHandler.class, service, Collections.emptyMap());
        ServiceReference ref = context.bundleContext().getServiceReference(SyncHandler.class.getName());
        tracker.modifiedService(ref, service);
        assertFalse(tracker.isEnabled());
    }

    @Test
    public void testRemovedService() {
        ServiceRegistration registration = context.bundleContext().registerService(SyncHandler.class.getName(), service, MapUtil.toDictionary(Collections.singletonMap(PARAM_USER_DYNAMIC_MEMBERSHIP, true)));
        assertTrue(tracker.isEnabled());

        registration.unregister();
        assertFalse(tracker.isEnabled());
    }

    @Test
    public void testGetAutoMembership() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        registerSyncHandlerMapping("idp", "sh");
        registerSyncHandlerWithAutoMembership("sh", true, uam, gam);

        Map<String,String[]> automembership = tracker.getAutoMembership();
        assertEquals(1, automembership.size());
        Set<String> expected = ImmutableSet.copyOf(ObjectArrays.concat(uam, gam, String.class));
        assertEquals(expected, ImmutableSet.copyOf(automembership.get("idp")));
    }

    @Test
    public void testGetAutoMembershipDynamicDisabled() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        registerSyncHandlerMapping("idp", "sh");
        registerSyncHandlerWithAutoMembership("sh", false, uam, gam);

        assertTrue(tracker.getAutoMembership().isEmpty());
    }

    @Test
    public void testGetAutoMembershipMissingSyncHandlerMapping() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        registerSyncHandlerWithAutoMembership("sh", true, uam, gam);

        assertTrue(tracker.getAutoMembership().isEmpty());
    }

    @Test
    public void testGetAutoMembershipNoMatchingSyncHandlerName() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        registerSyncHandlerMapping("idp", "differentSH");
        registerSyncHandlerWithAutoMembership("sh", true, uam, gam);

        assertTrue(tracker.getAutoMembership().isEmpty());
    }

    @Test
    public void testGetAutoMembershipWithMultipleIDPs() {
        assertTrue(tracker.getAutoMembership().isEmpty());
        
        registerSyncHandlerWithAutoMembership("sh", true, uam, new String[0]);

        registerSyncHandlerMapping("idp", "sh");
        registerSyncHandlerMapping("idp2", "sh");

        Map<String,String[]> automembership = tracker.getAutoMembership();
        assertEquals(2, automembership.size());
        assertArrayEquals(uam, automembership.get("idp"));
        assertArrayEquals(uam, automembership.get("idp2"));
    }

    @Test
    public void testGetAutoMembershipMultipleHandlersAndIdps() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        registerSyncHandlerWithAutoMembership("sh",true, uam, gam);
        registerSyncHandlerWithAutoMembership("sh2",true, uam, new String[0]);

        registerSyncHandlerMapping("idp", "sh");
        registerSyncHandlerMapping("idp2", "sh2");
        
        Map<String,String[]> automembership = tracker.getAutoMembership();
        assertEquals(2, automembership.size());
        Set<String> expected = ImmutableSet.copyOf(ObjectArrays.concat(uam, gam, String.class));
        assertEquals(expected, ImmutableSet.copyOf(automembership.get("idp")));
        assertArrayEquals(uam, automembership.get("idp2"));
    }

    @Test
    public void testGetAutoMembershipWithCollision() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        registerSyncHandlerWithAutoMembership("sh", true, new String[0], gam);
        registerSyncHandlerWithAutoMembership("sh", true, uam, new String[0]);

        registerSyncHandlerMapping("idp", "sh");

        Map<String,String[]> automembership = tracker.getAutoMembership();
        assertEquals(1, automembership.size());
    }

    @Test
    public void testGetAutoMembershipWithDuplication() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        registerSyncHandlerWithAutoMembership("sh", true, new String[0], gam);
        registerSyncHandlerWithAutoMembership("sh", true, new String[0], gam);

        registerSyncHandlerMapping("idp", "sh");
        
        Map<String,String[]> automembership = tracker.getAutoMembership();
        assertEquals(1, automembership.size());
        assertArrayEquals(gam, automembership.get("idp"));
    }
    
    @Test
    public void testNotAutoMembershipAware() {
        assertTrue(tracker.getAutoMembershipConfig().isEmpty());
        
        context.registerService(SyncHandlerMapping.class, mapping, Map.of(PARAM_IDP_NAME, "idp", PARAM_SYNC_HANDLER_NAME, "sh"));
        // sync-handler mock is not AutoMembershipAware
        context.registerService(SyncHandler.class, service, Map.of(PARAM_NAME, "sh", PARAM_USER_DYNAMIC_MEMBERSHIP, true));
        
        assertTrue(tracker.isEnabled());
        assertTrue(tracker.getAutoMembershipConfig().isEmpty());
    }

    @Test
    public void testAutomembershipAware() {
        assertTrue(tracker.getAutoMembershipConfig().isEmpty());
        
        context.registerService(SyncHandlerMapping.class, mapping, Map.of(PARAM_IDP_NAME, "idp", PARAM_SYNC_HANDLER_NAME, "sh"));
        // sync-handler mock is AutoMembershipAware
        SyncHandler syncHandler = mock(SyncHandler.class, withSettings().extraInterfaces(AutoMembershipAware.class));
        AutoMembershipConfig amc = mock(AutoMembershipConfig.class);
        when(((AutoMembershipAware) syncHandler).getAutoMembershipConfig()).thenReturn(amc);
        context.registerService(SyncHandler.class, syncHandler, Map.of(PARAM_NAME, "sh", PARAM_USER_DYNAMIC_MEMBERSHIP, true));
        
        assertTrue(tracker.isEnabled());
        Map<String, AutoMembershipConfig> m = tracker.getAutoMembershipConfig();
        assertEquals(1, m.size());
        assertTrue(m.containsKey("idp"));
        assertEquals(amc, m.get("idp"));
    }

    @Test
    public void testAutomembershipAwareWithDuplication() {
        assertTrue(tracker.getAutoMembershipConfig().isEmpty());

        context.registerService(SyncHandlerMapping.class, mapping, Map.of(PARAM_IDP_NAME, "idp", PARAM_SYNC_HANDLER_NAME, "sh"));
        // sync-handler mock is AutoMembershipAware
        SyncHandler syncHandler = mock(SyncHandler.class, withSettings().extraInterfaces(AutoMembershipAware.class));
        AutoMembershipConfig amc = mock(AutoMembershipConfig.class);
        when(((AutoMembershipAware) syncHandler).getAutoMembershipConfig()).thenReturn(amc);
        context.registerService(SyncHandler.class, syncHandler, Map.of(PARAM_NAME, "sh", PARAM_USER_DYNAMIC_MEMBERSHIP, true));

        // duplicate registration
        context.registerService(SyncHandler.class, syncHandler, Map.of(PARAM_NAME, "sh", PARAM_USER_DYNAMIC_MEMBERSHIP, true));

        assertTrue(tracker.isEnabled());
        Map<String, AutoMembershipConfig> m = tracker.getAutoMembershipConfig();
        assertEquals(1, m.size());
        assertTrue(m.containsKey("idp"));
        assertEquals(amc, m.get("idp"));
    }

    @Test
    public void testAutomembershipAwareWithCollision() {
        assertTrue(tracker.getAutoMembershipConfig().isEmpty());
        
        context.registerService(SyncHandlerMapping.class, mapping, Map.of(PARAM_IDP_NAME, "idp", PARAM_SYNC_HANDLER_NAME, "sh"));
        // sync-handler mock is AutoMembershipAware
        SyncHandler syncHandler = mock(SyncHandler.class, withSettings().extraInterfaces(AutoMembershipAware.class));
        AutoMembershipConfig amc = mock(AutoMembershipConfig.class);
        when(((AutoMembershipAware) syncHandler).getAutoMembershipConfig()).thenReturn(amc);
        context.registerService(SyncHandler.class, syncHandler, Map.of(PARAM_NAME, "sh", PARAM_USER_DYNAMIC_MEMBERSHIP, true));

        assertTrue(tracker.isEnabled());
        Map<String, AutoMembershipConfig> m = tracker.getAutoMembershipConfig();
        assertEquals(1, m.size());
        assertTrue(m.containsKey("idp"));
        
        // colliding registration
        SyncHandler syncHandler2 = mock(SyncHandler.class, withSettings().extraInterfaces(AutoMembershipAware.class));
        AutoMembershipConfig amc2 = mock(AutoMembershipConfig.class);
        when(((AutoMembershipAware) syncHandler2).getAutoMembershipConfig()).thenReturn(amc2);
        context.registerService(SyncHandler.class, syncHandler2, Map.of(PARAM_NAME, "sh", PARAM_USER_DYNAMIC_MEMBERSHIP, true));

        assertTrue(tracker.isEnabled());
        m = tracker.getAutoMembershipConfig();
        assertEquals(1, m.size());
        assertTrue(m.containsKey("idp"));
    }
}