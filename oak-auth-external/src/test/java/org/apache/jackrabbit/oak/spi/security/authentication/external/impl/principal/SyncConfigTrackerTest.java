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

import com.google.common.collect.ObjectArrays;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncHandler;
import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;

import java.util.Map;

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

public class SyncConfigTrackerTest {

    private final BundleContext bundleContext = mock(BundleContext.class);
    private final ServiceReference ref = mock(ServiceReference.class);

    private SyncHandlerMappingTracker mappingTracker;
    private SyncConfigTracker tracker;
    private SyncHandler service = mock(SyncHandler.class);

    @Before
    public void before() {
        mappingTracker = new SyncHandlerMappingTracker(bundleContext);
        tracker = new SyncConfigTracker(bundleContext, mappingTracker);
        assertFalse(tracker.isEnabled());
    }

    @Test
    public void testAddingServiceWithoutProps() {
        tracker.addingService(ref);
        assertFalse(tracker.isEnabled());
    }

    @Test
    public void testAddingServiceWithProperties() {
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(false);
        tracker.addingService(ref);
        assertFalse(tracker.isEnabled());

        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref);
        assertTrue(tracker.isEnabled());
    }

    @Test
    public void testAddingMultipleServices() {
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref);
        assertTrue(tracker.isEnabled());

        ServiceReference ref2 = mock(ServiceReference.class);
        when(ref2.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(false);
        tracker.addingService(ref2);
        assertTrue(tracker.isEnabled());

        ServiceReference ref3 = mock(ServiceReference.class);
        when(ref3.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(false);
        tracker.addingService(ref3);
        assertTrue(tracker.isEnabled());

        ServiceReference ref4 = mock(ServiceReference.class);
        when(ref4.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref4);
        assertTrue(tracker.isEnabled());
    }

    @Test
    public void testModifiedServiceWithoutProps() {
        tracker.addingService(ref);
        tracker.modifiedService(ref, service);
        assertFalse(tracker.isEnabled());
    }

    @Test
    public void testModifiedServiceWithProperties() {
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(false);
        tracker.addingService(ref);

        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.modifiedService(ref, service);
        assertTrue(tracker.isEnabled());

        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(false);
        tracker.modifiedService(ref, service);
        assertFalse(tracker.isEnabled());
    }

    @Test
    public void testModifiedMultipleServices() {
        // modify props not changed
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref);
        tracker.modifiedService(ref, service);
        assertTrue(tracker.isEnabled());

        // props changd to 'enabled'
        ServiceReference ref2 = mock(ServiceReference.class);
        when(ref2.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(false);
        tracker.addingService(ref2);
        when(ref2.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.modifiedService(ref2, service);
        assertTrue(tracker.isEnabled());

        // modify (prop = disabled) without having added it before
        ServiceReference ref3 = mock(ServiceReference.class);
        when(ref3.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(false);
        tracker.modifiedService(ref3, service);
        assertTrue(tracker.isEnabled());

        // modify (prop = enabled) without having added it before
        ServiceReference ref4 = mock(ServiceReference.class);
        when(ref4.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.modifiedService(ref4, service);
        assertTrue(tracker.isEnabled());
    }

    @Test
    public void testRemovedService() {
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref);
        tracker.removedService(ref, service);

        assertFalse(tracker.isEnabled());
    }

    @Test
    public void testGetAutoMembership() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        String[] gam = new String[] {"g1", "g2"};
        String[] uam = new String[] {"g3", "g4"};
        when(ref.getProperty(PARAM_NAME)).thenReturn("sh");
        when(ref.getProperty(PARAM_GROUP_AUTO_MEMBERSHIP)).thenReturn(gam);
        when(ref.getProperty(PARAM_USER_AUTO_MEMBERSHIP)).thenReturn(uam);
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref);

        ServiceReference mappingRef = mock(ServiceReference.class);
        when(mappingRef.getProperty(PARAM_IDP_NAME)).thenReturn("idp");
        when(mappingRef.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("sh");
        mappingTracker.addingService(mappingRef);

        Map<String,String[]> automembership = tracker.getAutoMembership();
        assertEquals(1, automembership.size());
        assertArrayEquals(ObjectArrays.concat(uam,gam,String.class), automembership.get("idp"));
    }

    @Test
    public void testGetAutoMembershipDynamicDisabled() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        String[] gam = new String[] {"g1", "g2"};
        String[] uam = new String[] {"g3", "g4"};
        when(ref.getProperty(PARAM_NAME)).thenReturn("sh");
        when(ref.getProperty(PARAM_GROUP_AUTO_MEMBERSHIP)).thenReturn(gam);
        when(ref.getProperty(PARAM_USER_AUTO_MEMBERSHIP)).thenReturn(uam);
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(false);
        tracker.addingService(ref);

        ServiceReference mappingRef = mock(ServiceReference.class);
        when(mappingRef.getProperty(PARAM_IDP_NAME)).thenReturn("idp");
        when(mappingRef.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("sh");
        mappingTracker.addingService(mappingRef);

        assertTrue(tracker.getAutoMembership().isEmpty());
    }

    @Test
    public void testGetAutoMembershipMissingMapping() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        String[] gam = new String[] {"g1", "g2"};
        String[] uam = new String[] {"g3", "g4"};
        when(ref.getProperty(PARAM_NAME)).thenReturn("sh");
        when(ref.getProperty(PARAM_GROUP_AUTO_MEMBERSHIP)).thenReturn(gam);
        when(ref.getProperty(PARAM_USER_AUTO_MEMBERSHIP)).thenReturn(uam);
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);

        tracker.addingService(ref);
        assertTrue(tracker.getAutoMembership().isEmpty());
    }

    @Test
    public void testGetAutoMembershipNoMatchingSyncHandlerName() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        String[] gam = new String[] {"g1", "g2"};
        String[] uam = new String[] {"g3", "g4"};
        when(ref.getProperty(PARAM_NAME)).thenReturn("sh");
        when(ref.getProperty(PARAM_GROUP_AUTO_MEMBERSHIP)).thenReturn(gam);
        when(ref.getProperty(PARAM_USER_AUTO_MEMBERSHIP)).thenReturn(uam);
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref);

        ServiceReference mappingRef = mock(ServiceReference.class);
        when(mappingRef.getProperty(PARAM_IDP_NAME)).thenReturn("idp");
        when(mappingRef.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("differentSH");
        mappingTracker.addingService(mappingRef);

        assertTrue(tracker.getAutoMembership().isEmpty());
    }

    @Test
    public void testGetAutoMembershipWithMultipleIDPs() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        String[] uam = new String[] {"g3", "g4"};
        when(ref.getProperty(PARAM_NAME)).thenReturn("sh");
        when(ref.getProperty(PARAM_USER_AUTO_MEMBERSHIP)).thenReturn(uam);
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref);

        ServiceReference mappingRef = mock(ServiceReference.class);
        when(mappingRef.getProperty(PARAM_IDP_NAME)).thenReturn("idp");
        when(mappingRef.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("sh");
        mappingTracker.addingService(mappingRef);

        ServiceReference mappingRef2 = mock(ServiceReference.class);
        when(mappingRef2.getProperty(PARAM_IDP_NAME)).thenReturn("idp2");
        when(mappingRef2.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("sh");
        mappingTracker.addingService(mappingRef2);

        Map<String,String[]> automembership = tracker.getAutoMembership();
        assertEquals(2, automembership.size());
        assertArrayEquals(uam, automembership.get("idp"));
        assertArrayEquals(uam, automembership.get("idp2"));
    }

    @Test
    public void testGetAutoMembershipMultipleHandlersAndIdps() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        String[] gam = new String[] {"g1", "g2"};
        String[] uam = new String[] {"g3", "g4"};
        when(ref.getProperty(PARAM_NAME)).thenReturn("sh");
        when(ref.getProperty(PARAM_GROUP_AUTO_MEMBERSHIP)).thenReturn(gam);
        when(ref.getProperty(PARAM_USER_AUTO_MEMBERSHIP)).thenReturn(uam);
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref);

        ServiceReference ref2 = mock(ServiceReference.class);
        when(ref2.getProperty(PARAM_NAME)).thenReturn("sh2");
        when(ref2.getProperty(PARAM_USER_AUTO_MEMBERSHIP)).thenReturn(uam);
        when(ref2.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref2);

        ServiceReference mappingRef = mock(ServiceReference.class);
        when(mappingRef.getProperty(PARAM_IDP_NAME)).thenReturn("idp");
        when(mappingRef.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("sh");
        mappingTracker.addingService(mappingRef);

        ServiceReference mappingRef2 = mock(ServiceReference.class);
        when(mappingRef2.getProperty(PARAM_IDP_NAME)).thenReturn("idp2");
        when(mappingRef2.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("sh2");
        mappingTracker.addingService(mappingRef2);

        Map<String,String[]> automembership = tracker.getAutoMembership();
        assertEquals(2, automembership.size());
        assertArrayEquals(ObjectArrays.concat(uam,gam,String.class), automembership.get("idp"));
        assertArrayEquals(uam, automembership.get("idp2"));
    }

    @Test
    public void testGetAutoMembershipWithCollision() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        String[] gam = new String[] {"g1", "g2"};
        when(ref.getProperty(PARAM_NAME)).thenReturn("sh");
        when(ref.getProperty(PARAM_GROUP_AUTO_MEMBERSHIP)).thenReturn(gam);
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref);

        String[] uam = new String[] {"g1", "g3"};
        ServiceReference ref2 = mock(ServiceReference.class);
        when(ref2.getProperty(PARAM_NAME)).thenReturn("sh");
        when(ref2.getProperty(PARAM_USER_AUTO_MEMBERSHIP)).thenReturn(uam);
        when(ref2.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref2);

        ServiceReference mappingRef = mock(ServiceReference.class);
        when(mappingRef.getProperty(PARAM_IDP_NAME)).thenReturn("idp");
        when(mappingRef.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("sh");
        mappingTracker.addingService(mappingRef);

        Map<String,String[]> automembership = tracker.getAutoMembership();
        assertEquals(1, automembership.size());
    }

    @Test
    public void testGetAutoMembershipWithDuplication() {
        assertTrue(tracker.getAutoMembership().isEmpty());

        String[] gam = new String[] {"g1", "g2"};
        when(ref.getProperty(PARAM_NAME)).thenReturn("sh");
        when(ref.getProperty(PARAM_GROUP_AUTO_MEMBERSHIP)).thenReturn(gam);
        when(ref.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref);

        ServiceReference ref2 = mock(ServiceReference.class);
        when(ref2.getProperty(PARAM_NAME)).thenReturn("sh");
        when(ref2.getProperty(PARAM_USER_AUTO_MEMBERSHIP)).thenReturn(gam);
        when(ref2.getProperty(PARAM_USER_DYNAMIC_MEMBERSHIP)).thenReturn(true);
        tracker.addingService(ref2);

        ServiceReference mappingRef = mock(ServiceReference.class);
        when(mappingRef.getProperty(PARAM_IDP_NAME)).thenReturn("idp");
        when(mappingRef.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("sh");
        mappingTracker.addingService(mappingRef);

        Map<String,String[]> automembership = tracker.getAutoMembership();
        assertEquals(1, automembership.size());
        assertArrayEquals(gam, automembership.get("idp"));
    }
}