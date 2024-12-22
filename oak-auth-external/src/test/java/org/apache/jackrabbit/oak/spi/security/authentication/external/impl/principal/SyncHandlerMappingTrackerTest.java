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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping;
import org.junit.Before;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;

import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_IDP_NAME;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.SyncHandlerMapping.PARAM_SYNC_HANDLER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SyncHandlerMappingTrackerTest {

    private final BundleContext bundleContext = mock(BundleContext.class);
    private final ServiceReference ref = mock(ServiceReference.class);
    private final SyncHandlerMapping service = mock(SyncHandlerMapping.class);

    private SyncHandlerMappingTracker tracker;

    @Before
    public void before() {
        tracker = new SyncHandlerMappingTracker(bundleContext);
    }

    @Test
    public void testAddingServiceWithoutProperties() {
        tracker.addingService(ref);
        assertTrue(Iterables.isEmpty(tracker.getIdpNames("testSH")));
    }

    @Test
    public void testAddingServiceWithIdpProp() {
        when(ref.getProperty(PARAM_IDP_NAME)).thenReturn("testIDP");
        tracker.addingService(ref);
        assertTrue(Iterables.isEmpty(tracker.getIdpNames("testSH")));
    }

    @Test
    public void testAddingServiceWithSyncHandlerProp() {
        when(ref.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("testSH");
        tracker.addingService(ref);
        assertTrue(Iterables.isEmpty(tracker.getIdpNames("testSH")));
    }

    @Test
    public void testAddingServiceWithProperties() {
        when(ref.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("testSH");
        when(ref.getProperty(PARAM_IDP_NAME)).thenReturn("testIDP");

        tracker.addingService(ref);
        assertEquals(Set.of("testIDP"), CollectionUtils.toSet(tracker.getIdpNames("testSH")));

        ServiceReference ref2 = mock(ServiceReference.class);
        when(ref2.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("testSH-2");
        when(ref2.getProperty(PARAM_IDP_NAME)).thenReturn("testIDP-2");
        tracker.addingService(ref2);

        assertEquals(Set.of("testIDP"), CollectionUtils.toSet(tracker.getIdpNames("testSH")));
        assertEquals(Set.of("testIDP-2"), CollectionUtils.toSet(tracker.getIdpNames("testSH-2")));

        ServiceReference ref3 = mock(ServiceReference.class);
        when(ref3.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("testSH");
        when(ref3.getProperty(PARAM_IDP_NAME)).thenReturn("testIDP-3");
        tracker.addingService(ref3);

        assertEquals(Set.of("testIDP", "testIDP-3"), CollectionUtils.toSet(tracker.getIdpNames("testSH")));
        assertEquals(Set.of("testIDP-2"), CollectionUtils.toSet(tracker.getIdpNames("testSH-2")));
    }

    @Test
    public void testModifiedServiceWithoutProperties() {
        tracker.modifiedService(ref, service);
        assertTrue(Iterables.isEmpty(tracker.getIdpNames("testSH")));
    }

    @Test
    public void testModifiedServiceWithIdpProp() {
        when(ref.getProperty(PARAM_IDP_NAME)).thenReturn("testIDP");
        tracker.modifiedService(ref, service);
        assertTrue(Iterables.isEmpty(tracker.getIdpNames("testSH")));
    }

    @Test
    public void testModifiedServiceWithSyncHandlerProp() {
        when(ref.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("testSH");
        tracker.modifiedService(ref, service);
        assertTrue(Iterables.isEmpty(tracker.getIdpNames("testSH")));
    }

    @Test
    public void testModifiedServiceWithProperties() {
        when(ref.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("testSH");
        when(ref.getProperty(PARAM_IDP_NAME)).thenReturn("testIDP");

        tracker.addingService(ref);

        when(ref.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("testSH");
        when(ref.getProperty(PARAM_IDP_NAME)).thenReturn("testIDP-2");
        tracker.modifiedService(ref, service);
        assertEquals(Set.of("testIDP-2"), CollectionUtils.toSet(tracker.getIdpNames("testSH")));

        when(ref.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("testSH-3");
        when(ref.getProperty(PARAM_IDP_NAME)).thenReturn("testIDP-3");
        tracker.modifiedService(ref, service);
        assertTrue(Iterables.isEmpty(tracker.getIdpNames("testSH")));
        assertEquals(Set.of("testIDP-3"), CollectionUtils.toSet(tracker.getIdpNames("testSH-3")));
    }

    @Test
    public void testRemovedService() {
        when(ref.getProperty(PARAM_SYNC_HANDLER_NAME)).thenReturn("testSH");
        when(ref.getProperty(PARAM_IDP_NAME)).thenReturn("testIDP");

        tracker.addingService(ref);
        tracker.removedService(ref, service);

        assertTrue(Iterables.isEmpty(tracker.getIdpNames("testSH")));
    }
}