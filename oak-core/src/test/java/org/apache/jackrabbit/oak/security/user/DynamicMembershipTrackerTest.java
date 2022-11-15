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
package org.apache.jackrabbit.oak.security.user;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.DynamicMembershipService;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.sling.testing.mock.osgi.MapUtil;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.osgi.framework.ServiceRegistration;

import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DynamicMembershipTrackerTest extends AbstractSecurityTest {

    @Rule
    public final OsgiContext context = new OsgiContext();
    
    private final Whiteboard whiteboard = new OsgiWhiteboard(context.bundleContext());
    private final DynamicMembershipTracker dynamicMembership = new DynamicMembershipTracker();
    
    private final List<ServiceRegistration> registrations = new ArrayList<>();
    private Group gr;
    
    @Before
    public void before() throws Exception {
        super.before();
        dynamicMembership.start(whiteboard);
        gr = getUserManager(root).createGroup("groupId");
        root.commit();
    }
    
    @After
    public void after() throws Exception {
        try {
            for (ServiceRegistration registration : registrations) {
                registration.unregister();
            }
            dynamicMembership.stop();
            gr.remove();
            root.commit();
        } finally {
            super.after();
        }
    }
    
    @Test
    public void testNoServiceRegistered() {
        DynamicMembershipProvider dmp = dynamicMembership.getDynamicMembershipProvider(root, getUserManager(root), getNamePathMapper());
        assertTrue(dmp instanceof EveryoneMembershipProvider);
    }

    @Test
    public void testServiceWithDefaultProvider() {
        DynamicMembershipService dms = (root, userManager, namePathMapper) -> DynamicMembershipProvider.EMPTY;
        registrations.add(context.bundleContext().registerService(DynamicMembershipService.class.getName(), dms, MapUtil.toDictionary(Collections.emptyMap())));
        
        DynamicMembershipProvider dmp = dynamicMembership.getDynamicMembershipProvider(root, getUserManager(root), getNamePathMapper());
        assertTrue(dmp instanceof EveryoneMembershipProvider);
    }
    
    @Test
    public void testServiceWithCustomProvider() throws Exception {
        Authorizable a = mock(Authorizable.class);
        User testUser = getTestUser();

        DynamicMembershipProvider dmp = mock(DynamicMembershipProvider.class);
        when(dmp.getMembership(eq(a), anyBoolean())).thenReturn(Iterators.singletonIterator(gr));
        when(dmp.getMembership(eq(testUser), anyBoolean())).thenReturn(Collections.emptyIterator());
        
        when(dmp.isMember(eq(gr), eq(a), anyBoolean())).thenReturn(true);
        when(dmp.coversAllMembers(gr)).thenReturn(true);
        when(dmp.getMembers(eq(gr), anyBoolean())).thenReturn(Iterators.singletonIterator(a));
        
        DynamicMembershipService dms = (root, userManager, namePathMapper) -> dmp;
        registrations.add(context.bundleContext().registerService(DynamicMembershipService.class.getName(), dms, MapUtil.toDictionary(Collections.emptyMap())));

        DynamicMembershipProvider provider = dynamicMembership.getDynamicMembershipProvider(root, getUserManager(root), getNamePathMapper());
        assertFalse(dmp instanceof EveryoneMembershipProvider);
        
        // verify dmp is properly wired
        assertTrue(Iterators.contains(provider.getMembership(a, false), gr));
        assertFalse(Iterators.contains(provider.getMembership(testUser, false), gr));
        
        assertTrue(provider.coversAllMembers(gr));
        assertFalse(provider.coversAllMembers(mock(Group.class)));
        
        assertTrue(provider.isMember(gr, a, false));
        assertFalse(provider.isMember(gr, testUser, true));
        
        assertTrue(Iterators.contains(provider.getMembers(gr, true), a));
        assertFalse(Iterators.contains(provider.getMembers(gr, true), testUser));
        
        // verify that EveryoneMembershipProvider is covered as well
        Group everyone = mock(Group.class);
        when(everyone.isGroup()).thenReturn(true);
        when(everyone.getPrincipal()).thenReturn(EveryonePrincipal.getInstance());
        assertTrue(provider.coversAllMembers(everyone));
        assertTrue(provider.isMember(everyone, testUser, false));
        assertTrue(provider.isMember(everyone, a, false));
    }
}