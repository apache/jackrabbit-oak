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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl;

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.authentication.external.AbstractExternalAuthTest;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.AutoMembershipConfig;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AutoMembershipAwareTest extends AbstractExternalAuthTest {
    
    private static final String GROUP_AUTOMEMBERSHIP_ID = "gr1";
    private static final String USER_AUTOMEMBERSHIP_ID = "gr2";
    private static final String CONFIG_AUTOMEMBERSHIP_ID_1 = "gr3";
    private static final String CONFIG_AUTOMEMBERSHIP_ID_2 = "gr4";
    
    private final DefaultSyncHandler sh = new DefaultSyncHandler();
    
    @Before
    public void before() throws Exception {
        super.before();
        
        Map<String, Object> properties = new HashMap<>();
        properties.put(DefaultSyncConfigImpl.PARAM_NAME, DefaultSyncConfig.DEFAULT_NAME);
        properties.put(DefaultSyncConfigImpl.PARAM_GROUP_AUTO_MEMBERSHIP, new String[] {GROUP_AUTOMEMBERSHIP_ID});
        properties.put(DefaultSyncConfigImpl.PARAM_USER_AUTO_MEMBERSHIP, new String[] {USER_AUTOMEMBERSHIP_ID});

        context.registerInjectActivateService(sh, properties);
    }

    @Test
    public void testNotActivated() {
        DefaultSyncHandler sh = new DefaultSyncHandler();
        assertSame(AutoMembershipConfig.EMPTY, sh.getAutoMembershipConfig());
    }
    
    @Test
    public void testMissingAutoMembershipServices() {
        assertSame(AutoMembershipConfig.EMPTY, sh.getAutoMembershipConfig());
    }
    
    @Test
    public void testNameMismatch() {
        AutoMembershipConfig amc = mock(AutoMembershipConfig.class);
        when(amc.getName()).thenReturn("nameMismatch");
        
        context.registerService(AutoMembershipConfig.class, amc, 
                Collections.singletonMap(AutoMembershipConfig.PARAM_SYNC_HANDLER_NAME, "nameMismatch"));

        assertSame(AutoMembershipConfig.EMPTY, sh.getAutoMembershipConfig());
        verifyNoInteractions(amc);
    }

    @Test
    public void testNameMatch() {
        Authorizable authorizable = mock(Authorizable.class);
        UserManager userManager = mock(UserManager.class);
        Group gr = mock(Group.class);
        Set<String> groupIds = Collections.singleton(CONFIG_AUTOMEMBERSHIP_ID_1);
        
        AutoMembershipConfig amc = mock(AutoMembershipConfig.class);
        when(amc.getName()).thenReturn(sh.getName());
        when(amc.getAutoMembership(any(Authorizable.class))).thenReturn(groupIds);
        when(amc.getAutoMembers(any(UserManager.class), any(Group.class))).thenReturn(Iterators.singletonIterator(authorizable));

        context.registerService(AutoMembershipConfig.class, amc, Collections.singletonMap(AutoMembershipConfig.PARAM_SYNC_HANDLER_NAME, sh.getName()));

        AutoMembershipConfig config = sh.getAutoMembershipConfig();
        assertNotSame(AutoMembershipConfig.EMPTY, config);
        assertNotSame(amc, config);
        
        assertEquals(sh.getName(), config.getName());
        assertEquals(groupIds, config.getAutoMembership(authorizable));
        assertTrue(Iterators.elementsEqual(Iterators.singletonIterator(authorizable), config.getAutoMembers(userManager, gr)));
        
        // verify that DefaultSyncHandler was notified about the service
        verify(amc).getAutoMembership(authorizable);
        verify(amc).getAutoMembers(userManager, gr);
        verifyNoMoreInteractions(amc);
    }

    @Test
    public void testMultipleMatches() {
        User user = mock(User.class);
        Authorizable authorizable = mock(Authorizable.class);

        AutoMembershipConfig amc = mock(AutoMembershipConfig.class);
        AutoMembershipConfig amc2 = mock(AutoMembershipConfig.class);
        
        injectAutoMembershipConfig(amc, amc2, context, sh);
        
        AutoMembershipConfig config = sh.getAutoMembershipConfig();
        assertNotSame(AutoMembershipConfig.EMPTY, config);
        assertNotSame(amc, config);
        assertNotSame(amc2, config);

        assertEquals(sh.getName(), config.getName());
        // verify that the 2 configurations get property aggregated
        assertEquals(Set.of(CONFIG_AUTOMEMBERSHIP_ID_1), config.getAutoMembership(authorizable));
        assertEquals(Set.of(CONFIG_AUTOMEMBERSHIP_ID_1, CONFIG_AUTOMEMBERSHIP_ID_2, USER_AUTOMEMBERSHIP_ID), config.getAutoMembership(user));

        // verify that DefaultSyncHandler was notified about the service
        verify(amc).getAutoMembership(authorizable);
        verify(amc).getAutoMembership(user);
        verifyNoMoreInteractions(amc);

        verify(amc2).getAutoMembership(authorizable);
        verify(amc2).getAutoMembership(user);
        verifyNoMoreInteractions(amc2);
    }
    
    @Test
    public void testSyncConfigIsUpdatedToCoverAutoMembershipConfig() throws Exception {
        User user = mock(User.class);
        Authorizable authorizable = mock(Authorizable.class);

        AutoMembershipConfig amc = mock(AutoMembershipConfig.class);
        AutoMembershipConfig amc2 = mock(AutoMembershipConfig.class);

        injectAutoMembershipConfig(amc, amc2, context, sh);
        
        Field f = DefaultSyncHandler.class.getDeclaredField("config");
        f.setAccessible(true);
        
        DefaultSyncConfig syncConfig = (DefaultSyncConfig) f.get(sh);
        DefaultSyncConfig.User scU = syncConfig.user();
        DefaultSyncConfig.Group scG = syncConfig.group();
        
        assertEquals(Set.of(USER_AUTOMEMBERSHIP_ID), scU.getAutoMembership());
        assertEquals(Set.of(GROUP_AUTOMEMBERSHIP_ID), scG.getAutoMembership());
        
        assertEquals(Set.of(CONFIG_AUTOMEMBERSHIP_ID_1, USER_AUTOMEMBERSHIP_ID), scU.getAutoMembership(authorizable));
        assertEquals(Set.of(CONFIG_AUTOMEMBERSHIP_ID_1, CONFIG_AUTOMEMBERSHIP_ID_2, USER_AUTOMEMBERSHIP_ID), scU.getAutoMembership(user));

        assertEquals(Set.of(CONFIG_AUTOMEMBERSHIP_ID_1, GROUP_AUTOMEMBERSHIP_ID), scG.getAutoMembership(authorizable));
        assertEquals(Set.of(CONFIG_AUTOMEMBERSHIP_ID_1, CONFIG_AUTOMEMBERSHIP_ID_2, USER_AUTOMEMBERSHIP_ID, GROUP_AUTOMEMBERSHIP_ID), scG.getAutoMembership(user));
    }
    
    private static void injectAutoMembershipConfig(@NotNull AutoMembershipConfig amc1, 
                                                                     @NotNull AutoMembershipConfig amc2, 
                                                                     @NotNull OsgiContext context, @NotNull DefaultSyncHandler syncHandler) {
        when(amc1.getName()).thenReturn(syncHandler.getName());
        when(amc1.getAutoMembership(any(Authorizable.class))).thenReturn(Set.of(CONFIG_AUTOMEMBERSHIP_ID_1));

        when(amc2.getName()).thenReturn(syncHandler.getName());
        when(amc2.getAutoMembership(any(User.class))).thenReturn(Set.of(CONFIG_AUTOMEMBERSHIP_ID_1, CONFIG_AUTOMEMBERSHIP_ID_2, USER_AUTOMEMBERSHIP_ID));

        context.registerService(AutoMembershipConfig.class, amc1, Collections.singletonMap(AutoMembershipConfig.PARAM_SYNC_HANDLER_NAME, syncHandler.getName()));
        context.registerService(AutoMembershipConfig.class, amc2, Collections.singletonMap(AutoMembershipConfig.PARAM_SYNC_HANDLER_NAME, syncHandler.getName()));
    }
}