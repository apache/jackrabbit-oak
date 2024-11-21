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
package org.apache.jackrabbit.oak.spi.security.authentication.external.basic;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultSyncConfigTest {

    private final DefaultSyncConfig config = new DefaultSyncConfig();

    private static void assertAuthorizableConfig(@NotNull DefaultSyncConfig.Authorizable authorizableConfig) {
        assertEquals("", authorizableConfig.getPathPrefix());
        assertSame(authorizableConfig, authorizableConfig.setPathPrefix(null));
        assertEquals("", authorizableConfig.getPathPrefix());
        assertEquals("hu/hu", authorizableConfig.setPathPrefix("hu/hu").getPathPrefix());

        Set<String> autoMembership = authorizableConfig.getAutoMembership();
        assertNotNull(autoMembership);
        assertTrue(autoMembership.isEmpty());

        assertSame(authorizableConfig, authorizableConfig.setAutoMembership());
        assertTrue(authorizableConfig.getAutoMembership().isEmpty());

        assertEquals(Set.of("gr1", "gr2"), authorizableConfig.setAutoMembership("gr1", "gr2").getAutoMembership());
        assertEquals(Set.of("gr"), authorizableConfig.setAutoMembership("", " gr ", null, "").getAutoMembership());

        Map<String, String> mapping = authorizableConfig.getPropertyMapping();
        assertNotNull(mapping);
        assertTrue(mapping.isEmpty());

        assertSame(authorizableConfig, authorizableConfig.setPropertyMapping(Map.of("a", "b")));
        assertEquals(Map.of("a", "b"), authorizableConfig.getPropertyMapping());
        assertEquals(Map.of(), authorizableConfig.setPropertyMapping(null).getPropertyMapping());

        assertEquals(0, authorizableConfig.getExpirationTime());
        assertSame(authorizableConfig, authorizableConfig.setExpirationTime(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, authorizableConfig.getExpirationTime());
    }

    @Test
    public void testName() {
        assertEquals(DefaultSyncConfig.DEFAULT_NAME, config.getName());

        assertSame(config, config.setName("name"));
        assertEquals("name", config.getName());
    }

    @Test
    public void testUserConfig() {
        DefaultSyncConfig.User userConfig = config.user();

        assertNotNull(userConfig);
        assertAuthorizableConfig(userConfig);

        assertEquals(0, userConfig.getMembershipExpirationTime());

        assertSame(userConfig, userConfig.setMembershipExpirationTime(1));
        assertEquals(1, userConfig.getMembershipExpirationTime());
        assertEquals(Long.MIN_VALUE, userConfig.setMembershipExpirationTime(Long.MIN_VALUE).getMembershipExpirationTime());

        assertEquals(0, userConfig.getMembershipNestingDepth());

        assertSame(userConfig, userConfig.setMembershipNestingDepth(5));
        assertEquals(5, userConfig.getMembershipNestingDepth());
        assertEquals(0, userConfig.setMembershipExpirationTime(0).getMembershipExpirationTime());
    }
    
    @Test
    public void testUserDynamicMembership() {
        DefaultSyncConfig.User userConfig = config.user();

        assertFalse(userConfig.getDynamicMembership());
        assertSame(userConfig, userConfig.setDynamicMembership(true));
        assertTrue(userConfig.getDynamicMembership());

        assertFalse(userConfig.getEnforceDynamicMembership());
        assertSame(userConfig, userConfig.setEnforceDynamicMembership(true));
        assertTrue(userConfig.getEnforceDynamicMembership());
    }

    @Test
    public void testGroupConfig() {
        DefaultSyncConfig.Group groupConfig = config.group();

        assertNotNull(groupConfig);
        assertAuthorizableConfig(groupConfig);
        
        assertFalse(groupConfig.getDynamicGroups());
        assertSame(groupConfig, groupConfig.setDynamicGroups(true));
        assertTrue(groupConfig.getDynamicGroups());
        assertSame(groupConfig, groupConfig.setDynamicGroups(false));
        assertFalse(groupConfig.getDynamicGroups());
    }
    
    @Test
    public void testAutoMembershipConfig() {
        // not set yet
        assertSame(AutoMembershipConfig.EMPTY, config.group().getAutoMembershipConfig());
        assertSame(AutoMembershipConfig.EMPTY, config.user().getAutoMembershipConfig());
        
        // set AutoMembershipConfig
        AutoMembershipConfig acm = mock(AutoMembershipConfig.class);
        config.group().setAutoMembershipConfig(acm);
        assertSame(acm, config.group().getAutoMembershipConfig());

        config.user().setAutoMembershipConfig(acm);
        assertSame(acm, config.user().getAutoMembershipConfig());
    }

    @Test
    public void testAutoMembership() {
        Set<String> globalGroupIds = Set.of("gr1", "gr2");
        Set<String> configGroupIds = Set.of("gr3", "gr4");
        
        Group gr = mock(Group.class);
        User user = mock(User.class);
        
        AutoMembershipConfig acm = mock(AutoMembershipConfig.class);
        when(acm.getAutoMembership(gr)).thenReturn(Collections.emptySet());
        when(acm.getAutoMembership(user)).thenReturn(configGroupIds);
      
        DefaultSyncConfig.Authorizable dscA = config.user();
        dscA.setAutoMembership(globalGroupIds.toArray(new String[0]));
        dscA.setAutoMembershipConfig(acm);

        // only global ids for getAutoMembership()
        assertEquals(globalGroupIds, dscA.getAutoMembership());
        // only global ids for getAutoMembership(Authorizable) as no specific config for 'gr'
        assertEquals(globalGroupIds, dscA.getAutoMembership(gr));
        // for 'user' the combine set of global and conditional config is returned
        Set<String> expected = ImmutableSet.<String>builder()
                .addAll(globalGroupIds)
                .addAll(configGroupIds).build();
        assertEquals(expected, dscA.getAutoMembership(user));
    }
}
