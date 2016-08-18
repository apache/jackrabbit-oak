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

import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DefaultSyncConfigTest {

    private DefaultSyncConfig config = new DefaultSyncConfig();

    private static void assertAuthorizableConfig(@Nonnull DefaultSyncConfig.Authorizable authorizableConfig) {
        assertEquals("", authorizableConfig.getPathPrefix());
        assertSame(authorizableConfig, authorizableConfig.setPathPrefix(null));
        assertEquals("", authorizableConfig.getPathPrefix());
        assertEquals("hu/hu", authorizableConfig.setPathPrefix("hu/hu").getPathPrefix());

        Set<String> autoMembership = authorizableConfig.getAutoMembership();
        assertNotNull(autoMembership);
        assertTrue(autoMembership.isEmpty());

        assertSame(authorizableConfig, authorizableConfig.setAutoMembership());
        assertTrue(authorizableConfig.getAutoMembership().isEmpty());

        assertEquals(ImmutableSet.of("gr1", "gr2"), authorizableConfig.setAutoMembership("gr1", "gr2").getAutoMembership());
        assertEquals(ImmutableSet.of("gr"), authorizableConfig.setAutoMembership("", " gr ", null, "").getAutoMembership());

        Map<String, String> mapping = authorizableConfig.getPropertyMapping();
        assertNotNull(mapping);
        assertTrue(mapping.isEmpty());

        assertSame(authorizableConfig, authorizableConfig.setPropertyMapping(ImmutableMap.of("a", "b")));
        assertEquals(ImmutableMap.of("a", "b"), authorizableConfig.getPropertyMapping());
        assertEquals(ImmutableMap.of(), authorizableConfig.setPropertyMapping(null).getPropertyMapping());

        assertEquals(0, authorizableConfig.getExpirationTime());
        assertSame(authorizableConfig, authorizableConfig.setExpirationTime(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, authorizableConfig.getExpirationTime());
    }


    @Test
    public void testName() {
        assertEquals("default", config.getName());

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
    public void testGroupConfig() {
        DefaultSyncConfig.Group groupConfig = config.group();

        assertNotNull(groupConfig);
        assertAuthorizableConfig(groupConfig);
    }
}