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

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_DISABLE_MISSING_USERS_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_GROUP_AUTO_MEMBERSHIP_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_GROUP_DYNAMIC_GROUPS;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_GROUP_DYNAMIC_GROUPS_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_GROUP_EXPIRATION_TIME_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_GROUP_PATH_PREFIX_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_GROUP_PROPERTY_MAPPING_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_AUTO_MEMBERSHIP_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_DYNAMIC_MEMBERSHIP_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_ENFORCE_DYNAMIC_MEMBERSHIP;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_ENFORCE_DYNAMIC_MEMBERSHIP_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_EXPIRATION_TIME_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_MEMBERSHIP_EXPIRATION_TIME_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_PATH_PREFIX_DEFAULT;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_PROPERTY_MAPPING;
import static org.apache.jackrabbit.oak.spi.security.authentication.external.impl.DefaultSyncConfigImpl.PARAM_USER_PROPERTY_MAPPING_DEFAULT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultSyncConfigImplTest {

    @Test
    public void testDefaultName() {
        DefaultSyncConfig config = DefaultSyncConfigImpl.of(ConfigurationParameters.EMPTY);
        assertEquals(DefaultSyncConfigImpl.PARAM_NAME_DEFAULT, config.getName());
    }

    @Test
    public void testDefaultUser() {
        DefaultSyncConfig.User userConfig = DefaultSyncConfigImpl.of(ConfigurationParameters.EMPTY).user();
        assertEquals(PARAM_DISABLE_MISSING_USERS_DEFAULT, userConfig.getDisableMissing());
        assertEquals(PARAM_USER_DYNAMIC_MEMBERSHIP_DEFAULT, userConfig.getDynamicMembership());
        assertEquals(PARAM_USER_ENFORCE_DYNAMIC_MEMBERSHIP_DEFAULT, userConfig.getEnforceDynamicMembership());
        assertEquals(PARAM_USER_MEMBERSHIP_NESTING_DEPTH_DEFAULT, userConfig.getMembershipNestingDepth());
        assertEquals(ConfigurationParameters.Milliseconds.of(PARAM_USER_MEMBERSHIP_EXPIRATION_TIME_DEFAULT), ConfigurationParameters.Milliseconds.of(userConfig.getMembershipExpirationTime()));

        assertEquals(ConfigurationParameters.Milliseconds.of(PARAM_USER_EXPIRATION_TIME_DEFAULT), ConfigurationParameters.Milliseconds.of(userConfig.getExpirationTime()));
        assertEquals(PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE_DEFAULT, userConfig.isApplyRFC7613UsernameCaseMapped());
        assertArrayEquals(PARAM_USER_AUTO_MEMBERSHIP_DEFAULT, userConfig.getAutoMembership().toArray(new String[0]));
        assertEquals(getMapping(PARAM_USER_PROPERTY_MAPPING_DEFAULT), userConfig.getPropertyMapping());
        assertEquals(PARAM_USER_PATH_PREFIX_DEFAULT, userConfig.getPathPrefix());
    }
    
    @Test
    public void testUserDynamicMembership() {
        ConfigurationParameters params = ConfigurationParameters.of(PARAM_USER_DYNAMIC_MEMBERSHIP, true);
        DefaultSyncConfig.User userConfig = DefaultSyncConfigImpl.of(params).user();
        assertTrue(userConfig.getDynamicMembership());
        assertFalse(userConfig.getEnforceDynamicMembership());

        params = ConfigurationParameters.of(PARAM_USER_ENFORCE_DYNAMIC_MEMBERSHIP, true);
        userConfig = DefaultSyncConfigImpl.of(params).user();
        assertFalse(userConfig.getDynamicMembership());
        assertTrue(userConfig.getEnforceDynamicMembership());

        params = ConfigurationParameters.of(PARAM_USER_DYNAMIC_MEMBERSHIP, true, PARAM_USER_ENFORCE_DYNAMIC_MEMBERSHIP, true);
        userConfig = DefaultSyncConfigImpl.of(params).user();
        assertTrue(userConfig.getDynamicMembership());
        assertTrue(userConfig.getEnforceDynamicMembership());
    }

    @Test
    public void testDefaultGroup() {
        DefaultSyncConfig.Group groupConfig = DefaultSyncConfigImpl.of(ConfigurationParameters.EMPTY).group();
        assertEquals(ConfigurationParameters.Milliseconds.of(PARAM_GROUP_EXPIRATION_TIME_DEFAULT), ConfigurationParameters.Milliseconds.of(groupConfig.getExpirationTime()));
        assertEquals(PARAM_ENABLE_RFC7613_USERCASE_MAPPED_PROFILE_DEFAULT, groupConfig.isApplyRFC7613UsernameCaseMapped());
        assertArrayEquals(PARAM_GROUP_AUTO_MEMBERSHIP_DEFAULT, groupConfig.getAutoMembership().toArray(new String[0]));
        assertEquals(getMapping(PARAM_GROUP_PROPERTY_MAPPING_DEFAULT), groupConfig.getPropertyMapping());
        assertEquals(PARAM_GROUP_PATH_PREFIX_DEFAULT, groupConfig.getPathPrefix());
        assertEquals(PARAM_GROUP_DYNAMIC_GROUPS_DEFAULT, groupConfig.getDynamicGroups());
    }

    @Test
    public void testGroup() {
        ConfigurationParameters params = ConfigurationParameters.of(PARAM_GROUP_DYNAMIC_GROUPS, true);
        DefaultSyncConfig.Group groupConfig = DefaultSyncConfigImpl.of(params).group();
        assertTrue(groupConfig.getDynamicGroups());
        
        params = ConfigurationParameters.of(PARAM_USER_ENFORCE_DYNAMIC_MEMBERSHIP, false);
        groupConfig = DefaultSyncConfigImpl.of(params).group();
        assertFalse(groupConfig.getDynamicGroups());
    }

    @Test
    public void testInvalidMapping() {
        String[] invalidMapping = new String[] {"invalid:Mapping"};
        DefaultSyncConfig.User userConfig = DefaultSyncConfigImpl.of(ConfigurationParameters.of(PARAM_USER_PROPERTY_MAPPING, invalidMapping)).user();
        assertEquals(Collections.emptyMap(), userConfig.getPropertyMapping());
    }

    @Test
    public void testValidMapping() {
        String[] validMapping = new String[] {"valid=mapping","valid2=mapping"};
        DefaultSyncConfig.User userConfig = DefaultSyncConfigImpl.of(ConfigurationParameters.of(PARAM_USER_PROPERTY_MAPPING, validMapping)).user();
        assertEquals(Map.of("valid","mapping","valid2","mapping"), userConfig.getPropertyMapping());
    }

    private static Map<String,String> getMapping(@NotNull String[] defaultMapping) {
        Map<String,String> expectedMapping = new HashMap<>();
        for (String s : defaultMapping) {
            int indx = s.indexOf('=');
            expectedMapping.put(s.substring(0, indx), s.substring(indx+1));
        }
        return expectedMapping;
    }
}