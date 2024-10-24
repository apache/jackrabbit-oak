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

import static org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants.PARAM_CACHE_EXPIRATION;
import static org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants.PARAM_CACHE_MAX_STALE;
import static org.junit.Assert.*;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.junit.Test;

public class CacheConfigurationTest extends AbstractSecurityTest {

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        return ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(
                        PARAM_CACHE_EXPIRATION, 10000,
                        PARAM_CACHE_MAX_STALE, 100
                )
        );
    }

    private void changeUserConfiguration(ConfigurationParameters params) {
        UserConfiguration userConfig = getUserConfiguration();
        ((ConfigurationBase) userConfig).setParameters(params);
    }

    @Test
    public void testParseUserConfiguration() {
        CacheConfiguration cacheConfiguration = CacheConfiguration.fromUserConfiguration(getUserConfiguration(), UserPrincipalProvider.REP_GROUP_PRINCIPAL_NAMES);

        assertNotNull(cacheConfiguration);
        assertTrue(cacheConfiguration.isCacheEnabled());
        assertEquals(10000, cacheConfiguration.getExpiration());
        assertEquals(100, cacheConfiguration.getMaxStale());
        assertNotNull(cacheConfiguration.getPropertyName());
    }

    @Test
    public void testIllegalArgumentOnConfiguration() {
        // Non-valid property name triggers IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> CacheConfiguration.fromUserConfiguration(getUserConfiguration(), null));
        assertThrows(IllegalArgumentException.class, () -> CacheConfiguration.fromUserConfiguration(getUserConfiguration(), ""));
        assertThrows(IllegalArgumentException.class, () -> CacheConfiguration.fromUserConfiguration(getUserConfiguration(), " "));
    }

    @Test
    public void testConfigurationWithAlternateProperty() {
        CacheConfiguration cacheConfiguration = CacheConfiguration.fromUserConfiguration(getUserConfiguration(), "alternatePropertyName");

        assertNotNull(cacheConfiguration);
        assertTrue(cacheConfiguration.isCacheEnabled());
        assertEquals(10000, cacheConfiguration.getExpiration());
        assertEquals(100, cacheConfiguration.getMaxStale());
        assertEquals("alternatePropertyName", cacheConfiguration.getPropertyName());
    }

    @Test
    public void testConfigurationWithNoCache() {
        changeUserConfiguration(ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.of(
                        PARAM_CACHE_EXPIRATION, 0,
                        PARAM_CACHE_MAX_STALE, 0
                )
        ));
        CacheConfiguration cacheConfiguration = CacheConfiguration.fromUserConfiguration(getUserConfiguration(), UserPrincipalProvider.REP_GROUP_PRINCIPAL_NAMES);

        assertNotNull(cacheConfiguration);
        assertFalse(cacheConfiguration.isCacheEnabled());
        assertEquals(0, cacheConfiguration.getExpiration());
        assertEquals(0, cacheConfiguration.getMaxStale());
        assertNotNull(cacheConfiguration.getPropertyName());
    }

    @Test
    public void testConfigurationWithMembershipThreshold() {
        CacheConfiguration cacheConfiguration = new CacheConfiguration(getUserConfiguration(), 10000, 100, UserPrincipalProvider.REP_GROUP_PRINCIPAL_NAMES, 10);

        assertNotNull(cacheConfiguration);
        assertTrue(cacheConfiguration.isCacheEnabled());
        assertEquals(10000, cacheConfiguration.getExpiration());
        assertEquals(100, cacheConfiguration.getMaxStale());
        assertNotNull(cacheConfiguration.getPropertyName());
        assertEquals(10, cacheConfiguration.getMembershipThreshold());
    }

    @Test
    public void testEmptyConfigurationParameters() {
        changeUserConfiguration(ConfigurationParameters.of(
                UserConfiguration.NAME,
                ConfigurationParameters.EMPTY
        ));
        CacheConfiguration cacheConfiguration = CacheConfiguration.fromUserConfiguration(getUserConfiguration(), UserPrincipalProvider.REP_GROUP_PRINCIPAL_NAMES);

        assertNotNull(cacheConfiguration);
        assertFalse(cacheConfiguration.isCacheEnabled());
        assertEquals(0, cacheConfiguration.getExpiration());
        assertEquals(0, cacheConfiguration.getMaxStale());
        assertNotNull(cacheConfiguration.getPropertyName());
    }
}