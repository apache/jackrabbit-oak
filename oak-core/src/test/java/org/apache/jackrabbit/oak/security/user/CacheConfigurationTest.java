package org.apache.jackrabbit.oak.security.user;

import static org.apache.jackrabbit.oak.security.user.CacheConfiguration.PARAM_CACHE_EXPIRATION;
import static org.apache.jackrabbit.oak.security.user.CacheConfiguration.PARAM_CACHE_MAX_STALE;
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
        CacheConfiguration cacheConfiguration = CacheConfiguration.fromUserConfiguration(getUserConfiguration());

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
        CacheConfiguration cacheConfiguration = CacheConfiguration.fromUserConfiguration(getUserConfiguration());

        assertNotNull(cacheConfiguration);
        assertFalse(cacheConfiguration.isCacheEnabled());
        assertEquals(0, cacheConfiguration.getExpiration());
        assertEquals(0, cacheConfiguration.getMaxStale());
        assertNotNull(cacheConfiguration.getPropertyName());
    }
}