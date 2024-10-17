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

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.jetbrains.annotations.NotNull;

/**
 * The cache configuration is based on the UserConfiguration parameters and the property name for
 * which the cache is configured.
 */
final class CacheConfiguration {

    public static final String PARAM_CACHE_EXPIRATION = "cacheExpiration";
    public static final String PARAM_CACHE_MAX_STALE = "cacheMaxStale";
    public static final long EXPIRATION_NO_CACHE = 0;
    public static final long NO_STALE_CACHE = 0;

    private static final int MEMBERSHIP_THRESHOLD = 0;


    private final UserConfiguration userConfig;
    private final long expiration;
    private final long maxStale;
    private final String propertyName;
    private final int membershipThreshold;

    private CacheConfiguration(UserConfiguration userConfig, long expiration, long maxStale, @NotNull String propertyName) {
        this.userConfig = userConfig;
        this.expiration = expiration;
        this.maxStale = maxStale;
        this.propertyName = propertyName;
        this.membershipThreshold = 0;
    }

    // Only for testing
    CacheConfiguration(UserConfiguration userConfig, long expiration, long maxStale,
            @NotNull String propertyName,
            int membershipThreshold) {
        this.userConfig = userConfig;
        this.expiration = expiration;
        this.maxStale = maxStale;
        this.propertyName = propertyName;
        this.membershipThreshold = Math.max(membershipThreshold, MEMBERSHIP_THRESHOLD);
    }

    /**
     * Create a cache configuration based on the user configuration and the property name used to store the
     * cached principal names.
     * @param config UserConfiguration to create the cache configuration from
     * @param propertyName Property name used to store the cached principal names
     * @return CacheConfiguration based on the user configuration
     */
    public static CacheConfiguration fromUserConfiguration(@NotNull UserConfiguration config, @NotNull String propertyName) {

        if (Strings.isNullOrEmpty(propertyName) || propertyName.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid property name: " + propertyName);
        }

        long maxStale = config.getParameters().getConfigValue(PARAM_CACHE_MAX_STALE, NO_STALE_CACHE);
        long expiration  = config.getParameters().getConfigValue(PARAM_CACHE_EXPIRATION, EXPIRATION_NO_CACHE);
        return new CacheConfiguration(config, expiration, maxStale, propertyName);
    }

    public boolean isCacheEnabled() {
        return expiration > EXPIRATION_NO_CACHE;
    }

    long getExpiration() {
        return expiration;
    }

    long getMaxStale() {
        return maxStale;
    }

    String getPropertyName() {
        return propertyName;
    }

    UserConfiguration getUserConfiguration() {
        return userConfig;
    }

    int getMembershipThreshold() {
        return membershipThreshold;
    }
}
