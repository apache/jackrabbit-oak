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
package org.apache.jackrabbit.oak.security;

import java.util.Arrays;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authentication.token.TokenConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.AuthorizationConfigurationImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConfigurationImpl;
import org.apache.jackrabbit.oak.security.user.UserConfigurationImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;

public class SecurityProviderImpl implements SecurityProvider {

    private final ConfigurationParameters configuration;

    // we only need 1 instance of authorization config.
    // todo: maybe provide general mechanism to singletons of configs
    private AuthorizationConfiguration authorizationConfiguration;

    public SecurityProviderImpl() {
        this(ConfigurationParameters.EMPTY);
    }

    public SecurityProviderImpl(ConfigurationParameters configuration) {
        this.configuration = configuration;
    }

    @Nonnull
    @Override
    public ConfigurationParameters getParameters(String name) {
        return (name == null) ? configuration : configuration.getConfigValue(name, ConfigurationParameters.EMPTY);
    }

    @Nonnull
    @Override
    public Iterable<? extends SecurityConfiguration> getConfigurations() {
        return Arrays.asList(
                getAuthenticationConfiguration(),
                getAuthorizationConfiguration(),
                getUserConfiguration(),
                getPrincipalConfiguration(),
                getPrivilegeConfiguration(),
                getTokenConfiguration());
    }

    @Nonnull
    @Override
    public <T> T getConfiguration(Class<T> configClass) {
        if (AuthenticationConfiguration.class == configClass) {
            return (T) getAuthenticationConfiguration();
        } else if (AuthorizationConfiguration.class == configClass) {
            return (T) getAuthorizationConfiguration();
        } else if (UserConfiguration.class == configClass) {
            return (T) getUserConfiguration();
        } else if (PrincipalConfiguration.class == configClass) {
            return (T) getPrincipalConfiguration();
        } else if (PrivilegeConfiguration.class == configClass) {
            return (T) getPrivilegeConfiguration();
        } else if (TokenConfiguration.class == configClass) {
            return (T) getTokenConfiguration();
        } else {
            throw new IllegalArgumentException("Unsupported security configuration class " + configClass);
        }
    }

    @Nonnull
    private AuthenticationConfiguration getAuthenticationConfiguration() {
        return new AuthenticationConfigurationImpl(this);
    }

    @Nonnull
    private AuthorizationConfiguration getAuthorizationConfiguration() {
        if (authorizationConfiguration == null) {
            authorizationConfiguration = new AuthorizationConfigurationImpl(this);
        }
        return authorizationConfiguration;
    }

    @Nonnull
    private PrivilegeConfiguration getPrivilegeConfiguration() {
        return new PrivilegeConfigurationImpl();
    }

    @Nonnull
    private UserConfiguration getUserConfiguration() {
        return new UserConfigurationImpl(this);
    }

    @Nonnull
    private PrincipalConfiguration getPrincipalConfiguration() {
        return new PrincipalConfigurationImpl(this);
    }

    @Nonnull
    private TokenConfiguration getTokenConfiguration() {
        return new TokenConfigurationImpl(this);
    }
}
