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

import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.security.authentication.AuthenticationConfigurationImpl;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConfigurationImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalConfigurationImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConfigurationImpl;
import org.apache.jackrabbit.oak.security.user.UserConfigurationImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;

public class SecurityProviderImpl implements SecurityProvider {

    private final ConfigurationParameters configuration;

    public SecurityProviderImpl() {
        this(new ConfigurationParameters());
    }

    public SecurityProviderImpl(ConfigurationParameters configuration) {
        this.configuration = configuration;
    }

    @Nonnull
    @Override
    public ConfigurationParameters getParameters(String name) {
        return (name == null) ? configuration : configuration.getConfigValue(name, new ConfigurationParameters());
    }

    @Nonnull
    @Override
    public Iterable<? extends SecurityConfiguration> getConfigurations() {
        Set<SecurityConfiguration> scs = new HashSet<SecurityConfiguration>();
        scs.add(getAuthenticationConfiguration());
        scs.add(getAccessControlConfiguration());
        scs.add(getUserConfiguration());
        scs.add(getPrincipalConfiguration());
        scs.add(getPrivilegeConfiguration());
        return scs;
    }

    @Nonnull
    @Override
    public <T> T getConfiguration(Class<T> configClass) {
        if (AuthenticationConfiguration.class == configClass) {
            return (T) getAuthenticationConfiguration();
        } else if (AccessControlConfiguration.class == configClass) {
            return (T) getAccessControlConfiguration();
        } else if (UserConfiguration.class == configClass) {
            return (T) getUserConfiguration();
        } else if (PrincipalConfiguration.class == configClass) {
            return (T) getPrincipalConfiguration();
        } else if (PrivilegeConfiguration.class == configClass) {
            return (T) getPrivilegeConfiguration();
        } else {
            throw new IllegalArgumentException("Unsupported security configuration class " + configClass);
        }
    }

    @Nonnull
    private AuthenticationConfiguration getAuthenticationConfiguration() {
        return new AuthenticationConfigurationImpl(this);
    }

    @Nonnull
    private AccessControlConfiguration getAccessControlConfiguration() {
        return new AccessControlConfigurationImpl(this);
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
}
