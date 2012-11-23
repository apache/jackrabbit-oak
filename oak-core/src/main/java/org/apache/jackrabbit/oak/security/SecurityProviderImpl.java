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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecurityProviderImpl implements SecurityProvider {

    private static final Logger log = LoggerFactory.getLogger(SecurityProviderImpl.class);

    public static final String PARAM_AUTHENTICATION_OPTIONS = "org.apache.jackrabbit.oak.authentication.options";
    public static final String PARAM_PRINCIPAL_OPTIONS = "org.apache.jackrabbit.oak.principal.options";
    public static final String PARAM_USER_OPTIONS = "org.apache.jackrabbit.oak.user.options";

    private final ConfigurationParameters configuration;

    public SecurityProviderImpl() {
        this(new ConfigurationParameters());
    }

    public SecurityProviderImpl(ConfigurationParameters configuration) {
        this.configuration = configuration;
    }

    @Nonnull
    @Override
    public Iterable<SecurityConfiguration> getSecurityConfigurations() {
        Set<SecurityConfiguration> scs = new HashSet<SecurityConfiguration>();
        scs.add(getAccessControlConfiguration());
        scs.add(getUserConfiguration());
        scs.add(getPrincipalConfiguration());
        scs.add(getPrivilegeConfiguration());
        return scs;
    }

    @Nonnull
    @Override
    public AuthenticationConfiguration getAuthenticationConfiguration() {
        return new AuthenticationConfigurationImpl(this, getOptions(PARAM_AUTHENTICATION_OPTIONS));
    }

    @Nonnull
    @Override
    public AccessControlConfiguration getAccessControlConfiguration() {
        return new AccessControlConfigurationImpl(this);
    }

    @Nonnull
    @Override
    public PrivilegeConfiguration getPrivilegeConfiguration() {
        return new PrivilegeConfigurationImpl();
    }

    @Nonnull
    @Override
    public UserConfiguration getUserConfiguration() {
        return new UserConfigurationImpl(this, getOptions(PARAM_USER_OPTIONS));
    }

    @Nonnull
    @Override
    public PrincipalConfiguration getPrincipalConfiguration() {
        return new PrincipalConfigurationImpl(this, getOptions(PARAM_PRINCIPAL_OPTIONS));
    }

    //------------------------------------------------------------< private >---
    /**
     *
     * @param name
     * @return
     */
    private ConfigurationParameters getOptions(String name) {
        return configuration.getConfigValue(name, new ConfigurationParameters());
    }
}
