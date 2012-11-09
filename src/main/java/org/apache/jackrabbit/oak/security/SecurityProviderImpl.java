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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.Session;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.authentication.LoginContextProviderImpl;
import org.apache.jackrabbit.oak.security.authentication.token.TokenProviderImpl;
import org.apache.jackrabbit.oak.security.authorization.AccessControlConfigurationImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalManagerImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalProviderImpl;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConfigurationImpl;
import org.apache.jackrabbit.oak.security.user.UserConfigurationImpl;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecurityProviderImpl implements SecurityProvider {

    private static final Logger log = LoggerFactory.getLogger(SecurityProviderImpl.class);

    public static final String PARAM_APP_NAME = "org.apache.jackrabbit.oak.auth.appName";
    private static final String DEFAULT_APP_NAME = "jackrabbit.oak";

    public static final String PARAM_USER_OPTIONS = "org.apache.jackrabbit.oak.user.options";
    public static final String PARAM_TOKEN_OPTIONS = "org.apache.jackrabbit.oak.token.options";

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
        scs.add(getAccessControlProvider());
        scs.add(getUserConfiguration());
        scs.add(getPrincipalConfiguration());
        scs.add(getPrivilegeConfiguration());
        return scs;
    }

    @Nonnull
    @Override
    public LoginContextProvider getLoginContextProvider(NodeStore nodeStore, QueryIndexProvider indexProvider) {
        String appName = configuration.getConfigValue(PARAM_APP_NAME, DEFAULT_APP_NAME);
        Configuration loginConfig;
        try {
            loginConfig = Configuration.getConfiguration();
        } catch (SecurityException e) {
            log.warn("Failed to retrieve login configuration: using default.", e);
            loginConfig = new OakConfiguration(configuration); // TODO: define configuration structure
            Configuration.setConfiguration(loginConfig);
        }
        return new LoginContextProviderImpl(appName, loginConfig, nodeStore, indexProvider, this);
    }

    @Nonnull
    @Override
    public TokenProvider getTokenProvider(Root root) {
        ConfigurationParameters options = configuration.getConfigValue(PARAM_TOKEN_OPTIONS, new ConfigurationParameters());
        return new TokenProviderImpl(root, options, getUserConfiguration());
    }

    @Nonnull
    @Override
    public AccessControlConfiguration getAccessControlProvider() {
        return new AccessControlConfigurationImpl();
    }

    @Nonnull
    @Override
    public PrivilegeConfiguration getPrivilegeConfiguration() {
        return new PrivilegeConfigurationImpl();
    }

    @Nonnull
    @Override
    public UserConfiguration getUserConfiguration() {
        ConfigurationParameters options = configuration.getConfigValue(PARAM_USER_OPTIONS, new ConfigurationParameters());
        return new UserConfigurationImpl(options, this);
    }

    @Nonnull
    @Override
    public PrincipalConfiguration getPrincipalConfiguration() {
        return new PrincipalConfigurationImpl();
    }

    private class PrincipalConfigurationImpl extends SecurityConfiguration.Default implements PrincipalConfiguration {
        @Nonnull
        @Override
        public PrincipalManager getPrincipalManager(Session session, Root root, NamePathMapper namePathMapper) {
            PrincipalProvider principalProvider = getPrincipalProvider(root, namePathMapper);
            return new PrincipalManagerImpl(principalProvider);
        }

        @Nonnull
        @Override
        public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
            return new PrincipalProviderImpl(root, getUserConfiguration(), namePathMapper);
        }

        @Nonnull
        @Override
        public List<ValidatorProvider> getValidatorProviders() {
            return Collections.emptyList();
        }

        @Nonnull
        @Override
        public List<ProtectedItemImporter> getProtectedItemImporters() {
            return Collections.emptyList();
        }
    }
}
