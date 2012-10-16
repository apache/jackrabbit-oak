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
import javax.annotation.Nonnull;
import javax.jcr.Session;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.security.authentication.LoginContextProviderImpl;
import org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl;
import org.apache.jackrabbit.oak.security.authentication.token.TokenProviderImpl;
import org.apache.jackrabbit.oak.security.authorization.AccessControlProviderImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalManagerImpl;
import org.apache.jackrabbit.oak.security.principal.PrincipalProviderImpl;
import org.apache.jackrabbit.oak.security.user.UserContextImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.apache.jackrabbit.oak.spi.security.user.MembershipProvider;
import org.apache.jackrabbit.oak.spi.security.user.UserContext;
import org.apache.jackrabbit.oak.spi.security.user.UserProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecurityProviderImpl implements SecurityProvider {

    private static final Logger log = LoggerFactory.getLogger(SecurityProviderImpl.class);

    public static final String PARAM_APP_NAME = "org.apache.jackrabbit.oak.auth.appName";
    private static final String DEFAULT_APP_NAME = "jackrabbit.oak";

    private final ConfigurationParameters configuration;

    public SecurityProviderImpl() {
        this(new ConfigurationParameters());
    }

    public SecurityProviderImpl(ConfigurationParameters configuration) {
        this.configuration = configuration;
    }

    @Nonnull
    @Override
    public LoginContextProvider getLoginContextProvider(NodeStore nodeStore) {
        String appName = configuration.getConfigValue(PARAM_APP_NAME, DEFAULT_APP_NAME);
        Configuration loginConfig;
        try {
            loginConfig = Configuration.getConfiguration();
        } catch (SecurityException e) {
            log.warn("Failed to read login configuration: using default.", e);
            loginConfig = new OakConfiguration();
            Configuration.setConfiguration(loginConfig);
        }
        return new LoginContextProviderImpl(appName, loginConfig, nodeStore, this);
    }

    @Nonnull
    @Override
    public AccessControlProvider getAccessControlProvider() {
        return new AccessControlProviderImpl();
    }

    @Nonnull
    @Override
    public TokenProvider getTokenProvider(Root root, ConfigurationParameters options) {
        return new TokenProviderImpl(root, options, getUserContext());
    }

    @Nonnull
    @Override
    public UserContext getUserContext() {
        return new UserContextImpl();
    }

    @Nonnull
    @Override
    public PrincipalConfiguration getPrincipalConfiguration() {
        return new PrincipalConfiguration() {
            @Nonnull
            @Override
            public PrincipalManager getPrincipalManager(Session session, Root root, NamePathMapper namePathMapper) {
                PrincipalProvider principalProvider = getPrincipalProvider(root, namePathMapper);
                return new PrincipalManagerImpl(principalProvider);
            }

            @Nonnull
            @Override
            public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
                UserContext userContext = getUserContext();
                UserProvider userProvider = userContext.getUserProvider(root);
                MembershipProvider msProvider = userContext.getMembershipProvider(root);
                return new PrincipalProviderImpl(userProvider, msProvider, namePathMapper);
            }
        };
    }

    //--------------------------------------------------------------------------
    private class OakConfiguration extends Configuration {

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String s) {
            AppConfigurationEntry entry = new AppConfigurationEntry(
                    LoginModuleImpl.class.getName(),
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    Collections.<String, Object>emptyMap());
            return new AppConfigurationEntry[] {entry};
        }
    }
}
