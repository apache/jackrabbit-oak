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
package org.apache.jackrabbit.oak.security.authentication;

import javax.annotation.Nonnull;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.security.OakConfiguration;
import org.apache.jackrabbit.oak.security.authentication.token.TokenProviderImpl;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AuthenticationConfigurationImpl... TODO
 */
public class AuthenticationConfigurationImpl extends SecurityConfiguration.Default implements AuthenticationConfiguration {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationConfigurationImpl.class);

    public static final String PARAM_APP_NAME = "org.apache.jackrabbit.oak.auth.appName";
    private static final String DEFAULT_APP_NAME = "jackrabbit.oak";

    public static final String PARAM_TOKEN_OPTIONS = "org.apache.jackrabbit.oak.token.options";

    private final SecurityProvider securityProvider;
    private final ConfigurationParameters options;

    public AuthenticationConfigurationImpl(SecurityProvider securityProvider, ConfigurationParameters options) {
        this.securityProvider = securityProvider;
        this.options = options;
    }

    @Nonnull
    @Override
    public LoginContextProvider getLoginContextProvider(NodeStore nodeStore, QueryIndexProvider indexProvider) {
        String appName = options.getConfigValue(PARAM_APP_NAME, DEFAULT_APP_NAME);
        Configuration loginConfig = null;
        try {
            loginConfig = Configuration.getConfiguration();
            // FIXME: workaround for Java7 behavior. needs clean up (see OAK-497)
            if (loginConfig.getAppConfigurationEntry(appName) == null) {
                log.warn("No login configuration available for " + appName + ": using default.");
                loginConfig = null;
            }
        } catch (SecurityException e) {
            log.warn("Failed to retrieve login configuration: using default. " + e);
        }
        if (loginConfig == null) {
            loginConfig = new OakConfiguration(options); // TODO: define configuration structure
        }
        return new LoginContextProviderImpl(appName, loginConfig, nodeStore, indexProvider, securityProvider);
    }

    @Nonnull
    @Override
    public TokenProvider getTokenProvider(Root root) {
        ConfigurationParameters tokenOptions = options.getConfigValue(PARAM_TOKEN_OPTIONS, new ConfigurationParameters());
        return new TokenProviderImpl(root, tokenOptions, securityProvider.getUserConfiguration());
    }
}