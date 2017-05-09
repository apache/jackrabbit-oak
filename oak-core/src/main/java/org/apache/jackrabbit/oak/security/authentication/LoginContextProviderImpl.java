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

import java.security.AccessController;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Credentials;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.JaasLoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthContext;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration.PARAM_CONFIG_SPI_NAME;

/**
 * {@code LoginContextProvider}
 */
class LoginContextProviderImpl implements LoginContextProvider {

    private static final Logger log = LoggerFactory.getLogger(LoginContextProviderImpl.class);

    private final String appName;
    private final ConfigurationParameters params;
    private final ContentRepository contentRepository;
    private final SecurityProvider securityProvider;
    private final Whiteboard whiteboard;

    private Configuration configuration;

    LoginContextProviderImpl(String appName, ConfigurationParameters params,
                             ContentRepository contentRepository,
                             SecurityProvider securityProvider,
                             Whiteboard whiteboard) {
        this.appName = appName;
        this.params = params;
        this.contentRepository = contentRepository;
        this.securityProvider = securityProvider;
        this.whiteboard = whiteboard;
    }

    @Override
    @Nonnull
    public LoginContext getLoginContext(Credentials credentials, String workspaceName)
            throws LoginException {
        Subject subject = getSubject();
        if (subject != null && credentials == null) {
            log.debug("Found pre-authenticated subject: No further login actions required.");
            return new PreAuthContext(subject);
        }

        if (subject == null) {
            subject = new Subject();
        }
        CallbackHandler handler = getCallbackHandler(credentials, workspaceName);
        return new JaasLoginContext(appName, subject, handler, getConfiguration());
    }

    //------------------------------------------------------------< private >---
    @CheckForNull
    private static Subject getSubject() {
        Subject subject = null;
        try {
            subject = Subject.getSubject(AccessController.getContext());
        } catch (SecurityException e) {
            log.debug("Can't check for pre-authenticated subject. Reason: {}", e.getMessage());
        }
        return subject;
    }

    @Nonnull
    private CallbackHandler getCallbackHandler(Credentials credentials, String workspaceName) {
        return new CallbackHandlerImpl(credentials, workspaceName, contentRepository, securityProvider, whiteboard);
    }

    @Nonnull
    private Configuration getConfiguration() {
        if (configuration == null) {
            Configuration loginConfig = null;

            String configSpiName = params.getConfigValue(PARAM_CONFIG_SPI_NAME, null, String.class);
            if (configSpiName != null) {
                try {
                    /*
                     Create a configuration instance with the following characteristics
                     - Algorithm name : "JavaLoginConfig"
                     - Extra parameters : 'null' for this impl
                     - Name of the config provider : 'configSpiName' as retrieved from the PARAM_CONFIG_SPI_NAME configuration (default: null)
                     */
                    loginConfig = Configuration.getInstance(
                            "JavaLoginConfig",
                            null,
                            configSpiName
                    );
                    if (loginConfig.getAppConfigurationEntry(appName) == null) {
                        log.warn("No configuration found for application {} though fetching JAAS " +
                                "configuration from SPI {} is enabled.", appName, configSpiName);
                    }
                } catch (NoSuchAlgorithmException e) {
                    log.warn("Error fetching JAAS config from SPI {}", configSpiName, e);
                } catch (NoSuchProviderException e) {
                    log.warn("Error fetching JAAS config from SPI {}", configSpiName, e);
                }
            }

            if (loginConfig == null) {
                try {
                    loginConfig = Configuration.getConfiguration();
                    // NOTE: workaround for Java7 behavior (see OAK-497)
                    if (loginConfig.getAppConfigurationEntry(appName) == null) {
                        loginConfig = null;
                    }
                } catch (SecurityException e) {
                    log.info("Failed to retrieve login configuration: using default. " + e);
                }
            }

            if (loginConfig == null) {
                log.debug("No login configuration available for {}; using default", appName);
                loginConfig = ConfigurationUtil.getDefaultConfiguration(params);
            }
            configuration = loginConfig;
        }
        return configuration;
    }
}
