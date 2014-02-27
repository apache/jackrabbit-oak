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

import java.util.Dictionary;

import javax.annotation.Nonnull;
import javax.security.auth.login.Configuration;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.spi.security.ConfigurationBase;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.ConfigurationUtil;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@code AuthenticationConfiguration} with the
 * following characteristics:
 *
 * <ul>
 * <li>
 *     {@link LoginContextProvider}: Returns the default implementation of
 *     {@code LoginContextProvider} that handles standard JAAS based logins and
 *     deals with pre-authenticated subjects.</li>
 * </ul>
 *
 */
@Component
@Service({AuthenticationConfiguration.class, SecurityConfiguration.class})
public class AuthenticationConfigurationImpl extends ConfigurationBase implements AuthenticationConfiguration {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationConfigurationImpl.class);

    /**
     * Constructor for OSGi
     */
    public AuthenticationConfigurationImpl() {
        super();
    }

    @Activate
    private void activate(Dictionary<String, Object> properties) {
        setParameters(ConfigurationParameters.of(properties));
    }

    /**
     * Constructor for non-OSGi
     * @param securityProvider
     */
    public AuthenticationConfigurationImpl(SecurityProvider securityProvider) {
        super(securityProvider, securityProvider.getParameters(NAME));
    }

    //----------------------------------------------< SecurityConfiguration >---
    @Nonnull
    @Override
    public String getName() {
        return NAME;
    }

    //----------------------------------------< AuthenticationConfiguration >---
    /**
     * Create a {@code LoginContextProvider} using standard
     * {@link javax.security.auth.login.Configuration#getConfiguration() JAAS}
     * functionality. In case no login configuration for the specified app name
     * can be retrieve this implementation uses the default as defined by
     * {@link ConfigurationUtil#getDefaultConfiguration(org.apache.jackrabbit.oak.spi.security.ConfigurationParameters)}.
     * <p>
     * The {@link LoginContextProvider} implementation is intended to be used with
     * <ul>
     *     <li>Regular login using JAAS {@link javax.security.auth.spi.LoginModule} or</li>
     *     <li>Pre-authenticated subjects in which case any authentication
     *     related validation is omitted</li>
     * </ul>
     *
     * <h4>Configuration Options</h4>
     * <ul>
     *     <li>{@link #PARAM_APP_NAME}: The appName passed to
     *     {@code Configuration#getAppConfigurationEntry(String)}. The default
     *     value is {@link #DEFAULT_APP_NAME}.</li>
     * </ul>
     *
     * @param contentRepository The content repository.
     * @return An new instance of {@link LoginContextProvider}.
     */
    @Nonnull
    @Override
    public LoginContextProvider getLoginContextProvider(ContentRepository contentRepository) {
        String appName = getParameters().getConfigValue(PARAM_APP_NAME, DEFAULT_APP_NAME);
        Configuration loginConfig = null;
        try {
            loginConfig = Configuration.getConfiguration();
            // NOTE: workaround for Java7 behavior (see OAK-497)
            if (loginConfig.getAppConfigurationEntry(appName) == null) {
                loginConfig = null;
            }
        } catch (SecurityException e) {
            log.info("Failed to retrieve login configuration: using default. " + e);
        }
        if (loginConfig == null) {
            log.debug("No login configuration available for {}; using default", appName);
            loginConfig = ConfigurationUtil.getDefaultConfiguration(getParameters());
        }
        // todo: temporary workaround
        SecurityProvider provider = getSecurityProvider();
        Whiteboard whiteboard = null;
        if (provider instanceof WhiteboardAware) {
            whiteboard = ((WhiteboardAware) provider).getWhiteboard();
        } else {
            log.warn("Unable to obtain whiteboard from SecurityProvider");
        }
        return new LoginContextProviderImpl(appName, loginConfig, contentRepository, getSecurityProvider(), whiteboard);
    }
}