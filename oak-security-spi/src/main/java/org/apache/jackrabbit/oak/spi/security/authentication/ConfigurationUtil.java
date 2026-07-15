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
package org.apache.jackrabbit.oak.spi.security.authentication;

import java.util.Collections;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;

/**
 * Utility to create {@link Configuration}s for built-in LoginModule implementations.
 */
public final class ConfigurationUtil {

    private ConfigurationUtil() {
    }

    /**
     * Creates a new {@link Configuration} for the default OAK authentication
     * setup which only handles login for standard JCR credentials.
     *
     * @param loginConfiguration The configuration parameters.
     * @return A new {@code Configuration}
     */
    public static Configuration getDefaultConfiguration(final ConfigurationParameters loginConfiguration) {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String applicationName) {
                Map<String, ?> options = loginConfiguration.getConfigValue(applicationName, Collections.<String, Object>emptyMap());
                return new AppConfigurationEntry[]{new DefaultEntry(options)};
            }
        };
    }

    /**
     * Creates a new {@link Configuration} backwards compatible with the default
     * Jackrabbit 2.x authentication setup. In addition to login with standard JCR
     * credentials this configuration also handles
     * {@link org.apache.jackrabbit.api.security.authentication.token.TokenCredentials}
     * and under certain circumstances treats login without credentials as
     * anonymous login.
     *
     * @param loginConfiguration The configuration parameters.
     * @return A new {@code Configuration}
     */
    public static Configuration getJackrabbit2Configuration(final ConfigurationParameters loginConfiguration) {
        return new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String applicationName) {
                Map<String, ?> options = loginConfiguration.getConfigValue(applicationName, Collections.<String, Object>emptyMap());
                return new AppConfigurationEntry[]{
                        new GuestEntry(options),
                        new TokenEntry(options),
                        new DefaultEntry(options)};
            }
        };
    }

    private static final class DefaultEntry extends AppConfigurationEntry {
        private DefaultEntry(Map<String, ?> options) {
            super("org.apache.jackrabbit.oak.security.authentication.user.LoginModuleImpl",
                    LoginModuleControlFlag.REQUIRED, options);
        }
    }

    private static final class GuestEntry extends AppConfigurationEntry {

        private GuestEntry(Map<String, ?> options) {
            super(GuestLoginModule.class.getName(), LoginModuleControlFlag.OPTIONAL, options);
        }
    }

    private static final class TokenEntry extends AppConfigurationEntry {

        private TokenEntry(Map<String, ?> options) {
            super("org.apache.jackrabbit.oak.security.authentication.token.TokenLoginModule",
                    LoginModuleControlFlag.SUFFICIENT, options);
        }
    }

}