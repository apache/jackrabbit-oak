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
package org.apache.jackrabbit.oak.spi.security.authorization.cug.impl;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderBuilder;
import org.apache.jackrabbit.oak.security.internal.SecurityProviderHelper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;

final class CugSecurityProvider {

    private CugSecurityProvider() {}

    public static SecurityProvider newTestSecurityProvider(@Nonnull ConfigurationParameters configuration) {
        CugConfiguration cugConfiguration = new CugConfiguration();

        ConfigurationParameters params = configuration.getConfigValue(AuthorizationConfiguration.NAME, ConfigurationParameters.EMPTY);
        cugConfiguration.setParameters(params);

        SecurityProvider sp = SecurityProviderBuilder.newBuilder().with(configuration).build();
        SecurityProviderHelper.updateConfig(sp, cugConfiguration, AuthorizationConfiguration.class);
        return sp;
    }

    public static CugConfiguration getCugConfiguration(@Nonnull SecurityProvider securityProvider) {
        AuthorizationConfiguration ac = securityProvider.getConfiguration(AuthorizationConfiguration.class);
        if (!(ac instanceof CompositeAuthorizationConfiguration)) {
            throw new IllegalStateException();
        }
        for (AuthorizationConfiguration config : ((CompositeAuthorizationConfiguration) ac).getConfigurations()) {
            if (config instanceof CugConfiguration) {
                return (CugConfiguration) config;
            }
        }
        throw new IllegalStateException();
    }
}