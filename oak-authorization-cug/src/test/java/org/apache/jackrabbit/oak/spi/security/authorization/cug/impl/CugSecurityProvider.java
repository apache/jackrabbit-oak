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

import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.security.authorization.composite.CompositeAuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;

import static com.google.common.base.Preconditions.checkNotNull;

final class CugSecurityProvider extends SecurityProviderImpl {
    public CugSecurityProvider(@Nonnull ConfigurationParameters configuration) {
        super(configuration);

        AuthorizationConfiguration authorizationConfiguration = getConfiguration(AuthorizationConfiguration.class);
        if (!(authorizationConfiguration instanceof CompositeAuthorizationConfiguration)) {
            throw new IllegalStateException();
        } else {
            CugConfiguration cugConfiguration = new CugConfiguration();
            cugConfiguration.setSecurityProvider(this);
            cugConfiguration.activate(configuration.getConfigValue(AuthorizationConfiguration.NAME, ConfigurationParameters.EMPTY));
            AuthorizationConfiguration defConfig = checkNotNull(((CompositeAuthorizationConfiguration) authorizationConfiguration).getDefaultConfig());
            bindAuthorizationConfiguration(cugConfiguration);
            bindAuthorizationConfiguration(defConfig);
        }
    }
}