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

import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;

/**
 * Base class for CUG related test that setup the authorization configuration
 * to expose the CUG specific implementations of {@code AccessControlManager}
 * and {@code PermissionProvider}.
 */
public class AbstractCugTest extends AbstractSecurityTest {

    @Override
    protected SecurityProvider getSecurityProvider() {
        if (securityProvider == null) {
            securityProvider = new CugSecurityProvider(getSecurityConfigParameters());
        }
        return securityProvider;
    }

    private final class CugSecurityProvider extends SecurityProviderImpl {

        private AuthorizationConfiguration cugConfiguration;

        private CugSecurityProvider(@Nonnull ConfigurationParameters configuration) {
            super(configuration);
            cugConfiguration = new CugConfiguration(this);
        }

        @Nonnull
        @Override
        public Iterable<? extends SecurityConfiguration> getConfigurations() {
            Set<SecurityConfiguration> configs = (Set<SecurityConfiguration>) super.getConfigurations();

            Iterator<SecurityConfiguration> it = configs.iterator();
            while (it.hasNext()) {
                if (it.next() instanceof AuthorizationConfiguration) {
                    it.remove();
                }
            }
            configs.add(cugConfiguration);
            return configs;
        }

        @Nonnull
        @Override
        public <T> T getConfiguration(@Nonnull Class<T> configClass) {
            if (AuthorizationConfiguration.class == configClass) {
                return (T) cugConfiguration;
            } else {
                return super.getConfiguration(configClass);
            }
        }
    }
}