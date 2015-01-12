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
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.security.composite.CompositeAuthorizationConfiguration;

/**
 * Base class for CUG related test that setup the authorization configuration
 * to expose the CUG specific implementations of {@code AccessControlManager}
 * and {@code PermissionProvider}.
 */
public class AbstractCugTest extends AbstractSecurityTest {

    @Override
    protected SecurityProvider getSecurityProvider() {
        if (securityProvider == null) {
            securityProvider = new CugSecurityProvider(getSecurityConfigParameters(), super.getSecurityProvider());
        }
        return securityProvider;
    }

    final class CugSecurityProvider implements SecurityProvider {

        private final ConfigurationParameters configuration;
        private final SecurityProvider base;

        private final CugConfiguration cugConfiguration;

        private CugSecurityProvider(@Nonnull ConfigurationParameters configuration, @Nonnull SecurityProvider base) {
            this.configuration = configuration;
            this.base = base;

            cugConfiguration = new CugConfiguration(this);
        }

        @Nonnull
        @Override
        public ConfigurationParameters getParameters(@Nullable String name) {
            return base.getParameters(name);
        }

        @Nonnull
        @Override
        public Iterable<? extends SecurityConfiguration> getConfigurations() {
            Set<SecurityConfiguration> configs = (Set<SecurityConfiguration>) base.getConfigurations();

            CompositeAuthorizationConfiguration composite = new CompositeAuthorizationConfiguration(this);
            Iterator<SecurityConfiguration> it = configs.iterator();
            while (it.hasNext()) {
                SecurityConfiguration sc = it.next();
                if (sc instanceof AuthorizationConfiguration) {
                    composite.addConfiguration((AuthorizationConfiguration) sc);
                    it.remove();
                }
            }
            composite.addConfiguration(new CugConfiguration(this));
            configs.add(composite);

            return configs;
        }

        @Nonnull
        @Override
        public <T> T getConfiguration(@Nonnull Class<T> configClass) {
            T c = base.getConfiguration(configClass);
            if (AuthorizationConfiguration.class == configClass) {
                CompositeAuthorizationConfiguration composite = new CompositeAuthorizationConfiguration(this);
                composite.addConfiguration((AuthorizationConfiguration) c);
                composite.addConfiguration(new CugConfiguration(this));
                return (T) composite;
            } else {
                return c;
            }
        }
    }
}