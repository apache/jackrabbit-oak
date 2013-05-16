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
package org.apache.jackrabbit.oak.spi.security;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.api.security.authorization.PrivilegeManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.OpenAuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.OpenAccessControlConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConfiguration;
import org.apache.jackrabbit.oak.spi.security.user.UserConfiguration;

/**
 * OpenSecurityProvider... TODO: review if we really have the need for that once TODO in InitialContent is resolved
 */
public class OpenSecurityProvider implements SecurityProvider {

    @Nonnull
    @Override
    public ConfigurationParameters getParameters(@Nullable String name) {
        return ConfigurationParameters.EMPTY;
    }

    @Nonnull
    @Override
    public Iterable<? extends SecurityConfiguration> getConfigurations() {
        return ImmutableList.of(new OpenAuthenticationConfiguration(), new OpenAccessControlConfiguration(), new OpenPrivilegeConfiguration());
    }

    @Nonnull
    @Override
    public <T> T getConfiguration(Class<T> configClass) {
        if (AuthenticationConfiguration.class == configClass) {
            return (T) new OpenAuthenticationConfiguration();
        } else if (AccessControlConfiguration.class == configClass) {
            return (T) new OpenAccessControlConfiguration();
        } else if (UserConfiguration.class == configClass) {
            throw new UnsupportedOperationException();
        } else if (PrincipalConfiguration.class == configClass) {
            throw new UnsupportedOperationException();
        } else if (PrivilegeConfiguration.class == configClass) {
            return (T) new OpenPrivilegeConfiguration();
        } else {
            throw new IllegalArgumentException("Unsupported security configuration class " + configClass);
        }
    }

    private static final class OpenPrivilegeConfiguration extends SecurityConfiguration.Default implements PrivilegeConfiguration {
        @Nonnull
        @Override
        public PrivilegeManager getPrivilegeManager(Root root, NamePathMapper namePathMapper) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public RepositoryInitializer getPrivilegeInitializer() {
            return new CompositeInitializer();
        }
    }
}
