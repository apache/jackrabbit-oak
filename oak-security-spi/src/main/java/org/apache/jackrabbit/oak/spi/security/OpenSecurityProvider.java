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
import org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authentication.OpenAuthenticationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.OpenAuthorizationConfiguration;

/**
 * Rudimentary {@code SecurityProvider} implementation that allow every subject
 * to authenticate and grants it full access everywhere. Note, that this
 * implementation does not provide support for other security related features
 * such as e.g. user or access control management.
 *
 * @see org.apache.jackrabbit.oak.spi.security.authentication.OpenAuthenticationConfiguration
 * @see org.apache.jackrabbit.oak.spi.security.authorization.OpenAuthorizationConfiguration
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
        return ImmutableList.of(new OpenAuthenticationConfiguration(), new OpenAuthorizationConfiguration());
    }

    @Nonnull
    @Override
    public <T> T getConfiguration(@Nonnull Class<T> configClass) {
        if (AuthenticationConfiguration.class == configClass) {
            return (T) new OpenAuthenticationConfiguration();
        } else if (AuthorizationConfiguration.class == configClass) {
            return (T) new OpenAuthorizationConfiguration();
        } else {
            throw new IllegalArgumentException("Unsupported security configuration class " + configClass);
        }
    }
}
