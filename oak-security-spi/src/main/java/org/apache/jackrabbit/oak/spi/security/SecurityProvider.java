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

import org.osgi.annotation.versioning.ProviderType;

/**
 * Main entry point for security related plugins to an Oak repository. The
 * interface allow to access the available {@code SecurityConfiguration}s
 * defining the individual plugins. In addition this provider gives access
 * to the configuration parameters that apply to the configurations with the
 * specified {@link SecurityConfiguration#getName() name}.
 */
@ProviderType
public interface SecurityProvider {

    /**
     * Allows to retrieve the configuration parameters associated with a
     * given {@link org.apache.jackrabbit.oak.spi.security.SecurityConfiguration}
     * accessible by this provider. If the specified name is {@code null}
     * the global config parameters will be returned.
     *
     * @param name The {@link SecurityConfiguration#getName() name} of the security
     * configuration.
     * @return The configuration parameters associated with the {@code SecurityConfiguration}
     * identified by the specified name. If the specified name is {@code null}
     * the global config parameters will be returned.
     */
    @Nonnull
    ConfigurationParameters getParameters(@Nullable String name);

    /**
     * Returns all available {@link org.apache.jackrabbit.oak.spi.security.SecurityConfiguration}s.
     *
     * @return the available {@link org.apache.jackrabbit.oak.spi.security.SecurityConfiguration}s.
     */
    @Nonnull
    Iterable<? extends SecurityConfiguration> getConfigurations();

    /**
     * Returns the security configuration of the specified {@code configClass}.
     *
     * @param configClass The class of the configuration to retrieve.
     * @param <T>
     * @return The desired security configuration.
     */
    @Nonnull
    <T> T getConfiguration(@Nonnull Class<T> configClass);
}
