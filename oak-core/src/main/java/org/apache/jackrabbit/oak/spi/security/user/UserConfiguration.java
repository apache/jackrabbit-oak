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
package org.apache.jackrabbit.oak.spi.security.user;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;

/**
 * Configuration interface for user management.
 */
public interface UserConfiguration extends SecurityConfiguration {

    String NAME = "org.apache.jackrabbit.oak.user";

    /**
     * Create a new {@code UserManager} instance
     *
     * @param root The root associated with the user manager.
     * @param namePathMapper A name path mapper used for conversion of jcr/oak names/paths.
     * @return a new instance of {@code UserManager}
     */
    @Nonnull
    UserManager getUserManager(Root root, NamePathMapper namePathMapper);

    /**
     * Optional method that allows a given user management implementation to
     * provide a specific and optimized implementation of the {@link PrincipalProvider}
     * interface for the principals represented by the user/groups known to
     * this implementation.
     *
     * If this method returns {@code null} the security setup will by default
     * use a basic {@code PrincipalProvider} implementation based on public
     * user management API or a combination of other {@link PrincipalProvider}s
     * as configured with the repository setup.
     *
     * @param root The root used to read the principal information from.
     * @param namePathMapper The {@code NamePathMapper} to convert oak paths to JCR paths.
     * @return An implementation of {@code PrincipalProvider} or {@code null} if
     * principal discovery is provided by other means of if the default principal
     * provider implementation should be used that acts on public user management
     * API.
     *
     * @see org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration
     */
    @Nullable
    PrincipalProvider getUserPrincipalProvider(@Nonnull Root root, @Nonnull NamePathMapper namePathMapper);
}
