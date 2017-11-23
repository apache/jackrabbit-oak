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
package org.apache.jackrabbit.oak.spi.security.principal;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;

/**
 * Configuration interface for principal management.
 */
public interface PrincipalConfiguration extends SecurityConfiguration {

    String NAME = "org.apache.jackrabbit.oak.principal";

    /**
     * Returns an instance of {@link PrincipalManager} that can be used
     * to query and retrieve principals such as needed for JCR access control
     * management.
     *
     * @param root The target root.
     * @param namePathMapper The {@code NamePathMapper} to be used.
     * @return An instance of {@link PrincipalManager}.
     * @see org.apache.jackrabbit.api.JackrabbitSession#getPrincipalManager()
     */
    @Nonnull
    PrincipalManager getPrincipalManager(Root root, NamePathMapper namePathMapper);

    /**
     * Returns an instance of the OAK {@link PrincipalProvider}.
     *
     * <h3>Backwards compatibility with Jackrabbit 2.x</h3>
     * <h4>Configuration of Principal Providers</h4>
     * In Jackrabbit 2.x the configuration of principal providers was tied to
     * the LoginModule configuration and thus mixing authentication concerns
     * with the principal management. Since OAK makes the {@code PrincipalProvider}
     * a public interface of the SPI, it's configuration goes along with the
     * configuration of the JCR level {@link PrincipalManager}. The authentication
     * setup may have access to the principal configuration if the
     * {@link org.apache.jackrabbit.oak.spi.security.SecurityProvider} is
     * made available in the {@link org.apache.jackrabbit.oak.spi.security.authentication.AuthenticationConfiguration}.
     *
     * <h4>Multiple Sources for Principals</h4>
     * In Jackrabbit 2.x it was possible to configure multiple principal providers.
     * As of OAK there is only one single principal provider implementation
     * responsible for a given workspace. If principals originate from different
     * sources it is recommended to use the {@link CompositePrincipalProvider}
     * to combine the different sources.
     *
     * @param root The target {@code Root}.
     * @param namePathMapper The {@code NamePathMapper} to be used.
     * @return An instance of {@link PrincipalProvider}.
     */
    @Nonnull
    PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper);
}
