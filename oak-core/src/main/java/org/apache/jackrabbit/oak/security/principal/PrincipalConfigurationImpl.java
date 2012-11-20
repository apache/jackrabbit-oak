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
package org.apache.jackrabbit.oak.security.principal;

import javax.annotation.Nonnull;
import javax.jcr.Session;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;

/**
 * PrincipalConfigurationImpl... TODO
 */
public class PrincipalConfigurationImpl extends SecurityConfiguration.Default implements PrincipalConfiguration {

    private final SecurityProvider securityProvider;
    private final ConfigurationParameters options;

    public PrincipalConfigurationImpl(SecurityProvider securityProvider, ConfigurationParameters options) {
        this.securityProvider = securityProvider;
        this.options = options;
    }

    @Nonnull
    @Override
    public PrincipalManager getPrincipalManager(Session session, Root root, NamePathMapper namePathMapper) {
        PrincipalProvider principalProvider = getPrincipalProvider(root, namePathMapper);
        return new PrincipalManagerImpl(principalProvider);
    }

    @Nonnull
    @Override
    public PrincipalProvider getPrincipalProvider(Root root, NamePathMapper namePathMapper) {
        return new PrincipalProviderImpl(root, securityProvider.getUserConfiguration(), namePathMapper);
    }
}