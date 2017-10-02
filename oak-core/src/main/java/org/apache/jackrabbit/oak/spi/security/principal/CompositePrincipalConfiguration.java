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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;

/**
 * {@link PrincipalConfiguration} that combines different principal provider
 * implementations that share a common principal manager implementation.
 */
public class CompositePrincipalConfiguration extends CompositeConfiguration<PrincipalConfiguration> implements PrincipalConfiguration {

    public CompositePrincipalConfiguration() {
        super(PrincipalConfiguration.NAME);
    }

    public CompositePrincipalConfiguration(@Nonnull SecurityProvider securityProvider) {
        super(PrincipalConfiguration.NAME, securityProvider);
    }

    @Nonnull
    @Override
    public PrincipalManager getPrincipalManager(Root root, NamePathMapper namePathMapper) {
        return new PrincipalManagerImpl(getPrincipalProvider(root, namePathMapper));
    }

    @Nonnull
    @Override
    public PrincipalProvider getPrincipalProvider(final Root root, final NamePathMapper namePathMapper) {
        List<PrincipalConfiguration> configurations = getConfigurations();
        switch (configurations.size()) {
            case 0: return EmptyPrincipalProvider.INSTANCE;
            case 1: return configurations.get(0).getPrincipalProvider(root, namePathMapper);
            default:
                List<PrincipalProvider> pps = new ArrayList<>(configurations.size());
                for (PrincipalConfiguration principalConfig : configurations) {
                    PrincipalProvider principalProvider = principalConfig.getPrincipalProvider(root, namePathMapper);
                    if (!(principalProvider instanceof EmptyPrincipalProvider)) {
                        pps.add(principalProvider);
                    }
                }
                return CompositePrincipalProvider.of(pps);
        }
    }
}
