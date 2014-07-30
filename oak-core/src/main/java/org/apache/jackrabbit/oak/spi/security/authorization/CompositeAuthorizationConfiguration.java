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
package org.apache.jackrabbit.oak.spi.security.authorization;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.jcr.security.AccessControlManager;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.CompositeAccessControlManager;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.CompositePermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositeRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

/**
 * {@link CompositeAuthorizationConfiguration} that combines different
 * authorization models. This implementation has the following characteristics:
 *
 * TODO OAK-1268
 */
public class CompositeAuthorizationConfiguration extends CompositeConfiguration<AuthorizationConfiguration> implements AuthorizationConfiguration {

    public CompositeAuthorizationConfiguration(@Nonnull SecurityProvider securityProvider) {
        super(AuthorizationConfiguration.NAME, securityProvider);
    }

    @Nonnull
    @Override
    public AccessControlManager getAccessControlManager(final @Nonnull Root root,
                                                        final @Nonnull NamePathMapper namePathMapper) {
        List<AuthorizationConfiguration> configurations = getConfigurations();
        switch (configurations.size()) {
            case 0: throw new IllegalArgumentException();
            case 1: return configurations.get(0).getAccessControlManager(root, namePathMapper);
            default:
                List<AccessControlManager> mgrs = Lists.transform(configurations, new Function<AuthorizationConfiguration, AccessControlManager>() {
                    @Override
                    public AccessControlManager apply(AuthorizationConfiguration authorizationConfiguration) {
                        return authorizationConfiguration.getAccessControlManager(root, namePathMapper);
                    }
                });
                return new CompositeAccessControlManager(root, namePathMapper, getSecurityProvider(), mgrs);

        }
    }

    @Nonnull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        return CompositeRestrictionProvider.newInstance(
                Lists.transform(getConfigurations(),
                        new Function<AuthorizationConfiguration, RestrictionProvider>() {
                            @Override
                            public RestrictionProvider apply(AuthorizationConfiguration authorizationConfiguration) {
                                return authorizationConfiguration.getRestrictionProvider();
                            }
                        }));
    }

    @Nonnull
    @Override
    public PermissionProvider getPermissionProvider(final @Nonnull Root root,
                                                    final @Nonnull String workspaceName,
                                                    final @Nonnull Set<Principal> principals) {
        List<AuthorizationConfiguration> configurations = getConfigurations();
        switch (configurations.size()) {
            case 0: throw new IllegalArgumentException();
            case 1: return configurations.get(0).getPermissionProvider(root, workspaceName, principals);
            default:
                List<AggregatedPermissionProvider> aggrPermissionProviders = Lists.newArrayListWithCapacity(configurations.size());
                for (AuthorizationConfiguration conf : configurations) {
                    PermissionProvider pProvider = conf.getPermissionProvider(root, workspaceName, principals);
                    if (pProvider instanceof AggregatedPermissionProvider) {
                        aggrPermissionProviders.add((AggregatedPermissionProvider) pProvider);
                    }
                }
                return new CompositePermissionProvider(root, aggrPermissionProviders);
        }
    }
}