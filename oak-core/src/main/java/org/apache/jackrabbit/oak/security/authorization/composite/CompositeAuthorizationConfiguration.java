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
package org.apache.jackrabbit.oak.security.authorization.composite;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.CompositeConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.AggregatedPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.EmptyPermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.CompositeRestrictionProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link CompositeAuthorizationConfiguration} that combines different
 * authorization models. This implementation has the following characteristics:
 *
 * <h2>AccessControlManager</h2>
 * <ul>
 *     <li>This method will return an aggregation of {@code AccessControlManager}s in case
 *     multiple {@code AuthorizationConfiguration}s are present (see {@code CompositeAccessControlManager}).</li>
 *     <li>If the composite only contains a single entry the {@code AccessControlManager}
 *     of this implementation is return without extra wrapping.</li>
 *     <li>If the list of configurations is empty an {@code IllegalStateException} is thrown.</li>
 * </ul>
 *
 * <h2>PermissionProvider</h2>
 * <ul>
 *     <li>This method will return an aggregation of {@code PermissionProvider}s in case
 *     multiple {@code AuthorizationConfiguration}s exposing an {@link AggregatedPermissionProvider}
 *     are present (see {@link CompositePermissionProvider}. Note however, that
 *     providers not implementing the {@code AggregatedPermissionProvider} extension
 *     will be ignored.</li>
 *     <li>If the composite only contains a single entry the {@code PermissionProvider}
 *     of this implementation is return without extra wrapping.</li>
 *     <li>If the list of configurations is empty an {@code IllegalStateException} is thrown.</li>
 * </ul>
 *
 * <h2>RestrictionProvider</h2>
  * <ul>
  *     <li>This method will return an aggregation of {@code RestrictionProvider}s in case
  *     multiple {@code AuthorizationConfiguration}s are present (see {@code CompositeRestrictionProvider}).</li>
  *     <li>If the composite only contains a single entry the {@code RestrictionProvider}
  *     of this implementation is return without extra wrapping.</li>
  *     <li>If the list of configurations is empty {@link RestrictionProvider#EMPTY } is returned.</li>
  * </ul>
 *
 */
public class CompositeAuthorizationConfiguration extends CompositeConfiguration<AuthorizationConfiguration> implements AuthorizationConfiguration {

    private static final Logger log = LoggerFactory.getLogger(CompositeAuthorizationConfiguration.class);

    public enum CompositionType {

        /**
         * Break as soon as any one of the aggregated permission providers
         * denies a privilege (default setup)
         */
        AND,

        /**
         * Check all aggregated permission providers for one that could provide
         * a privilege (multiplexing setup)
         */
        OR;

        /**
         * Returns the corresponding composition type.
         * @param type
         *            String representation of the composition type, or
         *            {@code null}
         * @return corresponding composition type, or {@code AND} if the
         *         provided type is {@code null}
         */
        public static CompositionType fromString(@Nullable String type) {
            String or = OR.name();
            if (or.equals(type) || or.toLowerCase().equals(type)) {
                return OR;
            } else {
                return AND;
            }
        }
    }

    private CompositionType compositionType = CompositionType.AND;

    public CompositeAuthorizationConfiguration() {
        super(AuthorizationConfiguration.NAME);
    }

    public CompositeAuthorizationConfiguration(@Nonnull SecurityProvider securityProvider) {
        super(AuthorizationConfiguration.NAME, securityProvider);
    }

    public void withCompositionType(@Nullable String ct) {
        this.compositionType = CompositionType.fromString(ct);
    }

    @Nonnull
    @Override
    public AccessControlManager getAccessControlManager(@Nonnull final Root root,
                                                        @Nonnull final NamePathMapper namePathMapper) {
        List<AuthorizationConfiguration> configurations = getConfigurations();
        switch (configurations.size()) {
            case 0: throw new IllegalStateException();
            case 1: return configurations.get(0).getAccessControlManager(root, namePathMapper);
            default:
                List<AccessControlManager> mgrs = Lists.transform(configurations, authorizationConfiguration -> authorizationConfiguration.getAccessControlManager(root, namePathMapper));
                return new CompositeAccessControlManager(root, namePathMapper, getSecurityProvider(), mgrs);

        }
    }

    @Nonnull
    @Override
    public RestrictionProvider getRestrictionProvider() {
        List<AuthorizationConfiguration> configurations = getConfigurations();
        switch (configurations.size()) {
            case 0: return RestrictionProvider.EMPTY;
            case 1: return configurations.get(0).getRestrictionProvider();
            default:
                List<RestrictionProvider> rps = new ArrayList<RestrictionProvider>(configurations.size());
                for (AuthorizationConfiguration c : configurations) {
                    RestrictionProvider rp = c.getRestrictionProvider();
                    if (RestrictionProvider.EMPTY != rp) {
                        rps.add(rp);
                    }
                }
                return CompositeRestrictionProvider.newInstance(rps);
        }
    }

    @Nonnull
    @Override
    public PermissionProvider getPermissionProvider(@Nonnull final Root root,
                                                    @Nonnull final String workspaceName,
                                                    @Nonnull final Set<Principal> principals) {
        List<AuthorizationConfiguration> configurations = getConfigurations();
        switch (configurations.size()) {
            case 0: throw new IllegalStateException();
            case 1: return configurations.get(0).getPermissionProvider(root, workspaceName, principals);
            default:
                List<AggregatedPermissionProvider> aggrPermissionProviders = new ArrayList<>(configurations.size());
                for (AuthorizationConfiguration conf : configurations) {
                    PermissionProvider pProvider = conf.getPermissionProvider(root, workspaceName, principals);
                    if (pProvider instanceof AggregatedPermissionProvider) {
                        aggrPermissionProviders.add((AggregatedPermissionProvider) pProvider);
                    } else {
                        log.debug("Ignoring permission provider of '{}': Not an AggregatedPermissionProvider", conf.getClass().getName());
                    }
                }
                PermissionProvider pp;
                switch (aggrPermissionProviders.size()) {
                    case 0 :
                        pp = EmptyPermissionProvider.getInstance();
                        break;
                    case 1 :
                        pp = aggrPermissionProviders.get(0);
                        break;
                    default :
                        pp = new CompositePermissionProvider(root, aggrPermissionProviders, getContext(), compositionType, getRootProvider());
                }
                return pp;
        }
    }
}