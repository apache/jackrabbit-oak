/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.spi.security;

import java.security.Principal;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeWorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;

/**
 * Abstract base implementation for {@link SecurityConfiguration}s that can
 * combine different implementations.
 */
public abstract class CompositeConfiguration<T extends SecurityConfiguration> implements SecurityConfiguration {

    /**
     * Parameter used to define the ranking of a given configuration compared to
     * other registered configuration in this aggregate. If the ranking parameter
     * is missing a new configuration will be added at the end of the list.
     */
    public static final String PARAM_RANKING = "configurationRanking";

    /**
     * Default ranking value used to insert a new configuration at the end of
     * the list.
     */
    private static final int NO_RANKING = Integer.MIN_VALUE;

    private final List<T> configurations = new CopyOnWriteArrayList<T>();

    private final String name;
    private final SecurityProvider securityProvider;

    private T defaultConfig;

    public CompositeConfiguration(@Nonnull String name, @Nonnull SecurityProvider securityProvider) {
        this.name = name;
        this.securityProvider = securityProvider;
    }

    public void setDefaultConfig(@Nonnull T defaultConfig) {
        this.defaultConfig = defaultConfig;
    }

    public void addConfiguration(@Nonnull T configuration) {
        int ranking = configuration.getParameters().getConfigValue(PARAM_RANKING, NO_RANKING);
        if (ranking == NO_RANKING || configurations.isEmpty()) {
            configurations.add(configuration);
        } else {
            int i = 0;
            for (T c : configurations) {
                int r = c.getParameters().getConfigValue(PARAM_RANKING, NO_RANKING);
                if (ranking > r) {
                    break;
                } else {
                    i++;
                }
            }
            configurations.add(i, configuration);
        }
    }

    public void removeConfiguration(@Nonnull T configuration) {
        configurations.remove(configuration);
    }

    protected List<T> getConfigurations() {
        if (configurations.isEmpty() && defaultConfig != null) {
            return ImmutableList.of(defaultConfig);
        } else {
            return ImmutableList.copyOf(configurations);
        }
    }

    protected SecurityProvider getSecurityProvider() {
        return securityProvider;
    }

    //----------------------------------------------< SecurityConfiguration >---
    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public ConfigurationParameters getParameters() {
        List<T> configs = getConfigurations();
        ConfigurationParameters[] params = new ConfigurationParameters[configs.size()];
        for (int i = 0; i < configs.size(); i++) {
            params[i] = configs.get(i).getParameters();
        }
        return ConfigurationParameters.of(params);
    }

    @Nonnull
    @Override
    public WorkspaceInitializer getWorkspaceInitializer() {
        return new CompositeWorkspaceInitializer(Lists.transform(getConfigurations(), new Function<T, WorkspaceInitializer>() {
            @Override
            public WorkspaceInitializer apply(T securityConfiguration) {
                return securityConfiguration.getWorkspaceInitializer();
            }
        }));
    }

    @Nonnull
    @Override
    public RepositoryInitializer getRepositoryInitializer() {
        return new CompositeInitializer(Lists.transform(getConfigurations(), new Function<T, RepositoryInitializer>() {
            @Override
            public RepositoryInitializer apply(T securityConfiguration) {
                return securityConfiguration.getRepositoryInitializer();
            }
        }));
    }

    @Nonnull
    @Override
    public List<? extends CommitHook> getCommitHooks(final String workspaceName) {
        return ImmutableList.copyOf(Iterables.concat(Lists.transform(getConfigurations(), new Function<T, List<? extends CommitHook>>() {
            @Override
            public List<? extends CommitHook> apply(T securityConfiguration) {
                return securityConfiguration.getCommitHooks(workspaceName);
            }
        })));
    }

    @Nonnull
    @Override
    public List<? extends ValidatorProvider> getValidators(final String workspaceName, final Set<Principal> principals, final MoveTracker moveTracker) {
        return ImmutableList.copyOf(Iterables.concat(Lists.transform(getConfigurations(), new Function<T, List<? extends ValidatorProvider>>() {
            @Override
            public List<? extends ValidatorProvider> apply(T securityConfiguration) {
                return securityConfiguration.getValidators(workspaceName, principals, moveTracker);
            }
        })));
    }

    @Nonnull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        return ImmutableList.copyOf(Iterables.concat(Lists.transform(getConfigurations(), new Function<T, List<? extends ProtectedItemImporter>>() {
            @Override
            public List<? extends ProtectedItemImporter> apply(T securityConfiguration) {
                return securityConfiguration.getProtectedItemImporters();
            }
        })));
    }

    @Override
    public Context getContext() {
        final List<T> configs = getConfigurations();
        return new Context() {

            @Override
            public boolean definesProperty(@Nonnull Tree parent, @Nonnull PropertyState property) {
                for (SecurityConfiguration sc : configs) {
                    if (sc.getContext().definesProperty(parent, property)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean definesContextRoot(@Nonnull Tree tree) {
                for (SecurityConfiguration sc : configs) {
                    if (sc.getContext().definesContextRoot(tree)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean definesTree(@Nonnull Tree tree) {
                for (SecurityConfiguration sc : configs) {
                    if (sc.getContext().definesTree(tree)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean definesLocation(@Nonnull TreeLocation location) {
                for (SecurityConfiguration sc : configs) {
                    if (sc.getContext().definesLocation(location)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }
}
