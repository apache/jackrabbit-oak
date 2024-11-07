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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.collect.ObjectArrays;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.tree.RootProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.plugins.tree.TreeProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeWorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.stats.Monitor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;
import org.osgi.framework.Constants;

import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * Abstract base implementation for {@link SecurityConfiguration}s that can
 * combine different implementations.
 */
@ProviderType
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

    private final List<T> configurations = new CopyOnWriteArrayList<>();

    private final Ranking rankings = new Ranking();

    private final String name;
    private final CompositeContext ctx = new CompositeContext();

    private SecurityProvider securityProvider;

    private RootProvider rootProvider;

    private TreeProvider treeProvider;

    private T defaultConfig;

    public CompositeConfiguration(@NotNull String name) {
        this.name = name;
    }

    public CompositeConfiguration(@NotNull String name, @NotNull SecurityProvider securityProvider) {
        this.name = name;
        this.securityProvider = securityProvider;
    }

    @Nullable
    public T getDefaultConfig() {
        return defaultConfig;
    }

    public void setDefaultConfig(@NotNull T defaultConfig) {
        this.defaultConfig = defaultConfig;
        ctx.defaultCtx = defaultConfig.getContext();
    }

    public void addConfiguration(@NotNull T configuration) {
        addConfiguration(configuration, ConfigurationParameters.EMPTY);
    }

    public void addConfiguration(@NotNull T configuration, @NotNull ConfigurationParameters params) {
        int ranking = configuration.getParameters().getConfigValue(PARAM_RANKING, NO_RANKING);
        if (ranking == NO_RANKING) {
            ranking = params.getConfigValue(Constants.SERVICE_RANKING, NO_RANKING);
        }
        if (ranking == NO_RANKING || configurations.isEmpty()) {
            configurations.add(configuration);
        } else {
            int i = 0;
            for (T c : configurations) {
                int r = rankings.get(c);
                if (ranking > r) {
                    break;
                } else {
                    i++;
                }
            }
            configurations.add(i, configuration);
        }
        rankings.set(configuration, ranking);
        ctx.add(configuration);
    }

    public void removeConfiguration(@NotNull T configuration) {
        configurations.remove(configuration);
        rankings.remove(configuration);
        ctx.refresh(configurations);
    }

    @NotNull
    public List<T> getConfigurations() {
        if (configurations.isEmpty() && defaultConfig != null) {
            return ImmutableList.of(defaultConfig);
        } else {
            return ImmutableList.copyOf(configurations);
        }
    }

    public void setSecurityProvider(@NotNull SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
    }

    @NotNull
    protected SecurityProvider getSecurityProvider() {
        if (securityProvider == null) {
            throw new IllegalStateException("SecurityProvider missing => CompositeConfiguration is not ready.");
        }
        return securityProvider;
    }

    public void setRootProvider(@NotNull RootProvider rootProvider) {
        this.rootProvider = rootProvider;
    }

    @NotNull
    protected RootProvider getRootProvider() {
        if (rootProvider == null) {
            throw new IllegalStateException("RootProvider missing.");
        }
        return rootProvider;
    }

    public void setTreeProvider(@NotNull TreeProvider treeProvider) {
        this.treeProvider = treeProvider;
    }

    @NotNull
    protected TreeProvider getTreeProvider() {
        if (treeProvider == null) {
            throw new IllegalStateException("TreeProvider missing.");
        }
        return treeProvider;
    }

    //----------------------------------------------< SecurityConfiguration >---
    @NotNull
    @Override
    public String getName() {
        return name;
    }

    @NotNull
    @Override
    public ConfigurationParameters getParameters() {
        List<T> configs = getConfigurations();
        ConfigurationParameters[] params = new ConfigurationParameters[configs.size()];
        for (int i = 0; i < configs.size(); i++) {
            params[i] = configs.get(i).getParameters();
        }
        return ConfigurationParameters.of(params);
    }

    @NotNull
    @Override
    public WorkspaceInitializer getWorkspaceInitializer() {
        return new CompositeWorkspaceInitializer(Lists.transform(getConfigurations(),
                securityConfiguration -> securityConfiguration.getWorkspaceInitializer()));
    }

    @NotNull
    @Override
    public RepositoryInitializer getRepositoryInitializer() {
        return new CompositeInitializer(Lists.transform(getConfigurations(),
                securityConfiguration -> securityConfiguration.getRepositoryInitializer()));
    }

    @NotNull
    @Override
    public List<? extends CommitHook> getCommitHooks(@NotNull final String workspaceName) {
        Iterable<CommitHook> t = Iterables.concat(Lists.transform(getConfigurations(),
                securityConfiguration -> securityConfiguration.getCommitHooks(workspaceName)));
        return ImmutableList.copyOf(t);
    }

    @NotNull
    @Override
    public List<? extends ValidatorProvider> getValidators(@NotNull final String workspaceName, @NotNull final Set<Principal> principals, @NotNull final MoveTracker moveTracker) {
        Iterable<ValidatorProvider> t = Iterables.concat(Lists.transform(getConfigurations(), securityConfiguration -> securityConfiguration.getValidators(workspaceName, principals, moveTracker)));
        return ImmutableList.copyOf(t);
    }

    @NotNull
    @Override
    public List<ThreeWayConflictHandler> getConflictHandlers() {
        return ImmutableList.copyOf(Iterables.concat(Lists.transform(getConfigurations(), securityConfiguration -> securityConfiguration.getConflictHandlers())));
    }

    @NotNull
    @Override
    public List<ProtectedItemImporter> getProtectedItemImporters() {
        Iterable<ProtectedItemImporter> t = Iterables.concat(Lists.transform(getConfigurations(),
                securityConfiguration -> securityConfiguration.getProtectedItemImporters()));
        return ImmutableList.copyOf(t);
    }

    @NotNull
    @Override
    public Context getContext() {
        return ctx;
    }

    @NotNull
    @Override
    public Iterable<Monitor<?>> getMonitors(@NotNull StatisticsProvider statisticsProvider) {
        return Iterables.concat(
                Iterables.transform(getConfigurations(), securityConfiguration -> securityConfiguration.getMonitors(statisticsProvider)));
    }

    private static final class Ranking {

        private Map<SecurityConfiguration, Integer> m = new ConcurrentHashMap<>();

        private int get(@NotNull SecurityConfiguration configuration) {
            Integer ranking = m.get(configuration);
            if (ranking == null) {
                return NO_RANKING;
            } else {
                return ranking;
            }
        }

        private void set(@NotNull SecurityConfiguration configuration, int ranking) {
            if (ranking != NO_RANKING) {
                m.put(configuration, ranking);
            }
        }

        private void remove(@NotNull SecurityConfiguration configuration) {
            m.remove(configuration);
        }
    }

    private static final class CompositeContext implements Context {

        @NotNull
        private Context defaultCtx = DEFAULT;
        @Nullable
        private Context[] delegatees = null;

        private void refresh(@NotNull List<? extends SecurityConfiguration> configurations) {
            Set<Context> s = CollectionUtils.newLinkedHashSet(configurations.size());
            for (Context c : Iterables.transform(configurations, ContextFunction.INSTANCE::apply)) {
                if (DEFAULT != c) {
                    s.add(c);
                }
            }
            delegatees = (s.isEmpty()) ? null : s.toArray(new Context[0]);
        }

        private void add(@NotNull SecurityConfiguration configuration) {
            Context c = configuration.getContext();
            if (DEFAULT != c) {
                if (delegatees == null) {
                    delegatees = new Context[] {c};
                } else {
                    for (Context ctx : delegatees) {
                        if (ctx.equals(c)) {
                            return;
                        }
                    }
                    delegatees = ObjectArrays.concat(delegatees, c);
                }
            }
        }

        @Override
        public boolean definesProperty(@NotNull Tree parent, @NotNull PropertyState property) {
            if (delegatees == null) {
                return defaultCtx.definesProperty(parent, property);
            }
            for (Context ctx : delegatees) {
                if (ctx.definesProperty(parent, property)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean definesContextRoot(@NotNull Tree tree) {
            if (delegatees == null) {
                return defaultCtx.definesContextRoot(tree);
            }
            for (Context ctx : delegatees) {
                if (ctx.definesContextRoot(tree)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean definesTree(@NotNull Tree tree) {
            if (delegatees == null) {
                return defaultCtx.definesTree(tree);
            }
            for (Context ctx : delegatees) {
                if (ctx.definesTree(tree)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean definesLocation(@NotNull TreeLocation location) {
            if (delegatees == null) {
                return defaultCtx.definesLocation(location);
            }
            for (Context ctx : delegatees) {
                if (ctx.definesLocation(location)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean definesInternal(@NotNull Tree tree) {
            if (delegatees == null) {
                return defaultCtx.definesInternal(tree);
            }
            for (Context ctx : delegatees) {
                if (ctx.definesInternal(tree)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static final class ContextFunction implements Function<SecurityConfiguration, Context> {

        private static final ContextFunction INSTANCE = new ContextFunction();

        private ContextFunction() {}

        @Override
        public Context apply(SecurityConfiguration input) {
            return input.getContext();
        }
    }
}
