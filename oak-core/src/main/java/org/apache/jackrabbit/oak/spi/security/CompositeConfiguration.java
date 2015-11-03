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
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.MoveTracker;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeWorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;

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
    private final CompositeContext ctx = new CompositeContext();

    private T defaultConfig;

    public CompositeConfiguration(@Nonnull String name, @Nonnull SecurityProvider securityProvider) {
        this.name = name;
        this.securityProvider = securityProvider;
    }

    @CheckForNull
    public T getDefaultConfig() {
        return defaultConfig;
    }

    public void setDefaultConfig(@Nonnull T defaultConfig) {
        this.defaultConfig = defaultConfig;
        ctx.defaultCtx = defaultConfig.getContext();
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
        ctx.add(configuration);
    }

    public void removeConfiguration(@Nonnull T configuration) {
        configurations.remove(configuration);
        ctx.refresh(configurations);
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
    public List<? extends CommitHook> getCommitHooks(@Nonnull final String workspaceName) {
        return ImmutableList.copyOf(Iterables.concat(Lists.transform(getConfigurations(), new Function<T, List<? extends CommitHook>>() {
            @Override
            public List<? extends CommitHook> apply(T securityConfiguration) {
                return securityConfiguration.getCommitHooks(workspaceName);
            }
        })));
    }

    @Nonnull
    @Override
    public List<? extends ValidatorProvider> getValidators(@Nonnull final String workspaceName, @Nonnull final Set<Principal> principals, @Nonnull final MoveTracker moveTracker) {
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

    @Nonnull
    @Override
    public Context getContext() {
        return ctx;
    }

    private static final class CompositeContext implements Context {

        @Nonnull
        private Context defaultCtx = DEFAULT;
        @Nullable
        private Context[] delegatees = null;

        private void refresh(@Nonnull List<? extends SecurityConfiguration> configurations) {
            Set<Context> s = Sets.newLinkedHashSetWithExpectedSize(configurations.size());
            for (Context c : Iterables.transform(configurations, ContextFunction.INSTANCE)) {
                if (DEFAULT != c) {
                    s.add(c);
                }
            }
            delegatees = (s.isEmpty()) ? null : s.toArray(new Context[s.size()]);
        }

        private void add(@Nonnull SecurityConfiguration configuration) {
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
        public boolean definesProperty(@Nonnull Tree parent, @Nonnull PropertyState property) {
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
        public boolean definesContextRoot(@Nonnull Tree tree) {
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
        public boolean definesTree(@Nonnull Tree tree) {
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
        public boolean definesLocation(@Nonnull TreeLocation location) {
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
