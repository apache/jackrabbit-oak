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
package org.apache.jackrabbit.oak.plugins.tree.factories;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.core.ImmutableRoot;
import org.apache.jackrabbit.oak.core.SystemRoot;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Factory to obtain immutable {@code Root} objects.
 *
 * @deprecated Please use {@code RootProvider} instead
 */
public final class RootFactory {

    private RootFactory() {}

    @Nonnull
    public static Root createReadOnlyRoot(@Nonnull NodeState rootState) {
        return new ImmutableRoot(rootState);
    }

    @Nonnull
    public static Root createReadOnlyRoot(@Nonnull Root root) {
        return ImmutableRoot.getInstance(root);
    }

    /**
     * @deprecated with Oak 1.7.2 due to the usage of deprecated {@link QueryEngineSettings}
     */
    @Nonnull
    public static Root createSystemRoot(@Nonnull NodeStore store,
                                        @Nullable CommitHook hook,
                                        @Nullable String workspaceName,
                                        @Nullable SecurityProvider securityProvider,
                                        @Nullable QueryEngineSettings queryEngineSettings,
                                        @Nullable QueryIndexProvider indexProvider) {
        return SystemRoot.create(store,
                (hook == null) ? EmptyHook.INSTANCE : hook,
                (workspaceName == null) ? Oak.DEFAULT_WORKSPACE_NAME : workspaceName,
                (securityProvider == null) ? new OpenSecurityProvider() : securityProvider,
                queryEngineSettings,
                (indexProvider == null) ? new CompositeQueryIndexProvider(): indexProvider);

    }

    @Nonnull
    public static Root createSystemRoot(@Nonnull NodeStore store,
                                        @Nullable CommitHook hook,
                                        @Nullable String workspaceName,
                                        @Nullable SecurityProvider securityProvider,
                                        @Nullable QueryIndexProvider indexProvider) {
        return SystemRoot.create(store,
                (hook == null) ? EmptyHook.INSTANCE : hook,
                (workspaceName == null) ? Oak.DEFAULT_WORKSPACE_NAME : workspaceName,
                (securityProvider == null) ? new OpenSecurityProvider() : securityProvider,
                (indexProvider == null) ? new CompositeQueryIndexProvider() : indexProvider);

    }
}