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
package org.apache.jackrabbit.oak.core;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * {@code MicroKernel}-based implementation of
 * the {@link ContentRepository} interface.
 */
public class ContentRepositoryImpl implements ContentRepository {

    private final String defaultWorkspaceName;
    private final SecurityProvider securityProvider;
    private final QueryIndexProvider indexProvider;
    private final NodeStore nodeStore;
    private final CommitHook commitHook;

    /**
     * Creates an content repository instance based on the given, already
     * initialized components.
     *
     * @param nodeStore            the node store this repository is based upon.
     * @param commitHook           the hook to use for processing commits
     * @param defaultWorkspaceName the default workspace name;
     * @param indexProvider        index provider
     * @param securityProvider     The configured security provider.
     */
    public ContentRepositoryImpl(@Nonnull NodeStore nodeStore,
                                 @Nonnull CommitHook commitHook,
                                 @Nonnull String defaultWorkspaceName,
                                 @Nullable QueryIndexProvider indexProvider,
                                 @Nonnull SecurityProvider securityProvider) {
        this.nodeStore = nodeStore;
        this.commitHook = commitHook;
        this.defaultWorkspaceName = defaultWorkspaceName;
        this.indexProvider = indexProvider != null ? indexProvider : new CompositeQueryIndexProvider();
        this.securityProvider = securityProvider;
    }

    @Nonnull
    @Override
    public ContentSession login(Credentials credentials, String workspaceName)
            throws LoginException, NoSuchWorkspaceException {
        if (workspaceName == null) {
            workspaceName = defaultWorkspaceName;
        }

        // TODO: support multiple workspaces. See OAK-118
        if (!defaultWorkspaceName.equals(workspaceName)) {
            throw new NoSuchWorkspaceException(workspaceName);
        }

        LoginContextProvider lcProvider = securityProvider.getAuthenticationConfiguration().getLoginContextProvider(nodeStore, commitHook, indexProvider);
        LoginContext loginContext = lcProvider.getLoginContext(credentials, workspaceName);
        loginContext.login();

        return new ContentSessionImpl(loginContext, securityProvider, workspaceName,
                nodeStore, commitHook, indexProvider);
    }

    public NodeStore getNodeStore() {
        return nodeStore;
    }

}
