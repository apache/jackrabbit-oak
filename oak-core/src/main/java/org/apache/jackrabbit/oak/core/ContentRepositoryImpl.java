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
import javax.jcr.Credentials;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.AccessControlProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link MicroKernel}-based implementation of
 * the {@link ContentRepository} interface.
 */
public class ContentRepositoryImpl implements ContentRepository {

    /** Logger instance */
    private static final Logger LOG = LoggerFactory.getLogger(ContentRepositoryImpl.class);

    // TODO: retrieve default wsp-name from configuration
    private static final String DEFAULT_WORKSPACE_NAME = "default";

    private final SecurityProvider securityProvider;
    private final QueryIndexProvider indexProvider;
    private final NodeStore nodeStore;
    private final ConflictHandler conflictHandler;

    /**
     * Creates an content repository instance based on the given, already
     * initialized components.
     *
     * @param microKernel   underlying kernel instance
     * @param indexProvider index provider
     * @param commitHook    the commit hook
     * @param securityProvider The configured security provider or {@code null} if
     * default implementations should be used.
     */
    public ContentRepositoryImpl(MicroKernel microKernel,
                                 QueryIndexProvider indexProvider,
                                 CommitHook commitHook,
                                 ConflictHandler conflictHandler,
                                 SecurityProvider securityProvider) {
        this(createNodeStore(microKernel, commitHook), conflictHandler, indexProvider, securityProvider);
    }

    /**
     * Creates an content repository instance based on the given, already
     * initialized components.
     *
     * @param nodeStore the node store this repository is based upon.
     * @param indexProvider index provider
     * @param securityProvider The configured security provider or {@code null} if
     * default implementations should be used.
     */
    public ContentRepositoryImpl(NodeStore nodeStore,
                                 ConflictHandler conflictHandler,
                                 QueryIndexProvider indexProvider,
                                 SecurityProvider securityProvider) {
        this.nodeStore = nodeStore;
        this.conflictHandler = conflictHandler;
        this.indexProvider = indexProvider != null ? indexProvider : new CompositeQueryIndexProvider();
        this.securityProvider = securityProvider;
    }

    @Nonnull
    @Override
    public ContentSession login(Credentials credentials, String workspaceName)
            throws LoginException, NoSuchWorkspaceException {
        if (workspaceName == null) {
            workspaceName = DEFAULT_WORKSPACE_NAME;
        }

        // TODO: support multiple workspaces. See OAK-118
        if (!DEFAULT_WORKSPACE_NAME.equals(workspaceName)) {
            throw new NoSuchWorkspaceException(workspaceName);
        }

        LoginContextProvider lcProvider = securityProvider.getLoginContextProvider(nodeStore);
        LoginContext loginContext = lcProvider.getLoginContext(credentials, workspaceName);
        loginContext.login();

        AccessControlProvider acProvider = securityProvider.getAccessControlProvider();
        return new ContentSessionImpl(loginContext, acProvider, workspaceName,
                nodeStore, conflictHandler, indexProvider);
    }

    //--------------------------------------------------------------------------
    private static NodeStore createNodeStore(MicroKernel microKernel, CommitHook commitHook) {
        KernelNodeStore nodeStore = new KernelNodeStore(microKernel);
        commitHook = new CompositeHook(commitHook, new OrderedChildrenEditor());
        nodeStore.setHook(commitHook);
        return nodeStore;
    }
}
