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
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandlerProvider;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandlerProvider;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatingHook;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
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

    private static final ConflictHandlerProvider DEFAULT_CONFLICT_HANDLER_PROVIDER =
            new AnnotatingConflictHandlerProvider();

    private final SecurityProvider securityProvider;
    private final QueryIndexProvider indexProvider;
    private final NodeStore nodeStore;

    /**
     * Utility constructor that creates a new in-memory repository with default
     * query index provider. This constructor is intended to be used within
     * test cases only.
     */
    public ContentRepositoryImpl() {
        this(new CompositeHook());
    }

    public ContentRepositoryImpl(CommitHook hook) {
        this(new MicroKernelImpl(), new CompositeQueryIndexProvider(), hook, null);
    }

    /**
     * Utility constructor, intended to be used within test cases only.
     *
     * Creates an Oak repository instance based on the given, already
     * initialized components.
     *
     * @param microKernel
     *            underlying kernel instance
     * @param indexProvider
     *            index provider
     * @param validatorProvider
     *            the validation provider
     */
    public ContentRepositoryImpl(
            MicroKernel microKernel, QueryIndexProvider indexProvider,
            ValidatorProvider validatorProvider) {
        this(microKernel, indexProvider,
                new ValidatingHook(validatorProvider != null ? validatorProvider : DefaultValidatorProvider.INSTANCE),
                null);
    }

    public ContentRepositoryImpl(MicroKernel microKernel, ValidatorProvider validatorProvider) {
        this(microKernel, null, validatorProvider);
    }

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
                                 SecurityProvider securityProvider) {
        this(createNodeStore(microKernel, commitHook), indexProvider, securityProvider);
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
                                 QueryIndexProvider indexProvider,
                                 SecurityProvider securityProvider) {
        this.nodeStore = nodeStore;
        this.indexProvider = indexProvider != null ? indexProvider : new CompositeQueryIndexProvider();

        // TODO: in order not to having failing tests we use SecurityProviderImpl as default
        //       - review if passing a security provider should be mandatory
        //       - review if another default (not enforcing any security constraint) was more appropriate.
        this.securityProvider = (securityProvider == null) ? new SecurityProviderImpl() : securityProvider;
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
                nodeStore, DEFAULT_CONFLICT_HANDLER_PROVIDER, indexProvider);
    }

    //--------------------------------------------------------------------------
    private static NodeStore createNodeStore(MicroKernel microKernel, CommitHook commitHook) {
        KernelNodeStore nodeStore = new KernelNodeStore(microKernel);
        nodeStore.setHook(commitHook);
        return nodeStore;
    }
}
