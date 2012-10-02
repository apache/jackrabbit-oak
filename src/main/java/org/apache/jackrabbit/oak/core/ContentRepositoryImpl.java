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
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandlerProvider;
import org.apache.jackrabbit.oak.query.QueryEngineImpl;
import org.apache.jackrabbit.oak.security.authentication.LoginContextProviderImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandlerProvider;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatingHook;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContextProvider;
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

    private final LoginContextProvider loginContextProvider;
    private final QueryEngineImpl queryEngine;
    private final KernelNodeStore nodeStore;

    /**
     * Utility constructor that creates a new in-memory repository with default
     * query index provider. This constructor is intended to be used within
     * test cases only.
     */
    public ContentRepositoryImpl() {
        this(new CompositeHook());
    }

    public ContentRepositoryImpl(CommitHook hook) {
        this(new MicroKernelImpl(), new LoginContextProviderImpl(),
                new CompositeQueryIndexProvider(), hook);
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
        this(microKernel, new LoginContextProviderImpl(), indexProvider,
                new ValidatingHook(validatorProvider != null
                    ? validatorProvider : DefaultValidatorProvider.INSTANCE));
    }

    public ContentRepositoryImpl(
            MicroKernel microKernel, ValidatorProvider validatorProvider) {
        this(microKernel, null, validatorProvider);
    }

    /**
     * Creates an Oak repository instance based on the given, already
     * initialized components.
     *
     * @param microKernel underlying kernel instance
     * @param loginContextProvider login context provider
     * @param indexProvider index provider
     * @param commitHook the commit hook
     */
    public ContentRepositoryImpl(
            MicroKernel microKernel, LoginContextProvider loginContextProvider,
            QueryIndexProvider indexProvider, CommitHook commitHook) {

        nodeStore = new KernelNodeStore(microKernel);
        nodeStore.setHook(commitHook);

        QueryIndexProvider qip = indexProvider != null ? indexProvider
                : new CompositeQueryIndexProvider();
        queryEngine = new QueryEngineImpl(nodeStore, qip);

        this.loginContextProvider = loginContextProvider;
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

        LoginContext loginContext = loginContextProvider.getLoginContext(credentials, workspaceName);
        loginContext.login();

        return new ContentSessionImpl(loginContext, workspaceName, nodeStore, DEFAULT_CONFLICT_HANDLER_PROVIDER,
                queryEngine);
    }
}
