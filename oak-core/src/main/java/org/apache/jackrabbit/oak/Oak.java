/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak;

import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexHookManager;
import org.apache.jackrabbit.oak.plugins.index.IndexHookProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ValidatingHook;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.OakInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;

/**
 * Builder class for constructing {@link ContentRepository} instances with
 * a set of specified plugin components. This class acts as a public facade
 * that hides the internal implementation classes and the details of how
 * they get instantiated and wired together.
 *
 * @since Oak 0.6
 */
public class Oak {

    private final MicroKernel kernel;

    private final List<RepositoryInitializer> initializers = newArrayList();

    private final List<QueryIndexProvider> queryIndexProviders = newArrayList();

    private final List<IndexHookProvider> indexHookProviders = newArrayList();

    private final List<CommitHook> commitHooks = newArrayList();

    private List<ValidatorProvider> validatorProviders = newArrayList();

    private List<CommitHook> securityHooks = newArrayList();

    // TODO: review if we really want to have the OpenSecurityProvider as default.
    private SecurityProvider securityProvider = new OpenSecurityProvider();

    private ConflictHandler conflictHandler;

    private String defaultWorkspaceName;

    public Oak(MicroKernel kernel) {
        this.kernel = kernel;
    }

    public Oak() {
        this(new MicroKernelImpl());
    }

    /**
     * Sets the default workspace name that should be used in case of login
     * with {@code null} workspace name. If this method has not been called
     * some internal default value will be used.
     *
     * @param defaultWorkspaceName The name of the default workspace.
     * @return this builder.
     */
    @Nonnull
    public Oak with(@Nonnull String defaultWorkspaceName) {
        this.defaultWorkspaceName = defaultWorkspaceName;
        return this;
    }

    @Nonnull
    public Oak with(@Nonnull RepositoryInitializer initializer) {
        initializers.add(checkNotNull(initializer));
        return this;
    }

    /**
     * Associates the given query index provider with the repository to
     * be created.
     *
     * @param provider query index provider
     * @return this builder
     */
    @Nonnull
    public Oak with(@Nonnull QueryIndexProvider provider) {
        queryIndexProviders.add(provider);
        return this;
    }

    /**
     * Associates the given index hook provider with the repository to
     * be created.
     *
     * @param provider index hook provider
     * @return this builder
     */
    @Nonnull
    public Oak with(@Nonnull IndexHookProvider provider) {
        indexHookProviders.add(provider);
        return this;
    }

    /**
     * Associates the given commit hook with the repository to be created.
     *
     * @param hook commit hook
     * @return this builder
     */
    @Nonnull
    public Oak with(@Nonnull CommitHook hook) {
        withValidatorHook();
        commitHooks.add(hook);
        return this;
    }

    /**
     * Turns all currently tracked validators to a validating commit hook
     * and associates that hook with the repository to be created. This way
     * a sequence of {@code with()} calls that alternates between validators
     * and other commit hooks will have all the validators in the correct
     * order while still being able to leverage the performance gains of
     * multiple validators iterating over the changes simultaneously.
     */
    private void withValidatorHook() {
        if (!validatorProviders.isEmpty()) {
            commitHooks.add(new ValidatingHook(
                    CompositeValidatorProvider.compose(validatorProviders)));
            validatorProviders = newArrayList();
        }
    }

    /**
     * Adds all currently tracked security related hooks to the commit hook that
     * is used to create the content repository.
     */
    private void withSecurityHooks() {
        if (!securityHooks.isEmpty()) {
            commitHooks.addAll(securityHooks);
            securityHooks = newArrayList();
        }
    }

    /**
     * Associates the given validator provider with the repository to
     * be created.
     *
     * @param provider validator provider
     * @return this builder
     */
    @Nonnull
    public Oak with(@Nonnull ValidatorProvider provider) {
        validatorProviders.add(provider);
        return this;
    }

    /**
     * Associates the given validator with the repository to be created.
     *
     * @param validator validator
     * @return this builder
     */
    @Nonnull
    public Oak with(@Nonnull final Validator validator) {
        return with(new ValidatorProvider() {
            @Override
            @Nonnull
            public Validator getRootValidator(
                    NodeState before, NodeState after) {
                return validator;
            }
        });
    }

    @Nonnull
    public Oak with(@Nonnull SecurityProvider securityProvider) {
        this.securityProvider = securityProvider;
        for (SecurityConfiguration sc : securityProvider.getSecurityConfigurations()) {
            validatorProviders.addAll(sc.getValidatorProviders());
            securityHooks.addAll(sc.getCommitHooks());
            initializers.add(sc.getRepositoryInitializer());
        }
        return this;
    }

    /**
     * Associates the given conflict handler with the repository to be created.
     *
     * @param conflictHandler conflict handler
     * @return this builder
     */
    @Nonnull
    public Oak with(@Nonnull ConflictHandler conflictHandler) {
        this.conflictHandler = conflictHandler;
        return this;
    }

    public ContentRepository createContentRepository() {
        KernelNodeStore store = new KernelNodeStore(kernel);

        IndexHookProvider indexHooks = CompositeIndexHookProvider
                .compose(indexHookProviders);
        OakInitializer.initialize(store,
                new CompositeInitializer(initializers), indexHooks);

        commitHooks.add(IndexHookManager.of(indexHooks));

        withValidatorHook();
        withSecurityHooks();
        store.setHook(CompositeHook.compose(commitHooks));

        return new ContentRepositoryImpl(
                store,
                defaultWorkspaceName,
                conflictHandler,
                CompositeQueryIndexProvider.compose(queryIndexProviders),
                securityProvider);
    }

    /**
     * Creates a content repository with the given configuration
     * and logs in to the default workspace with no credentials,
     * returning the resulting content session.
     * <p/>
     * This method exists mostly as a convenience for one-off tests,
     * as there's no way to create other sessions for accessing the
     * same repository.
     * <p/>
     * There is typically no need to explicitly close the returned
     * session unless the repository has explicitly been configured
     * to reserve some resources until all sessions have been closed.
     * The repository will be garbage collected once the session is no
     * longer used.
     *
     * @return content session
     */
    public ContentSession createContentSession() {
        try {
            return createContentRepository().login(null, null);
        } catch (NoSuchWorkspaceException e) {
            throw new IllegalStateException("Default workspace not found", e);
        } catch (LoginException e) {
            throw new IllegalStateException("Anonymous login not allowed", e);
        }
    }

    /**
     * Creates a content repository with the given configuration
     * and returns a {@link Root} instance after logging in to the
     * default workspace with no credentials.
     * <p/>
     * This method exists mostly as a convenience for one-off tests, as
     * the returned root is the only way to access the session or the
     * repository.
     * <p/>
     * Note that since there is no way to close the underlying content
     * session, this method should only be used when no components that
     * require sessions to be closed have been configured. The repository
     * and the session will be garbage collected once the root is no longer
     * used.
     *
     * @return root instance
     */
    public Root createRoot() {
        return createContentSession().getLatestRoot();
    }

}