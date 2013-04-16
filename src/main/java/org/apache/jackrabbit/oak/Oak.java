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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.Executors.newScheduledThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexHookManager;
import org.apache.jackrabbit.oak.plugins.index.IndexHookProvider;
import org.apache.jackrabbit.oak.plugins.observation2.EventQueueWriterProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.OakInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Builder class for constructing {@link ContentRepository} instances with
 * a set of specified plugin components. This class acts as a public facade
 * that hides the internal implementation classes and the details of how
 * they get instantiated and wired together.
 *
 * @since Oak 0.6
 */
public class Oak {

    /**
     * Constant for the default workspace name
     */
    public static final String DEFAULT_WORKSPACE_NAME = "default";

    private final NodeStore store;

    private final List<RepositoryInitializer> initializers = newArrayList();

    private final List<QueryIndexProvider> queryIndexProviders = newArrayList();

    private final List<IndexHookProvider> indexHookProviders = newArrayList();

    private final List<CommitHook> commitHooks = newArrayList();

    private List<EditorProvider> editorProviders = newArrayList();

    private SecurityProvider securityProvider;

    private ScheduledExecutorService executor = newScheduledThreadPool(0);

    private String defaultWorkspaceName = DEFAULT_WORKSPACE_NAME;

    public Oak(NodeStore store) {
        this.store = checkNotNull(store);
    }

    public Oak(MicroKernel kernel) {
        this(new KernelNodeStore(checkNotNull(kernel)));
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
        withEditorHook();
        commitHooks.add(hook);
        return this;
    }

    /**
     * Turns all currently tracked editors to an editor commit hook and
     * associates that hook with the repository to be created. This way
     * a sequence of {@code with()} calls that alternates between editors
     * and other commit hooks will have all the editors in the correct
     * order while still being able to leverage the performance gains of
     * multiple editors iterating over the changes simultaneously.
     */
    private void withEditorHook() {
        if (!editorProviders.isEmpty()) {
            commitHooks.add(new EditorHook(
                    CompositeEditorProvider.compose(editorProviders)));
            editorProviders = newArrayList();
        }
    }

    /**
     * Associates the given editor provider with the repository to be created.
     *
     * @param provider editor provider
     * @return this builder
     */
    @Nonnull
    public Oak with(@Nonnull EditorProvider provider) {
        editorProviders.add(checkNotNull(provider));
        return this;
    }

    /**
     * Associates the given editor with the repository to be created.
     *
     * @param editor editor
     * @return this builder
     */
    @Nonnull
    public Oak with(@Nonnull final Editor editor) {
        checkNotNull(editor);
        return with(new EditorProvider() {
            @Override @Nonnull
            public Editor getRootEditor(
                    NodeState before, NodeState after, NodeBuilder builder) {
                return editor;
            }
        });
    }

    @Nonnull
    public Oak with(@Nonnull SecurityProvider securityProvider) {
        this.securityProvider = checkNotNull(securityProvider);
        initializers.add(securityProvider.getPrivilegeConfiguration().getPrivilegeInitializer());
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
        withEditorHook();
        commitHooks.add(new ConflictHook(conflictHandler));
        return this;
    }

    @Nonnull
    public Oak with(@Nonnull ScheduledExecutorService executorService) {
        this.executor = executorService;
        return this;
    }

    @Nonnull
    public ScheduledExecutorService getExecutorService() {
        return this.executor;
    }

    public ContentRepository createContentRepository() {
        IndexHookProvider indexHooks = CompositeIndexHookProvider.compose(indexHookProviders);
        OakInitializer.initialize(store, new CompositeInitializer(initializers), indexHooks);

        QueryIndexProvider indexProvider = CompositeQueryIndexProvider.compose(queryIndexProviders);

        List<CommitHook> initHooks = new ArrayList<CommitHook>(commitHooks);
        initHooks.add(new EditorHook(CompositeEditorProvider
                .compose(editorProviders)));

        // FIXME: move to proper workspace initialization
        // initialize default workspace
        Iterable<WorkspaceInitializer> workspaceInitializers =
                Iterables.transform(securityProvider.getSecurityConfigurations(),
                        new Function<SecurityConfiguration, WorkspaceInitializer>() {
                            @Override
                            public WorkspaceInitializer apply(SecurityConfiguration sc) {
                                return sc.getWorkspaceInitializer();
                            }
                        });
        OakInitializer.initialize(workspaceInitializers, store,
                defaultWorkspaceName, indexHooks, indexProvider,
                CompositeHook.compose(initHooks));

        // add index hooks later to prevent the OakInitializer to do excessive indexing
        with(IndexHookManager.of(indexHooks));
        with(new EventQueueWriterProvider());
        withEditorHook();
        CommitHook commitHook = CompositeHook.compose(commitHooks);
        return new ContentRepositoryImpl(
                store,
                commitHook,
                defaultWorkspaceName,
                indexProvider,
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