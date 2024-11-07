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
package org.apache.jackrabbit.oak.jcr;

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler.createJcrConflictHandler;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import javax.jcr.Repository;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.Oak.OakDefaultComponents;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.jmx.SessionMBean;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CompositeConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandlers;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.PartialConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.jetbrains.annotations.NotNull;

/**
 * Builder class which encapsulates the details of building a JCR
 * {@link Repository} backed by an Oak {@link ContentRepository} instance
 *
 * <p>The backing {@linkplain ContentRepository} instance will be constructed with
 * reasonable defaults and additional components can be registered by calling
 * the {@code with} methods. Note that it is not possible to remove components
 * once registered.</p>
 *
 * <p>The Jcr builder is a lazy initializer, to have a working repository make sure
 * you call {@link Jcr#createContentRepository()} or
 * {@link Jcr#createRepository()}.</p>
 */
public class Jcr {
    public static final int DEFAULT_OBSERVATION_QUEUE_LENGTH = BackgroundObserver.DEFAULT_QUEUE_SIZE;

    private final Oak oak;

    private final Set<RepositoryInitializer> repositoryInitializers = new LinkedHashSet<>();
    private final Set<QueryIndexProvider> queryIndexProviders = new LinkedHashSet<>();
    private final Set<CommitHook> commitHooks = new LinkedHashSet<>();
    private final Set<IndexEditorProvider> indexEditorProviders = new LinkedHashSet<>();
    private final Set<EditorProvider> editorProviders = new LinkedHashSet<>();
    private final Set<Editor> editors = new LinkedHashSet<>();
    private final Set<Observer> observers = new LinkedHashSet<>();

    private final CompositeConflictHandler conflictHandler = createJcrConflictHandler();
    private SecurityProvider securityProvider;
    private CommitRateLimiter commitRateLimiter;
    private ScheduledExecutorService scheduledExecutor;
    private Executor executor;
    private QueryLimits queryEngineSettings;
    private String defaultWorkspaceName;
    private Whiteboard whiteboard;

    private int observationQueueLength = DEFAULT_OBSERVATION_QUEUE_LENGTH;
    private boolean fastQueryResultSize;
    private boolean createSessionMBeans = true; // by default every (long-living) session will register an MBean

    private ContentRepository contentRepository;
    private Repository repository;

    private Clusterable clusterable;

    public Jcr(Oak oak, boolean initialize) {
        this.oak = oak;
        if (initialize) {
            OakDefaultComponents defs = new OakDefaultComponents();
            with(defs.securityProvider());
            for (CommitHook ch : defs.commitHooks()) {
                with(ch);
            }
            for (RepositoryInitializer ri : defs.repositoryInitializers()) {
                with(ri);
            }
            for (EditorProvider ep : defs.editorProviders()) {
                with(ep);
            }
            for (IndexEditorProvider iep : defs.indexEditorProviders()) {
                with(iep);
            }
            for (QueryIndexProvider qip : defs.queryIndexProviders()) {
                with(qip);
            }
        }
    }

    public Jcr(Oak oak) {
        this(oak, true);
    }

    public Jcr() {
        this(new Oak());
    }

    public Jcr(NodeStore store) {
        this(new Oak(store));
    }

    @NotNull
    public Jcr with(@NotNull Clusterable c) {
        ensureRepositoryIsNotCreated();
        this.clusterable = requireNonNull(c);
        return this;
    }

    @NotNull
    public final Jcr with(@NotNull RepositoryInitializer initializer) {
        ensureRepositoryIsNotCreated();
        repositoryInitializers.add(requireNonNull(initializer));
        return this;
    }

    public Jcr withAtomicCounter() {
        ensureRepositoryIsNotCreated();
        oak.withAtomicCounter();
        return this;
    }

    private void ensureRepositoryIsNotCreated() {
        Validate.checkState(repository == null && contentRepository == null,
                "Repository was already created");
    }

    @NotNull
    public final Jcr with(@NotNull QueryIndexProvider provider) {
        ensureRepositoryIsNotCreated();
        queryIndexProviders.add(requireNonNull(provider));
        return this;
    }

    @NotNull
    public final Jcr with(@NotNull IndexEditorProvider indexEditorProvider) {
        ensureRepositoryIsNotCreated();
        indexEditorProviders.add(requireNonNull(indexEditorProvider));
        return this;
    }

    @NotNull
    public final Jcr with(@NotNull CommitHook hook) {
        ensureRepositoryIsNotCreated();
        commitHooks.add(requireNonNull(hook));
        return this;
    }

    @NotNull
    public final Jcr with(@NotNull EditorProvider provider) {
        ensureRepositoryIsNotCreated();
        editorProviders.add(requireNonNull(provider));
        return this;
    }

    @NotNull
    public final Jcr with(@NotNull Editor editor) {
        ensureRepositoryIsNotCreated();
        editors.add(requireNonNull(editor));
        return this;
    }

    @NotNull
    public final Jcr with(@NotNull SecurityProvider securityProvider) {
        ensureRepositoryIsNotCreated();
        this.securityProvider = requireNonNull(securityProvider);
        return this;
    }

    /**
     * @deprecated Use {@link #with(ThreeWayConflictHandler)} instead
     */
    @Deprecated
    @NotNull
    public final Jcr with(@NotNull PartialConflictHandler conflictHandler) {
        return with(ConflictHandlers.wrap(conflictHandler));
    }

    @NotNull
    public final Jcr with(@NotNull ThreeWayConflictHandler conflictHandler) {
        ensureRepositoryIsNotCreated();
        this.conflictHandler.addHandler(requireNonNull(conflictHandler));
        return this;
    }

    @NotNull
    public final Jcr with(@NotNull ScheduledExecutorService executor) {
        ensureRepositoryIsNotCreated();
        this.scheduledExecutor = requireNonNull(executor);
        return this;
    }

    @NotNull
    public final Jcr with(@NotNull Executor executor) {
        ensureRepositoryIsNotCreated();
        this.executor = requireNonNull(executor);
        return this;
    }

    @NotNull
    public final Jcr with(@NotNull Observer observer) {
        ensureRepositoryIsNotCreated();
        observers.add(requireNonNull(observer));
        return this;
    }

    /**
     * @deprecated Use {@link #withAsyncIndexing(String, long)} instead
     */
    @NotNull
    @Deprecated
    public Jcr withAsyncIndexing() {
        ensureRepositoryIsNotCreated();
        oak.withAsyncIndexing();
        return this;
    }

    @NotNull
    public Jcr withAsyncIndexing(@NotNull String name, long delayInSeconds) {
        ensureRepositoryIsNotCreated();
        oak.withAsyncIndexing(name, delayInSeconds);
        return this;
    }

    @NotNull
    public Jcr withObservationQueueLength(int observationQueueLength) {
        ensureRepositoryIsNotCreated();
        this.observationQueueLength = observationQueueLength;
        return this;
    }

    @NotNull
    public Jcr with(@NotNull CommitRateLimiter commitRateLimiter) {
        ensureRepositoryIsNotCreated();
        this.commitRateLimiter = requireNonNull(commitRateLimiter);
        return this;
    }

    @NotNull
    public Jcr with(@NotNull QueryLimits qs) {
        ensureRepositoryIsNotCreated();
        this.queryEngineSettings = requireNonNull(qs);
        return this;
    }

    @NotNull
    public Jcr withFastQueryResultSize(boolean fastQueryResultSize) {
        ensureRepositoryIsNotCreated();
        this.fastQueryResultSize = fastQueryResultSize;
        return this;
    }

    @NotNull
    public Jcr with(@NotNull String defaultWorkspaceName) {
        ensureRepositoryIsNotCreated();
        this.defaultWorkspaceName = requireNonNull(defaultWorkspaceName);
        return this;
    }

    @NotNull
    public Jcr with(@NotNull Whiteboard whiteboard) {
        ensureRepositoryIsNotCreated();
        this.whiteboard = requireNonNull(whiteboard);
        return this;
    }

    /**
     * Disables registration of {@link SessionMBean} for every open Session in the repository.
     * This gets rid of some overhead for cases where MBeans are not leveraged.
     * @return the Jcr object
     * @since 1.46
     */
    @NotNull
    public Jcr withoutSessionMBeans() {
        createSessionMBeans = false;
        return this;
    }

    private void setUpOak() {
        // whiteboard
        if (whiteboard != null) {
            oak.with(whiteboard);
        }

        // repository initializers
        for (RepositoryInitializer repositoryInitializer : repositoryInitializers) {
            oak.with(repositoryInitializer);
        }

        // query index providers
        for (QueryIndexProvider queryIndexProvider : queryIndexProviders) {
            oak.with(queryIndexProvider);
        }

        // commit hooks
        for (CommitHook commitHook : commitHooks) {
            oak.with(commitHook);
        }

        // conflict handlers
        oak.with(conflictHandler);

        // index editor providers
        for (IndexEditorProvider indexEditorProvider : indexEditorProviders) {
            oak.with(indexEditorProvider);
        }

        // editors
        for (Editor editor : editors) {
            oak.with(editor);
        }

        // editor providers
        for (EditorProvider editorProvider : editorProviders) {
            oak.with(editorProvider);
        }

        // securityProvider
        oak.with(securityProvider);

        // executors
        if (scheduledExecutor != null) {
            oak.with(scheduledExecutor);
        }
        if (executor != null) {
            oak.with(executor);
        }

        // observers
        for (Observer observer : observers) {
            oak.with(observer);
        }

        // commit rate limiter
        if (commitRateLimiter != null) {
            oak.with(commitRateLimiter);
        }

        // query engine settings
        if (queryEngineSettings != null) {
            oak.with(queryEngineSettings);
        }

        // default workspace name
        if (defaultWorkspaceName != null) {
            oak.with(defaultWorkspaceName);
        }

        if (clusterable != null) {
            oak.with(clusterable);
        }
    }

    @NotNull
    public ContentRepository createContentRepository() {
        if (contentRepository == null) {
            setUpOak();
            contentRepository = oak.createContentRepository();
        }
        return contentRepository;
    }

    @NotNull
    public Repository createRepository() {
        if (repository == null) {
            repository = new RepositoryImpl(
                    createContentRepository(),
                    oak.getWhiteboard(),
                    securityProvider,
                    observationQueueLength,
                    commitRateLimiter,
                    fastQueryResultSize,
                    createSessionMBeans);
        }
        return repository;
    }

}
