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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.security.auth.login.LoginException;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.io.Closer;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
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
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder class for constructing {@link ContentRepository} instances with
 * a set of specified plugin components. This class acts as a public facade
 * that hides the internal implementation classes and the details of how
 * they get instantiated and wired together.
 *
 * @since Oak 0.6
 */
public class Oak {
    private static final Logger LOG = LoggerFactory.getLogger(Oak.class);

    /**
     * Constant for the default workspace name
     */
    public static final String DEFAULT_WORKSPACE_NAME = "default";

    private final NodeStore store;

    private final List<RepositoryInitializer> initializers = newArrayList();

    private final List<QueryIndexProvider> queryIndexProviders = newArrayList();

    private final List<IndexEditorProvider> indexEditorProviders = newArrayList();

    private final List<CommitHook> commitHooks = newArrayList();

    private List<EditorProvider> editorProviders = newArrayList();

    private SecurityProvider securityProvider;

    private ScheduledExecutorService scheduledExecutor = defaultScheduledExecutor();

    private Executor executor = defaultExecutorService();

    /**
     * Default {@code ScheduledExecutorService} used for scheduling background tasks.
     * This default spawns up to 32 background thread on an as need basis. Idle
     * threads are pruned after one minute.
     * @return  fresh ScheduledExecutorService
     */
    public static ScheduledExecutorService defaultScheduledExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(32, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, createName());
                thread.setDaemon(true);
                return thread;
            }

            private String createName() {
                return "oak-scheduled-executor-" + counter.getAndIncrement();
            }
        });
        executor.setKeepAliveTime(1, TimeUnit.MINUTES);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * Default {@code ExecutorService} used for scheduling concurrent tasks.
     * This default spawns as many threads as required with a priority of
     * {@code Thread.MIN_PRIORITY}. Idle threads are pruned after one minute.
     * @return  fresh ExecutorService
     */
    public static ExecutorService defaultExecutorService() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, createName());
                thread.setDaemon(true);
                thread.setPriority(Thread.MIN_PRIORITY);
                return thread;
            }

            private String createName() {
                return "oak-executor-" + counter.getAndIncrement();
            }
        });
        executor.setKeepAliveTime(1, TimeUnit.MINUTES);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    private MBeanServer mbeanServer;

    private String defaultWorkspaceName = DEFAULT_WORKSPACE_NAME;

    @SuppressWarnings("unchecked")
    private static <T> T getValue(
            Map<?, ?> properties, String name, Class<T> type, T def) {
        Object value = properties.get(name);
        if (type.isInstance(value)) {
            return (T) value;
        } else {
            return def;
        }
    }

    private static <T> T getValue(
            Map<?, ?> properties, String name, Class<T> type) {
        return getValue(properties, name, type, null);
    }

    private Whiteboard whiteboard = new DefaultWhiteboard() {
        @Override
        public <T> Registration register(
                Class<T> type, T service, Map<?, ?> properties) {
            final Registration registration =
                    super.register(type, service, properties);

            final Closer observerSubscription = Closer.create();
            Future<?> future = null;
            if (scheduledExecutor != null && type == Runnable.class) {
                Runnable runnable = (Runnable) service;
                Long period = getValue(properties, "scheduler.period", Long.class);
                if (period != null) {
                    Boolean concurrent = getValue(
                            properties, "scheduler.concurrent",
                            Boolean.class, Boolean.FALSE);
                    if (concurrent) {
                        future = scheduledExecutor.scheduleAtFixedRate(
                                runnable, period, period, TimeUnit.SECONDS);
                    } else {
                        future = scheduledExecutor.scheduleWithFixedDelay(
                                runnable, period, period, TimeUnit.SECONDS);
                    }
                }
            } else if (type == Observer.class && store instanceof Observable) {
                BackgroundObserver backgroundObserver =
                        new BackgroundObserver((Observer) service, executor);
                observerSubscription.register(backgroundObserver);
                observerSubscription.register(((Observable) store).addObserver(backgroundObserver));
            }

            ObjectName objectName = null;
            Object name = properties.get("jmx.objectname");
            if (mbeanServer != null && name != null) {
                try {
                    if (name instanceof ObjectName) {
                        objectName = (ObjectName) name;
                    } else {
                        objectName = new ObjectName(String.valueOf(name));
                    }
                    mbeanServer.registerMBean(service, objectName);
                } catch (JMException e) {
                    // ignore
                }
            }

            final Future<?> f = future;
            final ObjectName on = objectName;
            return new Registration() {
                @Override
                public void unregister() {
                    if (f != null) {
                        f.cancel(false);
                    }
                    if (on != null) {
                        try {
                            mbeanServer.unregisterMBean(on);
                        } catch (JMException e) {
                            // ignore
                        }
                    }
                    try {
                        observerSubscription.close();
                    } catch (IOException e) {
                        LOG.warn("Unexpected IOException while unsubscribing observer", e);
                    }

                    registration.unregister();
                }
            };
        }
    };

    /**
     * Flag controlling the asynchronous indexing behavior. If false (default)
     * there will be no background indexing happening.
     * 
     */
    private boolean asyncIndexing = false;

    public Oak(NodeStore store) {
        this.store = checkNotNull(store);
    }

    public Oak(MicroKernel kernel) {
        this(new KernelNodeStore(checkNotNull(kernel)));
    }

    public Oak() {
        this(new MicroKernelImpl());
        // this(new DocumentMK.Builder().open());
        // this(new LogWrapper(new MicroKernelImpl()));
        // this(new LogWrapper(new DocumentMK.Builder().open()));
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
        this.defaultWorkspaceName = checkNotNull(defaultWorkspaceName);
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
        queryIndexProviders.add(checkNotNull(provider));
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
    public Oak with(@Nonnull IndexEditorProvider provider) {
        indexEditorProviders.add(checkNotNull(provider));
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
        checkNotNull(hook);
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
                    NodeState before, NodeState after,
                    NodeBuilder builder, CommitInfo info) {
                return editor;
            }
        });
    }

    @Nonnull
    public Oak with(@Nonnull SecurityProvider securityProvider) {
        this.securityProvider = checkNotNull(securityProvider);
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            RepositoryInitializer ri = sc.getRepositoryInitializer();
            if (ri != RepositoryInitializer.DEFAULT) {
                initializers.add(ri);
            }
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
        checkNotNull(conflictHandler);
        withEditorHook();
        commitHooks.add(new ConflictHook(conflictHandler));
        return this;
    }

    @Nonnull
    public Oak with(@Nonnull ScheduledExecutorService scheduledExecutor) {
        this.scheduledExecutor = checkNotNull(scheduledExecutor);
        return this;
    }

    @Nonnull
    public Oak with(@Nonnull Executor executor) {
        this.executor = checkNotNull(executor);
        return this;
    }

    @Nonnull
    public Oak with(@Nonnull MBeanServer mbeanServer) {
        this.mbeanServer = checkNotNull(mbeanServer);
        return this;
    }

    @Nonnull
    public Oak with(@Nonnull Whiteboard whiteboard) {
        this.whiteboard = checkNotNull(whiteboard);
        return this;
    }

    /**
     * Enable the asynchronous (background) indexing behavior.
     * 
     * Please not that when enabling the background indexer, you need to take
     * care of calling
     * <code>#shutdown<code> on the <code>executor<code> provided for this Oak instance.
     * 
     */
    public Oak withAsyncIndexing() {
        this.asyncIndexing = true;
        return this;
    }

    @Nonnull
    public Whiteboard getWhiteboard() {
        return this.whiteboard;
    }

    public ContentRepository createContentRepository() {
        IndexEditorProvider indexEditors = CompositeIndexEditorProvider.compose(indexEditorProviders);
        OakInitializer.initialize(store, new CompositeInitializer(initializers), indexEditors);

        QueryIndexProvider indexProvider = CompositeQueryIndexProvider.compose(queryIndexProviders);

        List<CommitHook> initHooks = new ArrayList<CommitHook>(commitHooks);
        initHooks.add(new EditorHook(CompositeEditorProvider
                .compose(editorProviders)));

        if (asyncIndexing) {
            String name = "async";
            AsyncIndexUpdate task = new AsyncIndexUpdate(name, store,
                    indexEditors);
            WhiteboardUtils.scheduleWithFixedDelay(whiteboard, task, 5, true);
            WhiteboardUtils.registerMBean(whiteboard, IndexStatsMBean.class,
                    task.getIndexStats(), IndexStatsMBean.TYPE, name);
        }

        // FIXME: OAK-810 move to proper workspace initialization
        // initialize default workspace
        Iterable<WorkspaceInitializer> workspaceInitializers =
                Iterables.transform(securityProvider.getConfigurations(),
                        new Function<SecurityConfiguration, WorkspaceInitializer>() {
                            @Override
                            public WorkspaceInitializer apply(SecurityConfiguration sc) {
                                return sc.getWorkspaceInitializer();
                            }
                        });
        OakInitializer.initialize(workspaceInitializers, store,
                defaultWorkspaceName, indexEditors, indexProvider,
                CompositeHook.compose(initHooks));

        // add index hooks later to prevent the OakInitializer to do excessive indexing
        with(new IndexUpdateProvider(indexEditors));
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