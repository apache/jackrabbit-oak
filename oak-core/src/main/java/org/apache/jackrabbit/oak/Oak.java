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
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import javax.management.StandardMBean;
import javax.security.auth.login.LoginException;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.jmx.QueryEngineSettingsMBean;
import org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.core.ContentRepositoryImpl;
import org.apache.jackrabbit.oak.management.RepositoryManager;
import org.apache.jackrabbit.oak.plugins.atomic.AtomicCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexMBeanRegistration;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounter;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounterMBean;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounterOld;
import org.apache.jackrabbit.oak.plugins.index.nodetype.NodeTypeIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedPropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.property.jmx.PropertyIndexAsyncReindex;
import org.apache.jackrabbit.oak.plugins.index.property.jmx.PropertyIndexAsyncReindexMBean;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceIndexProvider;
import org.apache.jackrabbit.oak.plugins.itemsave.ItemSaveValidatorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.name.NameValidatorProvider;
import org.apache.jackrabbit.oak.plugins.name.NamespaceEditorProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditorProvider;
import org.apache.jackrabbit.oak.plugins.observation.ChangeCollectorProvider;
import org.apache.jackrabbit.oak.plugins.version.VersionHook;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.stats.QueryStatsMBean;
import org.apache.jackrabbit.oak.security.SecurityProviderImpl;
import org.apache.jackrabbit.oak.spi.commit.CompositeConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandler;
import org.apache.jackrabbit.oak.spi.commit.ConflictHandlers;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.ThreeWayConflictHandler;
import org.apache.jackrabbit.oak.spi.lifecycle.CompositeInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.lifecycle.WorkspaceInitializer;
import org.apache.jackrabbit.oak.spi.query.CompositeQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProviderAware;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.spi.security.SecurityConfiguration;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardAware;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.spi.descriptors.AggregatingDescriptors;
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

    private AnnotatedQueryEngineSettings queryEngineSettings = new AnnotatedQueryEngineSettings();

    private final List<QueryIndexProvider> queryIndexProviders = newArrayList();

    private final List<IndexEditorProvider> indexEditorProviders = newArrayList();

    private final List<CommitHook> commitHooks = newArrayList();

    private final List<Observer> observers = Lists.newArrayList();

    private List<EditorProvider> editorProviders = newArrayList();

    private CompositeConflictHandler conflictHandler;

    private SecurityProvider securityProvider;

    private ScheduledExecutorService scheduledExecutor;

    private Executor executor;

    private final Closer closer = Closer.create();

    private ContentRepository contentRepository;
    
    private Clusterable clusterable;

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
            public Thread newThread(@Nonnull Runnable r) {
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
            public Thread newThread(@Nonnull Runnable r) {
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

    private synchronized ScheduledExecutorService getScheduledExecutor() {
        if (scheduledExecutor == null) {
            scheduledExecutor = defaultScheduledExecutor();
            closer.register(new ExecutorCloser(scheduledExecutor));
        }
        return scheduledExecutor;
    }

    private synchronized Executor getExecutor() {
        if (executor == null) {
            ExecutorService executorService = defaultExecutorService();
            executor = executorService;
            closer.register(new ExecutorCloser(executorService));
        }
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
                final Class<T> type, T service, Map<?, ?> properties) {
            final Registration registration =
                    super.register(type, service, properties);

            final Closer observerSubscription = Closer.create();
            Future<?> future = null;
            if (type == Runnable.class) {
                Runnable runnable = (Runnable) service;
                Long period = getValue(properties, "scheduler.period", Long.class);
                if (period != null) {
                    Boolean concurrent = getValue(
                            properties, "scheduler.concurrent",
                            Boolean.class, Boolean.FALSE);
                    if (concurrent) {
                        future = getScheduledExecutor().scheduleAtFixedRate(
                                runnable, period, period, TimeUnit.SECONDS);
                    } else {
                        future = getScheduledExecutor().scheduleWithFixedDelay(
                                runnable, period, period, TimeUnit.SECONDS);
                    }
                }
            } else if (type == Observer.class && store instanceof Observable) {
                observerSubscription.register(((Observable) store).addObserver((Observer) service));
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

                    if (type.getName().equals(service.getClass().getName().concat("MBean"))
                            || service instanceof StandardMBean){
                        mbeanServer.registerMBean(service, objectName);
                    } else {
                        //Wrap the MBean in std MBean
                        mbeanServer.registerMBean(new StandardMBean(service, type), objectName);
                    }

                } catch (JMException e) {
                    LOG.warn("Unexpected exception while registering MBean of type [{}] " +
                            "against name [{}]", type, objectName, e);
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
                            LOG.warn("Unexpected exception while unregistering MBean of type {} " +
                                    "against name {} ", type, on, e);
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
     * Map containing the (names -> delayInSecods) of the background indexing
     * tasks that need to be started with this repository. A {@code null} value
     * means no background tasks will run.
     */
    private Map<String, Long> asyncTasks;

    private boolean failOnMissingIndexProvider;

    public Oak(NodeStore store) {
        this.store = checkNotNull(store);
    }

    public Oak() {
        this(new MemoryNodeStore());
        // this(new DocumentMK.Builder().open());
        // this(new LogWrapper(new DocumentMK.Builder().open()));
    }
    
    /**
     * Define the current repository as being a {@link Clusterable} one.
     * 
     * @param c
     * @return
     */
    @Nonnull
    public Oak with(@Nonnull Clusterable c) {
        this.clusterable = checkNotNull(c);
        return this;
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

    @Nonnull
    public Oak with(@Nonnull QueryLimits settings) {
        QueryEngineSettings s = new QueryEngineSettings();
        s.setFailTraversal(settings.getFailTraversal());
        s.setFullTextComparisonWithoutIndex(settings.getFullTextComparisonWithoutIndex());
        s.setLimitInMemory(settings.getLimitInMemory());
        s.setLimitReads(settings.getLimitReads());
        this.queryEngineSettings = new AnnotatedQueryEngineSettings(s);
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
        if (securityProvider instanceof WhiteboardAware) {
            ((WhiteboardAware) securityProvider).setWhiteboard(whiteboard);
        }
        for (SecurityConfiguration sc : securityProvider.getConfigurations()) {
            RepositoryInitializer ri = sc.getRepositoryInitializer();
            if (ri != RepositoryInitializer.DEFAULT) {
                initializers.add(ri);
            }

            for (ThreeWayConflictHandler tch : sc.getConflictHandlers()) {
                with(tch);
            }
        }
        return this;
    }

    /**
     * Associates the given conflict handler with the repository to be created.
     *
     * @param conflictHandler conflict handler
     * @return this builder
     * @deprecated Use {@link #with(ThreeWayConflictHandler)} instead
     */
    @Deprecated
    @Nonnull
    public Oak with(@Nonnull ConflictHandler conflictHandler) {
        return with(ConflictHandlers.wrap(conflictHandler));
    }

    @Nonnull
    public Oak with(@Nonnull ThreeWayConflictHandler conflictHandler) {
        checkNotNull(conflictHandler);
        withEditorHook();

        if (this.conflictHandler == null) {
            if (conflictHandler instanceof CompositeConflictHandler) {
                this.conflictHandler = (CompositeConflictHandler) conflictHandler;
            } else {
                this.conflictHandler = new CompositeConflictHandler();
                this.conflictHandler.addHandler(conflictHandler);
            }
            commitHooks.add(new ConflictHook(conflictHandler));
        } else {
            this.conflictHandler.addHandler(conflictHandler);
        }
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
        if (securityProvider instanceof WhiteboardAware) {
            ((WhiteboardAware) securityProvider).setWhiteboard(whiteboard);
        }
        QueryEngineSettings queryEngineSettings = WhiteboardUtils.getService(whiteboard, QueryEngineSettings.class);
        if (queryEngineSettings != null) {
            this.queryEngineSettings = new AnnotatedQueryEngineSettings(queryEngineSettings);
        }
        return this;
    }

    @Nonnull
    public Oak with(@Nonnull Observer observer) {
        observers.add(checkNotNull(observer));
        return this;
    }

    /**
     * <p>
     * Enable the asynchronous (background) indexing behavior.
     * </p>
     * <p>
     * Please note that when enabling the background indexer, you need to take
     * care of calling
     * <code>#shutdown</code> on the <code>executor</code> provided for this Oak instance.
     * </p>
     * @deprecated Use {@link Oak#withAsyncIndexing(String, long)} instead
     */
    @Deprecated
    public Oak withAsyncIndexing() {
        return withAsyncIndexing("async", 5);
    }

    public Oak withFailOnMissingIndexProvider(){
        failOnMissingIndexProvider = true;
        return this;
    }

    public Oak withAtomicCounter() {
        return with(new AtomicCounterEditorProvider(
            new Supplier<Clusterable>() {
                @Override
                public Clusterable get() {
                    return clusterable;
                }
            },
            new Supplier<ScheduledExecutorService>() {
                @Override
                public ScheduledExecutorService get() {
                    return scheduledExecutor;
                }
            }, 
            new Supplier<NodeStore>() {
                @Override
                public NodeStore get() {
                    return store;
                }
            },
            new Supplier<Whiteboard>() {
                @Override
                public Whiteboard get() {
                    return whiteboard;
                }
            }));
    }
    
    /**
     * <p>
     * Enable the asynchronous (background) indexing behavior for the provided
     * task name.
     * </p>
     * <p>
     * Please note that when enabling the background indexer, you need to take
     * care of calling
     * <code>#shutdown</code> on the <code>executor</code> provided for this Oak instance.
     * </p>
     */
    public Oak withAsyncIndexing(@Nonnull String name, long delayInSeconds) {
        if (this.asyncTasks == null) {
            asyncTasks = new HashMap<String, Long>();
        }
        checkState(delayInSeconds > 0, "delayInSeconds value must be > 0");
        asyncTasks.put(AsyncIndexUpdate.checkValidName(name), delayInSeconds);
        return this;
    }

    @Nonnull
    public Whiteboard getWhiteboard() {
        return this.whiteboard;
    }

    /**
     * Returns the content repository instance created with the given
     * configuration. If the repository doesn't exist yet, a new instance will
     * be created and returned for each subsequent call of this method.
     *
     * @return content repository
     */
    public ContentRepository createContentRepository() {
        if (contentRepository == null) {
            contentRepository = createNewContentRepository();
        }

        return contentRepository;
    }

    private ContentRepository createNewContentRepository() {
        final RepoStateCheckHook repoStateCheckHook = new RepoStateCheckHook();
        final List<Registration> regs = Lists.newArrayList();
        regs.add(whiteboard.register(Executor.class, getExecutor(), Collections.emptyMap()));

        IndexEditorProvider indexEditors = CompositeIndexEditorProvider.compose(indexEditorProviders);
        OakInitializer.initialize(store, new CompositeInitializer(initializers), indexEditors);

        QueryIndexProvider indexProvider = CompositeQueryIndexProvider.compose(queryIndexProviders);

        commitHooks.add(repoStateCheckHook);
        List<CommitHook> initHooks = new ArrayList<CommitHook>(commitHooks);
        initHooks.add(new EditorHook(CompositeEditorProvider
                .compose(editorProviders)));

        if (asyncTasks != null) {
            IndexMBeanRegistration indexRegistration = new IndexMBeanRegistration(
                    whiteboard);
            regs.add(indexRegistration);
            for (Entry<String, Long> t : asyncTasks.entrySet()) {
                AsyncIndexUpdate task = new AsyncIndexUpdate(t.getKey(), store,
                        indexEditors);
                indexRegistration.registerAsyncIndexer(task, t.getValue());
                closer.register(task);
            }

            PropertyIndexAsyncReindex asyncPI = new PropertyIndexAsyncReindex(
                    new AsyncIndexUpdate(IndexConstants.ASYNC_REINDEX_VALUE,
                            store, indexEditors, true), getExecutor());
            regs.add(registerMBean(whiteboard,
                    PropertyIndexAsyncReindexMBean.class, asyncPI,
                    PropertyIndexAsyncReindexMBean.TYPE, "async"));
        }

        if (NodeCounter.USE_OLD_COUNTER) {
            regs.add(registerMBean(whiteboard, NodeCounterMBean.class,
                    new NodeCounterOld(store), NodeCounterMBean.TYPE, "nodeCounter"));
        } else {
            regs.add(registerMBean(whiteboard, NodeCounterMBean.class,
                    new NodeCounter(store), NodeCounterMBean.TYPE, "nodeCounter"));
        }

        regs.add(registerMBean(whiteboard, QueryEngineSettingsMBean.class,
                queryEngineSettings, QueryEngineSettingsMBean.TYPE, "settings"));

        regs.add(registerMBean(whiteboard, QueryStatsMBean.class,
                queryEngineSettings.getQueryStats(), QueryStatsMBean.TYPE, "Oak Query Statistics (Extended)"));

        // FIXME: OAK-810 move to proper workspace initialization
        // initialize default workspace
        Iterable<WorkspaceInitializer> workspaceInitializers =
                Iterables.transform(securityProvider.getConfigurations(),
                        new Function<SecurityConfiguration, WorkspaceInitializer>() {
                            @Override
                            public WorkspaceInitializer apply(SecurityConfiguration sc) {
                                WorkspaceInitializer wi = sc.getWorkspaceInitializer();
                                if (wi instanceof QueryIndexProviderAware){
                                    ((QueryIndexProviderAware) wi).setQueryIndexProvider(indexProvider);
                                }
                                return wi;
                            }
                        });
        OakInitializer.initialize(
                workspaceInitializers, store, defaultWorkspaceName, indexEditors);

        // add index hooks later to prevent the OakInitializer to do excessive indexing
        with(new IndexUpdateProvider(indexEditors, failOnMissingIndexProvider));
        withEditorHook();

        // Register observer last to prevent sending events while initialising
        for (Observer observer : observers) {
            regs.add(whiteboard.register(Observer.class, observer, emptyMap()));
        }

        RepositoryManager repositoryManager = new RepositoryManager(whiteboard);
        regs.add(registerMBean(whiteboard, RepositoryManagementMBean.class, repositoryManager,
                RepositoryManagementMBean.TYPE, repositoryManager.getName()));

        CommitHook composite = CompositeHook.compose(commitHooks);
        regs.add(whiteboard.register(CommitHook.class, composite, Collections.emptyMap()));
        
        final Tracker<Descriptors> t = whiteboard.track(Descriptors.class);

        return new ContentRepositoryImpl(
                store,
                composite,
                defaultWorkspaceName,
                queryEngineSettings.unwrap(),
                indexProvider,
                securityProvider,
                new AggregatingDescriptors(t)) {
            @Override
            public void close() throws IOException {
                super.close();
                repoStateCheckHook.close();
                new CompositeRegistration(regs).unregister();
                closer.close();
            }
        };
    }

    /**
     * Creates a content repository with the given configuration
     * and logs in to the default workspace with no credentials,
     * returning the resulting content session.
     * <p>
     * This method exists mostly as a convenience for one-off tests,
     * as there's no way to create other sessions for accessing the
     * same repository.
     * <p>
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
     * <p>
     * This method exists mostly as a convenience for one-off tests, as
     * the returned root is the only way to access the session or the
     * repository.
     * <p>
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

    /**
     * CommitHook to ensure that commit only go through till repository is not
     * closed. Once repository is closed the commits would be failed
     */
    private static class RepoStateCheckHook implements CommitHook, Closeable {
        private volatile boolean closed;

        @Nonnull
        @Override
        public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) throws CommitFailedException {
            if (closed){
                throw new CommitFailedException(
                        CommitFailedException.OAK, 2, "ContentRepository closed");
            }
            return after;
        }

        @Override
        public void close() throws IOException {
            this.closed = true;
        }
    }

    /**
     * Settings of the query engine. This instance is an AnnotatedStandardMBean.
     */
    private static final class AnnotatedQueryEngineSettings extends AnnotatedStandardMBean implements QueryEngineSettingsMBean {

        private final QueryEngineSettings settings;

        /**
         * Create a new query engine settings object. Creating the object is
         * relatively slow, and at runtime, as few such objects as possible should
         * be created (ideally, only one per Oak instance). Creating new instances
         * also means they can not be configured using JMX, as one would expect.
         */
        private AnnotatedQueryEngineSettings(QueryEngineSettings settings) {
            super(QueryEngineSettingsMBean.class);
            this.settings = settings;
        }

        /**
         * Create a new query engine settings object. Creating the object is
         * relatively slow, and at runtime, as few such objects as possible should
         * be created (ideally, only one per Oak instance). Creating new instances
         * also means they can not be configured using JMX, as one would expect.
         */
        private AnnotatedQueryEngineSettings() {
            this(new QueryEngineSettings());
        }

        @Override
        public long getLimitInMemory() {
            return settings.getLimitInMemory();
        }

        @Override
        public void setLimitInMemory(long limitInMemory) {
            settings.setLimitInMemory(limitInMemory);
        }

        @Override
        public long getLimitReads() {
            return settings.getLimitReads();
        }

        @Override
        public void setLimitReads(long limitReads) {
            settings.setLimitReads(limitReads);
        }

        @Override
        public boolean getFailTraversal() {
            return settings.getFailTraversal();
        }

        @Override
        public void setFailTraversal(boolean failQueriesWithoutIndex) {
            settings.setFailTraversal(failQueriesWithoutIndex);
        }

        @Override
        public boolean isFastQuerySize() {
            return settings.isFastQuerySize();
        }

        @Override
        public void setFastQuerySize(boolean fastQuerySize) {
            settings.setFastQuerySize(fastQuerySize);
        }

        public QueryStatsMBean getQueryStats() {
            return settings.getQueryStats();
        }

        public QueryEngineSettings unwrap() {
            return settings;
        }

        @Override
        public String toString() {
            return settings.toString();
        }
    }

    public static class OakDefaultComponents {

        public static final OakDefaultComponents INSTANCE = new OakDefaultComponents();

        private final Iterable<CommitHook> commitHooks = ImmutableList.of(new VersionHook());

        private  final Iterable<RepositoryInitializer> repositoryInitializers = ImmutableList
                .of(new InitialContent());

        private  final Iterable<EditorProvider> editorProviders = ImmutableList.of(
                new ItemSaveValidatorProvider(), new NameValidatorProvider(), new NamespaceEditorProvider(),
                new TypeEditorProvider(), new ConflictValidatorProvider(), new ChangeCollectorProvider());

        private  final Iterable<IndexEditorProvider> indexEditorProviders = ImmutableList.of(
                new ReferenceEditorProvider(), new PropertyIndexEditorProvider(), new NodeCounterEditorProvider(),
                new OrderedPropertyIndexEditorProvider());

        private  final Iterable<QueryIndexProvider> queryIndexProviders = ImmutableList
                .of(new ReferenceIndexProvider(), new PropertyIndexProvider(), new NodeTypeIndexProvider());

        private  final SecurityProvider securityProvider = new SecurityProviderImpl();

        private OakDefaultComponents() {
        }

        public Iterable<CommitHook> commitHooks() {
            return commitHooks;
        }

        public Iterable<RepositoryInitializer> repositoryInitializers() {
            return repositoryInitializers;
        }

        public Iterable<EditorProvider> editorProviders() {
            return editorProviders;
        }

        public Iterable<IndexEditorProvider> indexEditorProviders() {
            return indexEditorProviders;
        }

        public Iterable<QueryIndexProvider> queryIndexProviders() {
            return queryIndexProviders;
        }

        public SecurityProvider securityProvider() {
            return securityProvider;
        }
    }

}
