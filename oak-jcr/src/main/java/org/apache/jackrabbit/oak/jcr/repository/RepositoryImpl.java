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
package org.apache.jackrabbit.oak.jcr.repository;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.security.auth.login.LoginException;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.commons.SimpleValueFactory;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.jmx.SessionMBean;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.session.RefreshStrategy;
import org.apache.jackrabbit.oak.jcr.session.RefreshStrategy.Composite;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.jcr.session.SessionStats;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.spi.gc.DelegatingGCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticManager;
import org.apache.jackrabbit.oak.spi.descriptors.GenericDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO document
 */
public class RepositoryImpl implements JackrabbitRepository {

    /**
     * Name of the session attribute value determining the session refresh
     * interval in seconds.
     *
     * @see org.apache.jackrabbit.oak.jcr.session.RefreshStrategy
     */
    public static final String REFRESH_INTERVAL = "oak.refresh-interval";

    /**
     * Name of the session attribute for enabling relaxed locking rules
     *
     * @see <a href="https://issues.apache.org/jira/browse/OAK-1329">OAK-1329</a>
     */
    public static final String RELAXED_LOCKING = "oak.relaxed-locking";

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(RepositoryImpl.class);

    protected final Whiteboard whiteboard;
    protected final boolean fastQueryResultSize;
    private final GenericDescriptors descriptors;
    private final ContentRepository contentRepository;
    private final SecurityProvider securityProvider;
    private final int observationQueueLength;
    private final CommitRateLimiter commitRateLimiter;
    private final Clock.Fast clock;
    private final DelegatingGCMonitor gcMonitor = new DelegatingGCMonitor();
    private final Registration gcMonitorRegistration;
    private final MountInfoProvider mountInfoProvider;

    /**
     * {@link ThreadLocal} counter that keeps track of the save operations
     * performed per thread so far. This is is then used to determine if
     * the current session needs to be refreshed to see the changes done by
     * another session in the same thread.
     * <p>
     * <b>Note</b> - This thread local is never cleared. However, we only
     * store a {@link Long} instance and do not derive from
     * {@link ThreadLocal} so that (class loader) leaks typically associated
     * with thread locals do not occur.
     */
    private final ThreadLocal<Long> threadSaveCount = new ThreadLocal<Long>();

    private final ScheduledExecutorService scheduledExecutor =
            createListeningScheduledExecutorService();

    private final StatisticManager statisticManager;

    /**
     * Constructor used for backward compatibility.
     */
    public RepositoryImpl(@Nonnull ContentRepository contentRepository,
                          @Nonnull Whiteboard whiteboard,
                          @Nonnull SecurityProvider securityProvider,
                          int observationQueueLength,
                          CommitRateLimiter commitRateLimiter) {
        this(contentRepository, whiteboard, securityProvider, 
                observationQueueLength, commitRateLimiter, false);
    }
    
    public RepositoryImpl(@Nonnull ContentRepository contentRepository,
                          @Nonnull Whiteboard whiteboard,
                          @Nonnull SecurityProvider securityProvider,
                          int observationQueueLength,
                          CommitRateLimiter commitRateLimiter,
                          boolean fastQueryResultSize) {
        this.contentRepository = checkNotNull(contentRepository);
        this.whiteboard = checkNotNull(whiteboard);
        this.securityProvider = checkNotNull(securityProvider);
        this.observationQueueLength = observationQueueLength;
        this.commitRateLimiter = commitRateLimiter;
        this.descriptors = determineDescriptors();
        this.statisticManager = new StatisticManager(whiteboard, scheduledExecutor);
        this.clock = new Clock.Fast(scheduledExecutor);
        this.gcMonitorRegistration = whiteboard.register(GCMonitor.class, gcMonitor, emptyMap());
        this.fastQueryResultSize = fastQueryResultSize;
        this.mountInfoProvider = WhiteboardUtils.getService(whiteboard, MountInfoProvider.class);
    }

    //---------------------------------------------------------< Repository >---
    /**
     * @see javax.jcr.Repository#getDescriptorKeys()
     */
    @Override
    public String[] getDescriptorKeys() {
        return descriptors.getKeys();
    }

    /**
     * @see Repository#isStandardDescriptor(String)
     */
    @Override
    public boolean isStandardDescriptor(String key) {
        return descriptors.isStandardDescriptor(key);
    }

    /**
     * @see javax.jcr.Repository#getDescriptor(String)
     */
    @Override
    public String getDescriptor(String key) {
        try {
            Value v = getDescriptorValue(key);
            return v == null
                    ? null
                    : v.getString();
        } catch (RepositoryException e) {
            log.debug("Error converting value for descriptor with key {} to string", key);
            return null;
        }
    }

    /**
     * @see javax.jcr.Repository#getDescriptorValue(String)
     */
    @Override
    public Value getDescriptorValue(String key) {
        return descriptors.getValue(key);
    }

    /**
     * @see javax.jcr.Repository#getDescriptorValues(String)
     */
    @Override
    public Value[] getDescriptorValues(String key) {
        return descriptors.getValues(key);
    }

    /**
     * @see javax.jcr.Repository#isSingleValueDescriptor(String)
     */
    @Override
    public boolean isSingleValueDescriptor(String key) {
        return descriptors.isSingleValueDescriptor(key);
    }

    /**
     * @see javax.jcr.Repository#login(javax.jcr.Credentials, String)
     */
    @Override
    public Session login(@Nullable Credentials credentials, @Nullable String workspaceName)
            throws RepositoryException {
        return login(credentials, workspaceName, null);
    }

    /**
     * Calls {@link Repository#login(Credentials, String)} with
     * {@code null} arguments.
     *
     * @return logged in session
     * @throws RepositoryException if an error occurs
     */
    @Override
    public Session login() throws RepositoryException {
        return login(null, null, null);
    }

    /**
     * Calls {@link Repository#login(Credentials, String)} with
     * the given credentials and a {@code null} workspace name.
     *
     * @param credentials login credentials
     * @return logged in session
     * @throws RepositoryException if an error occurs
     */
    @Override
    public Session login(Credentials credentials) throws RepositoryException {
        return login(credentials, null, null);
    }

    /**
     * Calls {@link Repository#login(Credentials, String)} with
     * {@code null} credentials and the given workspace name.
     *
     * @param workspace workspace name
     * @return logged in session
     * @throws RepositoryException if an error occurs
     */
    @Override
    public Session login(String workspace) throws RepositoryException {
        return login(null, workspace, null);
    }

    //------------------------------------------------------------< JackrabbitRepository >---

    @Override
    public Session login(@CheckForNull Credentials credentials, @CheckForNull String workspaceName,
            @CheckForNull Map<String, Object> attributes) throws RepositoryException {
        try {
            if (attributes == null) {
                attributes = emptyMap();
            }
            Long refreshInterval = getRefreshInterval(credentials);
            if (refreshInterval == null) {
                refreshInterval = getRefreshInterval(attributes);
            } else if (attributes.containsKey(REFRESH_INTERVAL)) {
                throw new RepositoryException("Duplicate attribute '" + REFRESH_INTERVAL + "'.");
            }
            boolean relaxedLocking = getRelaxedLocking(attributes);

            RefreshPredicate predicate = new RefreshPredicate();
            RefreshStrategy refreshStrategy = refreshInterval == null
                ? new RefreshStrategy.ConditionalRefreshStrategy(new RefreshStrategy.LogOnce(60), predicate)
                : new RefreshStrategy.Timed(refreshInterval);
            ContentSession contentSession = contentRepository.login(credentials, workspaceName);
            SessionDelegate sessionDelegate = createSessionDelegate(refreshStrategy, contentSession);
            SessionContext context = createSessionContext(
                    statisticManager, securityProvider,
                    createAttributes(refreshInterval, relaxedLocking),
                    sessionDelegate, observationQueueLength, commitRateLimiter);
            predicate.setSessionContext(context);
            return context.getSession();
        } catch (LoginException e) {
            throw new javax.jcr.LoginException(e.getMessage(), e);
        }
    }

    private SessionDelegate createSessionDelegate(
            RefreshStrategy refreshStrategy,
            ContentSession contentSession) {

        final RefreshOnGC refreshOnGC = new RefreshOnGC(gcMonitor);
        refreshStrategy = Composite.create(refreshStrategy, refreshOnGC);

        return new SessionDelegate(
                contentSession, securityProvider, refreshStrategy,
                threadSaveCount, statisticManager, clock) {
            // Defer session MBean registration to avoid cluttering the
            // JMX name space with short lived sessions
            RegistrationTask registrationTask = new RegistrationTask(getSessionStats(), whiteboard);
            ScheduledFuture<?> scheduledTask = scheduledExecutor.schedule(registrationTask, 1, TimeUnit.MINUTES);

            @Override
            public void logout() {
                refreshOnGC.close();
                // Cancel session MBean registration
                registrationTask.cancel();
                scheduledTask.cancel(false);
                super.logout();
            }
        };
    }

    @Override
    public void shutdown() {
        statisticManager.dispose();
        gcMonitorRegistration.unregister();
        clock.close();
        new ExecutorCloser(scheduledExecutor).close();
        if (contentRepository instanceof Closeable) {
            IOUtils.closeQuietly((Closeable) contentRepository);
        }
    }

    //------------------------------------------------------------< internal >---

    /**
     * Factory method for creating a {@link SessionContext} instance for
     * a new session. Called by {@link #login()}. Can be overridden by
     * subclasses to customize the session implementation.
     *
     * @return session context
     */
    protected SessionContext createSessionContext(
            StatisticManager statisticManager, SecurityProvider securityProvider,
            Map<String, Object> attributes, SessionDelegate delegate, int observationQueueLength,
            CommitRateLimiter commitRateLimiter) {
        return new SessionContext(this, statisticManager, securityProvider, whiteboard, attributes,
                delegate, observationQueueLength, commitRateLimiter, mountInfoProvider, fastQueryResultSize);
    }

    /**
     * Provides descriptors for current repository implementations. Can be overridden
     * by the subclasses to add more values to the descriptor
     * @return  repository descriptor
     */
    protected GenericDescriptors determineDescriptors() {
        return new JcrDescriptorsImpl(contentRepository.getDescriptors(), new SimpleValueFactory());
    }

    /**
     * Returns the descriptors associated with the repository
     * @return repository descriptor
     */
    protected GenericDescriptors getDescriptors() {
        return descriptors;
    }

    //------------------------------------------------------------< private >---

    private static ScheduledExecutorService createListeningScheduledExecutorService() {
        ThreadFactory tf = new ThreadFactory() {
            private final AtomicLong counter = new AtomicLong();
            @Override
            public Thread newThread(@Nonnull Runnable r) {
                Thread t = new Thread(r, newName());
                t.setDaemon(true);
                return t;
            }

            private String newName() {
                return "oak-repository-executor-" + counter.incrementAndGet();
            }
        };
        return new ScheduledThreadPoolExecutor(1, tf) {
            // purge the list of schedule tasks before scheduling a new task in order
            // to reduce memory consumption in the face of many cancelled tasks. See OAK-1890.

            @Override
            public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
                purge();
                return super.schedule(callable, delay, unit);
            }

            @Override
            public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
                purge();
                return super.schedule(command, delay, unit);
            }

            @Override
            public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay,
                    long period, TimeUnit unit) {
                purge();
                return super.scheduleAtFixedRate(command, initialDelay, period, unit);
            }

            @Override
            public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
                    long delay, TimeUnit unit) {
                purge();
                return super.scheduleWithFixedDelay(command, initialDelay, delay, unit);
            }
        };
    }

    private static Long getRefreshInterval(Credentials credentials) {
        if (credentials instanceof SimpleCredentials) {
            Object value = ((SimpleCredentials) credentials).getAttribute(REFRESH_INTERVAL);
            return toLong(value);
        } else if (credentials instanceof TokenCredentials) {
            String value = ((TokenCredentials) credentials).getAttribute(REFRESH_INTERVAL);
            if (value != null) {
                return toLong(value);
            }
        }
        return null;
    }

    private static Long getRefreshInterval(Map<String, Object> attributes) {
        return toLong(attributes.get(REFRESH_INTERVAL));
    }

    private static boolean getRelaxedLocking(Map<String, Object> attributes) {
        Object value = attributes.get(RELAXED_LOCKING);
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        } else {
            return false;
        }
    }

    private static Long toLong(Object value) {
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof String) {
            return toLong((String) value);
        } else {
            return null;
        }
    }

    private static Long toLong(String longValue) {
        try {
            return Long.valueOf(longValue);
        } catch (NumberFormatException e) {
            log.warn("Invalid value '" + longValue + "' for " + REFRESH_INTERVAL +
                    ". Expected long. ", e);
            return null;
        }
    }

    private static Map<String, Object> createAttributes(
            Long refreshInterval, boolean relaxedLocking) {
        if (refreshInterval == null && !relaxedLocking) {
            return emptyMap();
        } else if (refreshInterval == null) {
            return singletonMap(RELAXED_LOCKING, (Object) Boolean.valueOf(relaxedLocking));
        } else if (!relaxedLocking) {
            return singletonMap(REFRESH_INTERVAL, (Object) refreshInterval);
        } else {
            return ImmutableMap.of(
                    REFRESH_INTERVAL, (Object) refreshInterval,
                    RELAXED_LOCKING,  (Object) Boolean.valueOf(relaxedLocking));
        }
    }

    private static class RefreshOnGC extends GCMonitor.Empty implements RefreshStrategy {
        private final Registration registration;
        private volatile boolean compacted;

        public RefreshOnGC(DelegatingGCMonitor gcMonitor) {
            registration = gcMonitor.registerGCMonitor(this);
        }

        public void close() {
            registration.unregister();
        }

        @Override
        public void compacted() {
            compacted = true;
        }

        @Override
        public boolean needsRefresh(long secondsSinceLastAccess) {
            return compacted;
        }

        @Override
        public void refreshed() {
            compacted = false;
        }

        @Override
        public String toString() {
            return "Refresh on revision garbage collection";
        }
    }

    /**
     * Predicate which ensures that refresh strategy is invoked only
     * if there is no event listeners registered with the session
     */
    private static class RefreshPredicate implements Predicate<Long>{
        private SessionContext sessionContext;

        @Override
        public boolean apply(@Nullable Long input) {
            if (sessionContext == null){
                return true;
            }
            return !sessionContext.hasEventListeners();
        }

        public void setSessionContext(SessionContext sessionContext) {
            this.sessionContext = sessionContext;
        }
    }

    private static class RegistrationTask implements Runnable {
        private final SessionStats sessionStats;
        private final Whiteboard whiteboard;
        private boolean cancelled;
        private Registration completed;

        public RegistrationTask(SessionStats sessionStats, Whiteboard whiteboard) {
            this.sessionStats = sessionStats;
            this.whiteboard = whiteboard;
        }

        @Override
        public synchronized void run() {
            if (!cancelled) {
                completed = registerMBean(whiteboard, SessionMBean.class, sessionStats,
                        SessionMBean.TYPE, sessionStats.toString());
            }
        }
        
        public synchronized void cancel() {
            cancelled = true;
            if (completed != null) {
                completed.unregister();
                completed = null;
            }
        }
    }
}
