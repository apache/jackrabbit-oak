/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.jcr.observation;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.OBSERVATION_EVENT_COUNTER;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.OBSERVATION_EVENT_DURATION;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerObserver;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.observation.EventListener;

import com.google.common.collect.ForwardingIterator;
import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.Monitor.Guard;
import org.apache.jackrabbit.api.jmx.EventListenerMBean;
import org.apache.jackrabbit.commons.iterator.EventIteratorAdapter;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.observation.filter.ACFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterProvider;
import org.apache.jackrabbit.oak.plugins.observation.filter.Filters;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.stats.StatisticManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code ChangeProcessor} generates observation {@link javax.jcr.observation.Event}s
 * based on a {@link FilterProvider filter} and delivers them to an {@link EventListener}.
 * <p>
 * After instantiation a {@code ChangeProcessor} must be started in order to start
 * delivering observation events and stopped to stop doing so.
 */
class ChangeProcessor implements Observer {

    private static final Logger log = LoggerFactory.getLogger(ChangeProcessor.class);

    private final ContentSession contentSession;
    private final NamePathMapper namePathMapper;
    private final PermissionProvider permissionProvider;
    private final ListenerTracker tracker;
    private final EventListener eventListener;
    private final AtomicReference<FilterProvider> filterProvider;
    private final AtomicLong eventCount;
    private final AtomicLong eventDuration;
    private final int queueLength;

    private CompositeRegistration registration;
    private NodeState previousRoot;

    public ChangeProcessor(
            ContentSession contentSession,
            NamePathMapper namePathMapper,
            PermissionProvider permissionProvider,
            ListenerTracker tracker,
            FilterProvider filter,
            StatisticManager statisticManager,
            int queueLength) {
        this.contentSession = contentSession;
        this.namePathMapper = namePathMapper;
        this.permissionProvider = permissionProvider;
        this.tracker = tracker;
        eventListener = tracker.getTrackedListener();
        filterProvider = new AtomicReference<FilterProvider>(filter);
        this.eventCount = statisticManager.getCounter(OBSERVATION_EVENT_COUNTER);
        this.eventDuration = statisticManager.getCounter(OBSERVATION_EVENT_DURATION);
        this.queueLength = queueLength;
    }

    /**
     * Set the filter for the events this change processor will generate.
     * @param filter
     */
    public void setFilterProvider(FilterProvider filter) {
        filterProvider.set(filter);
    }

    /**
     * Start this change processor
     * @param whiteboard  the whiteboard instance to used for scheduling individual
     *                    runs of this change processor.
     * @throws IllegalStateException if started already
     */
    public synchronized void start(Whiteboard whiteboard) {
        checkState(registration == null, "Change processor started already");
        final WhiteboardExecutor executor = new WhiteboardExecutor();
        executor.start(whiteboard);
        registration = new CompositeRegistration(
            registerObserver(whiteboard, new BackgroundObserver(this, executor, queueLength)),
            registerMBean(whiteboard, EventListenerMBean.class,
                    tracker.getListenerMBean(), "EventListener", tracker.toString()),
            new Registration() {
        @Override
        public void unregister() {
            executor.stop();
        }
    });
    }

    private final Monitor runningMonitor = new Monitor();
    private final RunningGuard running = new RunningGuard(runningMonitor);

    /**
     * Try to stop this change processor if running. This method will wait
     * the specified time for a pending event listener to complete. If
     * no timeout occurred no further events will be delivered after this
     * method returns.
     *
     * @param timeOut time this method will wait for an executing event
     *                listener to complete.
     * @param unit    time unit for {@code timeOut}
     * @return {@code true} if no time out occurred and this change processor
     *         could be stopped, {@code false} otherwise.
     * @throws IllegalStateException if not yet started or stopped already
     */
    public synchronized boolean stopAndWait(int timeOut, TimeUnit unit) {
        checkState(registration != null, "Change processor not started");
        running.stop();
        if (runningMonitor.enter(timeOut, unit)) {
            registration.unregister();
            runningMonitor.leave();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Stop this change processor after all pending events have been
     * delivered. In contrast to {@link #stopAndWait(int, java.util.concurrent.TimeUnit)}
     * this method returns immediately without waiting for pending listeners to
     * complete.
     */
    public synchronized void stop() {
        checkState(registration != null, "Change processor not started");
        running.stop();
        registration.unregister();
        runningMonitor.leave();
    }

    @Override
    public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
        if (previousRoot != null) {
            try {
                FilterProvider provider = filterProvider.get();
                // FIXME don't rely on toString for session id
                if (provider.includeCommit(contentSession.toString(), info)) {
                    String basePath = provider.getPath();
                    EventFilter userFilter = provider.getFilter(previousRoot, root);
                    EventFilter acFilter = new ACFilter(previousRoot, root, permissionProvider, basePath);
                    EventQueue events = new EventQueue(
                            namePathMapper, info, previousRoot, root, basePath,
                            Filters.all(userFilter, acFilter));
                    if (events.hasNext() && runningMonitor.enterIf(running)) {
                        try {
                            eventListener.onEvent(
                                    new EventIteratorAdapter(statisticProvider(events)));
                        } finally {
                            runningMonitor.leave();
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Error while dispatching observation events", e);
            }
        }
        previousRoot = root;
    }

    private <T> Iterator<T> statisticProvider(final Iterator<T> events) {
        return new ForwardingIterator<T>() {
            @Override
            protected Iterator<T> delegate() {
                return events;
            }

            @Override
            public T next() {
                long t0 = System.nanoTime();
                try {
                    return super.next();
                } finally {
                    eventCount.incrementAndGet();
                    eventDuration.addAndGet(System.nanoTime() - t0);
                }
            }
        };
    }

    private static class RunningGuard extends Guard {
        private boolean stopped;

        public RunningGuard(Monitor monitor) {
            super(monitor);
        }

        @Override
        public boolean isSatisfied() {
            return !stopped;
        }

        public void stop() {
            checkState(!stopped, "Change processor already stopped");
            stopped = true;
        }
    }
}
