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
import static org.apache.jackrabbit.oak.plugins.observation.filter.VisibleFilter.VISIBLE_FILTER;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerObserver;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.scheduleWithFixedDelay;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.Monitor.Guard;
import org.apache.jackrabbit.api.jmx.EventListenerMBean;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterConfigMBean;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterProvider;
import org.apache.jackrabbit.oak.plugins.observation.filter.Filters;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserverMBean;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.stats.StatisticManager;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.apache.jackrabbit.stats.TimeSeriesMax;
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
    private static final Logger LOG = LoggerFactory.getLogger(ChangeProcessor.class);
    private static final PerfLogger PERF_LOGGER = new PerfLogger(
            LoggerFactory.getLogger(ChangeProcessor.class.getName() + ".perf"));

    /**
     * Fill ratio of the revision queue at which commits should be delayed
     * (conditional of {@code commitRateLimiter} being non {@code null}).
     */
    public static final double DELAY_THRESHOLD;

    /**
     * Maximal number of milli seconds a commit is delayed once {@code DELAY_THRESHOLD}
     * kicks in.
     */
    public static final int MAX_DELAY;

    // OAK-4533: make DELAY_THRESHOLD and MAX_DELAY adjustable - using System.properties for now
    static {
        final String delayThresholdStr = System.getProperty("oak.commitRateLimiter.delayThreshold");
        final String maxDelayStr = System.getProperty("oak.commitRateLimiter.maxDelay");
        double delayThreshold = 0.8; /* default is 0.8 still */
        int maxDelay = 10000; /* default is 10000 still */
        try{
            if (delayThresholdStr != null && delayThresholdStr.length() != 0) {
                delayThreshold = Double.parseDouble(delayThresholdStr);
                LOG.info("<clinit> using oak.commitRateLimiter.delayThreshold of " + delayThreshold);
            }
        } catch(RuntimeException e) {
            LOG.warn("<clinit> could not parse oak.commitRateLimiter.delayThreshold, using default(" + delayThreshold + "): " + e, e);
        }
        try{
            if (maxDelayStr != null && maxDelayStr.length() != 0) {
                maxDelay = Integer.parseInt(maxDelayStr);
                LOG.info("<clinit> using oak.commitRateLimiter.maxDelay of " + maxDelay + "ms");
            }
        } catch(RuntimeException e) {
            LOG.warn("<clinit> could not parse oak.commitRateLimiter.maxDelay, using default(" + maxDelay + "): " + e, e);
        }
        DELAY_THRESHOLD = delayThreshold;
        MAX_DELAY = maxDelay;
    }
    
    private static final AtomicInteger COUNTER = new AtomicInteger();

    /**
     * JMX ObjectName property storing the listenerId which allows
     * to correlate various mbeans
     */
    static final String LISTENER_ID = "listenerId";

    private final ContentSession contentSession;
    private final NamePathMapper namePathMapper;
    private final ListenerTracker tracker;
    private final EventListener eventListener;
    private final AtomicReference<FilterProvider> filterProvider;
    private final MeterStats eventCount;
    private final TimerStats eventDuration;
    private final TimeSeriesMax maxQueueLength;
    private final int queueLength;
    private final CommitRateLimiter commitRateLimiter;

    /**
     * Lazy initialization via the {@link #start(Whiteboard)} method
     */
    private String listenerId;

    /**
     * Lazy initialization via the {@link #start(Whiteboard)} method
     */
    private CompositeRegistration registration;

    private volatile NodeState previousRoot;

    public ChangeProcessor(
            ContentSession contentSession,
            NamePathMapper namePathMapper,
            ListenerTracker tracker,
            FilterProvider filter,
            StatisticManager statisticManager,
            int queueLength,
            CommitRateLimiter commitRateLimiter) {
        this.contentSession = contentSession;
        this.namePathMapper = namePathMapper;
        this.tracker = tracker;
        eventListener = tracker.getTrackedListener();
        filterProvider = new AtomicReference<FilterProvider>(filter);
        this.eventCount = statisticManager.getMeter(OBSERVATION_EVENT_COUNTER);
        this.eventDuration = statisticManager.getTimer(OBSERVATION_EVENT_DURATION);
        this.maxQueueLength = statisticManager.maxQueLengthRecorder();
        this.queueLength = queueLength;
        this.commitRateLimiter = commitRateLimiter;
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
        final BackgroundObserver observer = createObserver(executor);
        listenerId = COUNTER.incrementAndGet() + "";
        Map<String, String> attrs = ImmutableMap.of(LISTENER_ID, listenerId);
        String name = tracker.toString();
        registration = new CompositeRegistration(
            registerObserver(whiteboard, observer),
            registerMBean(whiteboard, EventListenerMBean.class,
                    tracker.getListenerMBean(), "EventListener", name, attrs),
            registerMBean(whiteboard, BackgroundObserverMBean.class,
                    observer.getMBean(), BackgroundObserverMBean.TYPE, name, attrs),
            //TODO If FilterProvider gets changed later then MBean would need to be
            // re-registered
            registerMBean(whiteboard, FilterConfigMBean.class,
                    filterProvider.get().getConfigMBean(), FilterConfigMBean.TYPE, name, attrs),
            new Registration() {
                @Override
                public void unregister() {
                    observer.close();
                }
            },
            new Registration() {
                @Override
                public void unregister() {
                    executor.stop();
                }
            },
            scheduleWithFixedDelay(whiteboard, new Runnable() {
                @Override
                public void run() {
                    tracker.recordOneSecond();
                }
            }, 1)
        );
    }

    private BackgroundObserver createObserver(final WhiteboardExecutor executor) {
        return new BackgroundObserver(this, executor, queueLength) {
            private volatile long delay;
            private volatile boolean blocking;

            @Override
            protected void added(int queueSize) {
                maxQueueLength.recordValue(queueSize);
                tracker.recordQueueLength(queueSize);

                if (queueSize == queueLength) {
                    if (commitRateLimiter != null) {
                        if (!blocking) {
                            LOG.warn("Revision queue is full. Further commits will be blocked.");
                        }
                        commitRateLimiter.blockCommits();
                    } else if (!blocking) {
                        LOG.warn("Revision queue is full. Further revisions will be compacted.");
                    }
                    blocking = true;
                } else {
                    double fillRatio = (double) queueSize / queueLength;
                    if (fillRatio > DELAY_THRESHOLD) {
                        if (commitRateLimiter != null) {
                            if (delay == 0) {
                                LOG.warn("Revision queue is becoming full. Further commits will be delayed.");
                            }

                            // Linear backoff proportional to the number of items exceeding
                            // DELAY_THRESHOLD. Offset by 1 to trigger the log message in the
                            // else branch once the queue falls below DELAY_THRESHOLD again.
                            int newDelay = 1 + (int) ((fillRatio - DELAY_THRESHOLD) / (1 - DELAY_THRESHOLD) * MAX_DELAY);
                            if (newDelay > delay) {
                                delay = newDelay;
                                commitRateLimiter.setDelay(delay);
                            }
                        }
                    } else {
                        if (commitRateLimiter != null) {
                            if (delay > 0) {
                                LOG.debug("Revision queue becoming empty. Unblocking commits");
                                commitRateLimiter.setDelay(0);
                                delay = 0;
                            }
                            if (blocking) {
                                LOG.debug("Revision queue becoming empty. Stop delaying commits.");
                                commitRateLimiter.unblockCommits();
                                blocking = false;
                            }
                        }
                    }
                }
            }

            @Override
            protected void removed(int queueSize, long created) {
                maxQueueLength.recordValue(queueSize);
                tracker.recordQueueLength(queueSize, created);
            }
        };
    }

    private final Monitor runningMonitor = new Monitor();
    private final RunningGuard running = new RunningGuard(runningMonitor);

    /**
     * Try to stop this change processor if running. This method will wait
     * the specified time for a pending event listener to complete. If
     * no timeout occurred no further events will be delivered after this
     * method returns.
     * <p>
     * Does nothing if stopped already.
     *
     * @param timeOut time this method will wait for an executing event
     *                listener to complete.
     * @param unit    time unit for {@code timeOut}
     * @return {@code true} if no time out occurred and this change processor
     *         could be stopped, {@code false} otherwise.
     * @throws IllegalStateException if not yet started
     */
    public synchronized boolean stopAndWait(int timeOut, TimeUnit unit) {
        checkState(registration != null, "Change processor not started");
        if (running.stop()) {
            if (runningMonitor.enter(timeOut, unit)) {
                registration.unregister();
                runningMonitor.leave();
                return true;
            } else {
                // Timed out
                return false;
            }
        } else {
            // Stopped already
            return true;
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
        if (running.stop()) {
            registration.unregister();
            runningMonitor.leave();
        }
    }

    @Override
    public void contentChanged(@Nonnull NodeState root, @Nullable CommitInfo info) {
        if (previousRoot != null) {
            try {
                long start = PERF_LOGGER.start();
                FilterProvider provider = filterProvider.get();
                // FIXME don't rely on toString for session id
                if (provider.includeCommit(contentSession.toString(), info)) {
                    EventFilter filter = provider.getFilter(previousRoot, root);
                    EventIterator events = new EventQueue(namePathMapper, info, previousRoot, root,
                            provider.getSubTrees(), Filters.all(filter, VISIBLE_FILTER));

                    if (events.hasNext() && runningMonitor.enterIf(running)) {
                        try {
                            CountingIterator countingEvents = new CountingIterator(events);
                            eventListener.onEvent(countingEvents);
                            countingEvents.updateCounters(eventCount, eventDuration);
                        } finally {
                            runningMonitor.leave();
                        }
                    }
                }
                PERF_LOGGER.end(start, 100,
                        "Generated events (before: {}, after: {})",
                        previousRoot, root);
            } catch (Exception e) {
                LOG.warn("Error while dispatching observation events for " + tracker, e);
            }
        }
        previousRoot = root;
    }

    private static class CountingIterator implements EventIterator {
        private final long t0 = System.nanoTime();
        private final EventIterator events;
        private long eventCount;
        private long sysTime;

        public CountingIterator(EventIterator events) {
            this.events = events;
        }

        public void updateCounters(MeterStats eventCount, TimerStats eventDuration) {
            checkState(this.eventCount >= 0);
            eventCount.mark(this.eventCount);
            eventDuration.update(System.nanoTime() - t0 - sysTime, TimeUnit.NANOSECONDS);
            this.eventCount = -1;
        }

        @Override
        public Event next() {
            if (eventCount == -1) {
                LOG.warn("Access to EventIterator outside the onEvent callback detected. This will " +
                    "cause observation related values in RepositoryStatistics to become unreliable.");
                eventCount = -2;
            }

            long t0 = System.nanoTime();
            try {
                return events.nextEvent();
            } finally {
                eventCount++;
                sysTime += System.nanoTime() - t0;
            }
        }

        @Override
        public boolean hasNext() {
            long t0 = System.nanoTime();
            try {
                return events.hasNext();
            } finally {
                sysTime += System.nanoTime() - t0;
            }
        }

        @Override
        public Event nextEvent() {
            return next();
        }

        @Override
        public void skip(long skipNum) {
            long t0 = System.nanoTime();
            try {
                events.skip(skipNum);
            } finally {
                sysTime += System.nanoTime() - t0;
            }
        }

        @Override
        public long getSize() {
            return events.getSize();
        }

        @Override
        public long getPosition() {
            return events.getPosition();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
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

        /**
         * @return  {@code true} if this call set this guard to stopped,
         *          {@code false} if another call set this guard to stopped before.
         */
        public boolean stop() {
            boolean wasStopped = stopped;
            stopped = true;
            return !wasStopped;
        }
    }

    @Override
    public String toString() {
        return "ChangeProcessor ["
                + "listenerId=" + listenerId
                + ", tracker=" + tracker 
                + ", contentSession=" + contentSession
                + ", eventCount=" + eventCount 
                + ", eventDuration=" + eventDuration 
                + ", commitRateLimiter=" + commitRateLimiter
                + ", running=" + running.isSatisfied() + "]";
    }
}
