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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.emptyMap;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.OBSERVATION_EVENT_COUNTER;
import static org.apache.jackrabbit.api.stats.RepositoryStatistics.Type.OBSERVATION_EVENT_DURATION;
import static org.apache.jackrabbit.oak.plugins.observation.filter.VisibleFilter.VISIBLE_FILTER;
import static org.apache.jackrabbit.oak.spi.observation.ChangeSet.COMMIT_CONTEXT_OBSERVATION_CHANGESET;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.scheduleWithFixedDelay;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.jcr.observation.Event;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;

import org.apache.jackrabbit.api.jmx.EventListenerMBean;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.plugins.observation.Filter;
import org.apache.jackrabbit.oak.plugins.observation.FilteringAwareObserver;
import org.apache.jackrabbit.oak.plugins.observation.FilteringDispatcher;
import org.apache.jackrabbit.oak.plugins.observation.FilteringObserver;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterConfigMBean;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterProvider;
import org.apache.jackrabbit.oak.plugins.observation.filter.Filters;
import org.apache.jackrabbit.oak.plugins.observation.filter.ChangeSetFilter;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.BackgroundObserverMBean;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.observation.ChangeSet;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticManager;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.stats.TimeSeriesMax;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.Monitor.Guard;

/**
 * A {@code ChangeProcessor} generates observation {@link javax.jcr.observation.Event}s
 * based on a {@link FilterProvider filter} and delivers them to an {@link EventListener}.
 * <p>
 * After instantiation a {@code ChangeProcessor} must be started in order to start
 * delivering observation events and stopped to stop doing so.
 */
class ChangeProcessor implements FilteringAwareObserver {
    private static final Logger LOG = LoggerFactory.getLogger(ChangeProcessor.class);
    private static final PerfLogger PERF_LOGGER = new PerfLogger(
            LoggerFactory.getLogger(ChangeProcessor.class.getName() + ".perf"));

    private enum FilterResult {
        /** marks a commit as to be included, ie delivered.
         * It's okay to falsely mark a commit as included,
         * since filtering (as part of converting to events)
         * will be applied at a later stage again. */
        INCLUDE,
        /** mark a commit as not of interest to this ChangeProcessor.
         * Exclusion is definite, ie it's not okay to falsely
         * mark a commit as excluded */
        EXCLUDE, 
        /** mark a commit as included but indicate that this
         * is not a result of prefiltering but that prefiltering
         * was skipped/not applicable for some reason */
        PREFILTERING_SKIPPED
    }
    
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

    //It'd would have been more useful to have following 2 properties as instance variables
    //which got set by tests. But, the tests won't get a handle to the actual instance, so
    //static-members it is.
    /**
     * Number of milliseconds to wait before issuing consecutive queue full warn messages
     * Controlled by command line property "oak.observation.full-queue.warn.interval".
     * Note, the command line parameter is wait interval in minutes.
     */
    static long QUEUE_FULL_WARN_INTERVAL = TimeUnit.MINUTES.toMillis(Integer
            .getInteger("oak.observation.full-queue.warn.interval", 30));
    static Clock clock = Clock.SIMPLE;

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
    private final TimeSeriesMax maxQueueLengthRecorder;
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

    /**
     * for statistics: tracks how many times prefiltering excluded a commit
     */
    private int prefilterExcludeCount;
    
    /**
     * for statistics: tracks how many times prefiltering included a commit
     */
    private int prefilterIncludeCount;
    
    /**
     * for statistics: tracks how many times prefiltering was ignored (not evaluated at all),
     * either because it was disabled, queue too small, CommitInfo null or CommitContext null
     */
    private int prefilterSkipCount;
    
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
        this.maxQueueLengthRecorder = statisticManager.maxQueLengthRecorder();
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
    
    /** for testing only - hence package protected **/
    FilterProvider getFilterProvider() {
        return filterProvider.get();
    }

    @Nonnull
    public ChangeProcessorMBean getMBean() {
        return new ChangeProcessorMBean() {

            @Override
            public int getPrefilterExcludeCount() {
                return prefilterExcludeCount;
            }

            @Override
            public int getPrefilterIncludeCount() {
                return prefilterIncludeCount;
            }

            @Override
            public int getPrefilterSkipCount() {
                return prefilterSkipCount;
            }

        };
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
        final FilteringObserver filteringObserver = createObserver(executor);
        listenerId = COUNTER.incrementAndGet() + "";
        Map<String, String> attrs = ImmutableMap.of(LISTENER_ID, listenerId);
        String name = tracker.toString();
        registration = new CompositeRegistration(
            whiteboard.register(Observer.class, filteringObserver, emptyMap()),
            registerMBean(whiteboard, EventListenerMBean.class,
                    tracker.getListenerMBean(), "EventListener", name, attrs),
            registerMBean(whiteboard, BackgroundObserverMBean.class,
                    filteringObserver.getBackgroundObserver().getMBean(), BackgroundObserverMBean.TYPE, name, attrs),
            registerMBean(whiteboard, ChangeProcessorMBean.class,
                    getMBean(), ChangeProcessorMBean.TYPE, name, attrs),
            //TODO If FilterProvider gets changed later then MBean would need to be
            // re-registered
            registerMBean(whiteboard, FilterConfigMBean.class,
                    filterProvider.get().getConfigMBean(), FilterConfigMBean.TYPE, name, attrs),
            new Registration() {
                @Override
                public void unregister() {
                    filteringObserver.close();
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

    private FilteringObserver createObserver(final WhiteboardExecutor executor) {
        FilteringDispatcher fd = new FilteringDispatcher(this);
        BackgroundObserver bo = new BackgroundObserver(fd, executor, queueLength) {
            private volatile long delay;
            private volatile boolean blocking;

            private long lastQueueFullWarnTimestamp = -1;

            @Override
            protected void added(int newQueueSize) {
                queueSizeChanged(newQueueSize);
            }
            
            @Override
            protected void removed(int newQueueSize, long created) {
                queueSizeChanged(newQueueSize);
            }
            
            private void queueSizeChanged(int newQueueSize) {
                maxQueueLengthRecorder.recordValue(newQueueSize);
                tracker.recordQueueLength(newQueueSize);
                if (newQueueSize >= queueLength) {
                    if (commitRateLimiter != null) {
                        if (!blocking) {
                            logQueueFullWarning("Revision queue is full. Further commits will be blocked.");
                        }
                        commitRateLimiter.blockCommits();
                    } else if (!blocking) {
                        logQueueFullWarning("Revision queue is full. Further revisions will be compacted.");
                    }
                    blocking = true;
                } else {
                    double fillRatio = (double) newQueueSize / queueLength;
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
                        } else {
                            blocking = false;
                        }
                    }
                }
            }

            private void logQueueFullWarning(String message) {
                long currTime = clock.getTime();
                if (lastQueueFullWarnTimestamp + QUEUE_FULL_WARN_INTERVAL < currTime) {
                    LOG.warn("{} Suppressing further such cases for {} minutes.",
                            message,
                            TimeUnit.MILLISECONDS.toMinutes(QUEUE_FULL_WARN_INTERVAL));
                    lastQueueFullWarnTimestamp = currTime;
                } else {
                    LOG.debug(message);
                }
            }
            
            @Override
            public String toString() {
                return "Prefiltering BackgroundObserver for "+ChangeProcessor.this;
            }
        };
        return new FilteringObserver(bo, new Filter() {
            
            @Override
            public boolean excludes(NodeState root, CommitInfo info) {
                final FilterResult filterResult = evalPrefilter(root, info, getChangeSet(info));
                switch (filterResult) {
                case PREFILTERING_SKIPPED: {
                    prefilterSkipCount++;
                    return false;
                }
                case EXCLUDE: {
                    prefilterExcludeCount++;
                    return true;
                }
                case INCLUDE: {
                    prefilterIncludeCount++;
                    return false;
                }
                default: {
                    LOG.info("isExcluded: unknown/unsupported filter result: " + filterResult);
                    prefilterSkipCount++;
                    return false;
                }
                }
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

    /**
     * Utility method that extracts the ChangeSet from a CommitInfo if possible.
     * @param info
     * @return
     */
    public static ChangeSet getChangeSet(CommitInfo info) {
        if (info == null) {
            return null;
        }
        CommitContext context = (CommitContext) info.getInfo().get(CommitContext.NAME);
        if (context == null) {
            return null;
        }
        return (ChangeSet) context.get(COMMIT_CONTEXT_OBSERVATION_CHANGESET);
    }

    @Override
    public void contentChanged(@Nonnull NodeState before, 
                               @Nonnull NodeState after,
                               @Nonnull CommitInfo info) {
        checkNotNull(before); // OAK-5160 before is now guaranteed to be non-null
        checkNotNull(after);
        checkNotNull(info);
        try {
            long start = PERF_LOGGER.start();
            FilterProvider provider = filterProvider.get();
            // FIXME don't rely on toString for session id
            if (provider.includeCommit(contentSession.toString(), info)) {
                EventFilter filter = provider.getFilter(before, after);
                EventIterator events = new EventQueue(namePathMapper, info, before, after,
                        provider.getSubTrees(), Filters.all(filter, VISIBLE_FILTER), 
                        provider.getEventAggregator());

                long time = System.nanoTime();
                boolean hasEvents = events.hasNext();
                tracker.recordProducerTime(System.nanoTime() - time, TimeUnit.NANOSECONDS);
                if (hasEvents && runningMonitor.enterIf(running)) {
                    if (commitRateLimiter != null) {
                        commitRateLimiter.beforeNonBlocking();
                    }
                    try {
                        CountingIterator countingEvents = new CountingIterator(events);
                        eventListener.onEvent(countingEvents);
                        countingEvents.updateCounters(eventCount, eventDuration);
                    } finally {
                        if (commitRateLimiter != null) {
                            commitRateLimiter.afterNonBlocking();
                        }
                        runningMonitor.leave();
                    }
                }
            }
            PERF_LOGGER.end(start, 100,
                    "Generated events (before: {}, after: {})",
                    before, after);
        } catch (Exception e) {
            LOG.warn("Error while dispatching observation events for " + tracker, e);
        }
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
    
    /** for logging only **/
    String getListenerToString() {
        if (tracker == null) {
            return "null";
        }
        EventListenerMBean listenerMBean = tracker.getListenerMBean();
        if (listenerMBean == null) {
            return "null (no listener mbean)";
        }
        return listenerMBean.getToString();
    }

    /**
     * Evaluate the prefilter for a given commit.
     * @param changeSet 
     * 
     * @return a FilterResult indicating either inclusion, exclusion or
     *         inclusion-due-to-skipping. The latter is used to reflect
     *         prefilter evaluation better in statistics (as it could also have
     *         been reported just as include)
     */
    private FilterResult evalPrefilter(NodeState root, CommitInfo info, ChangeSet changeSet) {
        if (info == null) {
            return FilterResult.PREFILTERING_SKIPPED;
        }
        if (root == null) {
            // likely only occurs at startup
            // we can't do any diffing etc, so just not exclude it
            return FilterResult.PREFILTERING_SKIPPED;
        }

        final FilterProvider fp = filterProvider.get();
        // FIXME don't rely on toString for session id
        if (!fp.includeCommit(contentSession.toString(), info)) {
            // 'classic' (and cheap pre-) filtering
            return FilterResult.EXCLUDE;
        }
        if (changeSet == null) {
            // then can't do any prefiltering since it was not
            // able to complete the sets (within the given boundaries)
            // (this corresponds to a large commit, which thus can't
            // go through prefiltering)
            return FilterResult.PREFILTERING_SKIPPED;
        }

        final ChangeSetFilter prefilter = fp;
        if (prefilter.excludes(changeSet)) {
            return FilterResult.EXCLUDE;
        } else {
            return FilterResult.INCLUDE;
        }
    }
}
