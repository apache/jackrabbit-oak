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
package org.apache.jackrabbit.oak.plugins.index;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean.STATUS_DONE;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

import java.io.Closeable;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.commit.AnnotatingConflictHandler;
import org.apache.jackrabbit.oak.plugins.commit.ConflictHook;
import org.apache.jackrabbit.oak.plugins.commit.ConflictValidatorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdate.MissingIndexProviderStrategy;
import org.apache.jackrabbit.oak.plugins.index.TrackingCorruptIndexHandler.CorruptIndexInfo;
import org.apache.jackrabbit.oak.plugins.index.progress.MetricRateEstimator;
import org.apache.jackrabbit.oak.plugins.index.progress.NodeCounterMBeanEstimator;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.ResetCommitAttributeHook;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Counting;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;
import org.apache.jackrabbit.util.ISO8601;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

public class AsyncIndexUpdate implements Runnable, Closeable {
    /**
     * Name of service property which determines the name of Async task
     */
    public static final String PROP_ASYNC_NAME = "oak.async";
    private static final Logger log = LoggerFactory
            .getLogger(AsyncIndexUpdate.class);

    /**
     * Name of the hidden node under which information about the checkpoints
     * seen and indexed by each async indexer is kept.
     */
    static final String ASYNC = ":async";

    private static final long DEFAULT_LIFETIME = TimeUnit.DAYS.toMillis(1000);

    private static final CommitFailedException INTERRUPTED = new CommitFailedException(
            "Async", 1, "Indexing stopped forcefully");

    /**
     * Timeout in milliseconds after which an async job would be considered as
     * timed out. Another node in cluster would wait for timeout before
     * taking over a running job
     */
    private static final long DEFAULT_ASYNC_TIMEOUT = TimeUnit.MINUTES.toMillis(
            Integer.getInteger("oak.async.lease.timeout", 15));

    private final String name;

    private final NodeStore store;

    private final IndexEditorProvider provider;

    /**
     * Property name which stores the timestamp upto which the repository is
     * indexed
     */
    private final String lastIndexedTo;

    private final long lifetime = DEFAULT_LIFETIME; // TODO: make configurable

    private final AsyncIndexStats indexStats;

    /** Flag to switch to synchronous updates once the index caught up to the repo */
    private final boolean switchOnSync;

    /**
     * Set of reindexed definitions updated between runs because a single diff
     * can report less definitions than there really are. Used in coordination
     * with the switchOnSync flag, so we know which def need to be updated after
     * a run with no changes.
     */
    private final Set<String> reindexedDefinitions = new HashSet<String>();

    private final MissingIndexProviderStrategy missingStrategy = new DefaultMissingIndexProviderStrategy();

    private final IndexTaskSpliter taskSplitter = new IndexTaskSpliter();

    private final Semaphore runPermit = new Semaphore(1);

    /**
     * Flag which would be set to true if the close operation is not
     * able to close within specific time. The flag would be an
     * indication to indexing thread to return straightway say by
     * throwing an exception
     */
    private final AtomicBoolean forcedStopFlag = new AtomicBoolean();

    private IndexMBeanRegistration mbeanRegistration;

    private long leaseTimeOut;

    /**
     * Controls the length of the interval (in minutes) at which an indexing
     * error is logged as 'warning'. for the rest of the indexing cycles errors
     * will be logged at 'debug' level
     */
    private static long ERROR_WARN_INTERVAL = TimeUnit.MINUTES.toMillis(Integer
            .getInteger("oak.async.warn.interval", 30));

    /**
     * Timeout in seconds for which close call would wait before forcefully
     * stopping the indexing thread
     */
    private int softTimeOutSecs = Integer.getInteger("oak.async.softTimeOutSecs", 2 * 60);

    private boolean closed;

    /**
     * The checkpoint cleanup interval in minutes. Defaults to 5 minutes.
     * Setting it to a negative value disables automatic cleanup. See OAK-4826.
     */
    private final int cleanupIntervalMinutes
            = Integer.getInteger("oak.async.checkpointCleanupIntervalMinutes", 5);

    /**
     * The time in minutes since the epoch when the last checkpoint cleanup ran.
     */
    private long lastCheckpointCleanUpTime;

    private List<ValidatorProvider> validatorProviders = Collections.emptyList();

    private TrackingCorruptIndexHandler corruptIndexHandler = new TrackingCorruptIndexHandler();

    private final StatisticsProvider statisticsProvider;

    public AsyncIndexUpdate(@Nonnull String name, @Nonnull NodeStore store,
                            @Nonnull IndexEditorProvider provider, boolean switchOnSync) {
        this(name, store, provider, StatisticsProvider.NOOP, switchOnSync);
    }

    public AsyncIndexUpdate(@Nonnull String name, @Nonnull NodeStore store,
                            @Nonnull IndexEditorProvider provider, StatisticsProvider statsProvider, boolean switchOnSync) {
        this.name = checkValidName(name);
        this.lastIndexedTo = lastIndexedTo(name);
        this.store = checkNotNull(store);
        this.provider = checkNotNull(provider);
        this.switchOnSync = switchOnSync;
        this.leaseTimeOut = DEFAULT_ASYNC_TIMEOUT;
        this.statisticsProvider = statsProvider;
        this.indexStats = new AsyncIndexStats(name, statsProvider);
    }

    public AsyncIndexUpdate(@Nonnull String name, @Nonnull NodeStore store,
            @Nonnull IndexEditorProvider provider) {
        this(name, store, provider, false);
    }

    public static String checkValidName(String asyncName){
        checkNotNull(asyncName, "async name should not be null");
        if (IndexConstants.ASYNC_REINDEX_VALUE.equals(asyncName)){
            return asyncName;
        }
        checkArgument(asyncName.endsWith("async"), "async name [%s] does not confirm to " +
                "naming pattern of ending with 'async'", asyncName);
        return asyncName;
    }

    public static boolean isAsyncLaneName(String asyncName){
        return IndexConstants.ASYNC_REINDEX_VALUE.equals(asyncName) || asyncName.endsWith("async");
    }

    /**
     * Index update callback that tries to raise the async status flag when
     * the first index change is detected.
     *
     * @see <a href="https://issues.apache.org/jira/browse/OAK-1292">OAK-1292</a>
     */
    protected static class AsyncUpdateCallback implements IndexUpdateCallback, NodeTraversalCallback {
        /**
         * Interval in terms of number of nodes traversed after which
         * time would be checked for lease expiry
         */
        public static final int LEASE_CHECK_INTERVAL = 10;
        private final NodeStore store;

        /** The base checkpoint */
        private String checkpoint;

        /**
         * Property name which stores the temporary checkpoint that need to be released on the next run
         */
        private final String tempCpName;

        private final long leaseTimeOut;

        private final String name;

        private final String leaseName;

        private final AsyncIndexStats indexStats;

        private final AtomicBoolean forcedStop;

        private List<ValidatorProvider> validatorProviders = Collections.emptyList();

        /**
         * Expiration time of the last lease we committed, null if lease is
         * disabled
         */
        private Long lease = null;

        private boolean hasLease = false;

        public AsyncUpdateCallback(NodeStore store, String name,
                long leaseTimeOut, String checkpoint,
                AsyncIndexStats indexStats, AtomicBoolean forcedStop) {
            this.store = store;
            this.name = name;
            this.forcedStop = forcedStop;
            this.leaseTimeOut = leaseTimeOut;
            this.checkpoint = checkpoint;
            this.tempCpName = getTempCpName(name);
            this.indexStats = indexStats;
            this.leaseName = leasify(name);
        }

        protected void initLease() throws CommitFailedException {
            if (hasLease) {
                return;
            }
            NodeState root = store.getRoot();
            NodeState async = root.getChildNode(ASYNC);
            if(isLeaseCheckEnabled(leaseTimeOut)) {
                long now = getTime();
                this.lease = now + 2 * leaseTimeOut;
                long beforeLease = async.getLong(leaseName);
                if (beforeLease > now) {
                    throw newConcurrentUpdateException();
                }

                NodeBuilder builder = root.builder();
                builder.child(ASYNC).setProperty(leaseName, lease);
                mergeWithConcurrencyCheck(store, validatorProviders, builder, checkpoint, beforeLease, name);
            } else {
                lease = null;
                // remove stale lease info if needed
                if (async.hasProperty(leaseName)) {
                    NodeBuilder builder = root.builder();
                    builder.child(ASYNC).removeProperty(leaseName);
                    mergeWithConcurrencyCheck(store, validatorProviders,
                            builder, checkpoint, null, name);
                }
            }
            hasLease = true;
        }

        protected void prepare(String afterCheckpoint)
                throws CommitFailedException {
            if (!hasLease) {
                initLease();
            }
            NodeState root = store.getRoot();
            NodeBuilder builder = root.builder();
            NodeBuilder async = builder.child(ASYNC);

            updateTempCheckpoints(async, checkpoint, afterCheckpoint);
            mergeWithConcurrencyCheck(store, validatorProviders, builder, checkpoint, lease, name);

            // reset updates counter
            indexStats.reset();
        }

        private void updateTempCheckpoints(NodeBuilder async,
                String checkpoint, String afterCheckpoint) {

            indexStats.setReferenceCheckpoint(checkpoint);
            indexStats.setProcessedCheckpoint(afterCheckpoint);

            // try to drop temp cps, add 'currentCp' to the temp cps list
            Set<String> temps = newHashSet();
            for (String cp : getStrings(async, tempCpName)) {
                if (cp.equals(checkpoint)) {
                    temps.add(cp);
                    continue;
                }
                boolean released = store.release(cp);
                log.debug("[{}] Releasing temporary checkpoint {}: {}", name, cp, released);
                if (!released) {
                    temps.add(cp);
                }
            }
            temps.add(afterCheckpoint);
            async.setProperty(tempCpName, temps, Type.STRINGS);
            indexStats.setTempCheckpoints(temps);
        }

        boolean isDirty() {
            return indexStats.getUpdates() > 0;
        }

        void close() throws CommitFailedException {
            if (isLeaseCheckEnabled(leaseTimeOut)) {
                NodeBuilder builder = store.getRoot().builder();
                NodeBuilder async = builder.child(ASYNC);
                async.removeProperty(leaseName);
                mergeWithConcurrencyCheck(store, validatorProviders, builder,
                        async.getString(name), lease, name);
            }
        }

        @Override
        public void indexUpdate() throws CommitFailedException {
            checkIfStopped();
            indexStats.incUpdates();
        }

        @Override
        public void traversedNode(PathSource pathSource) throws CommitFailedException{
            checkIfStopped();

            if (indexStats.incTraversal() % LEASE_CHECK_INTERVAL == 0 && isLeaseCheckEnabled(leaseTimeOut)) {
                long now = getTime();
                if (now + leaseTimeOut > lease) {
                    long newLease = now + 2 * leaseTimeOut;
                    NodeBuilder builder = store.getRoot().builder();
                    builder.child(ASYNC).setProperty(leaseName, newLease);
                    mergeWithConcurrencyCheck(store, validatorProviders, builder, checkpoint, lease, name);
                    lease = newLease;
                }
            }
        }

        protected long getTime() {
            return System.currentTimeMillis();
        }

        public void setCheckpoint(String checkpoint) {
            this.checkpoint = checkpoint;
        }

        public void setValidatorProviders(List<ValidatorProvider> validatorProviders) {
            this.validatorProviders = checkNotNull(validatorProviders);
        }

        private void checkIfStopped() throws CommitFailedException {
            if (forcedStop.get()){
                forcedStop.set(false);
                throw INTERRUPTED;
            }
        }
    }

    @Override
    public synchronized void run() {
        boolean permitAcquired = false;
        try{
            if (runPermit.tryAcquire()){
                permitAcquired = true;
                runWhenPermitted();
            } else {
                log.warn("[{}] Could not acquire run permit. Stop flag set to [{}] Skipping the run", name, forcedStopFlag);
            }
        } finally {
            if (permitAcquired){
                runPermit.release();
            }
        }
    }


    @Override
    public void close() {
        if (closed) {
            return;
        }
        int hardTimeOut = 5 * softTimeOutSecs;
        if(!runPermit.tryAcquire()){
            //First let current run complete without bothering it
            log.debug("[{}] [WAITING] Indexing in progress. Would wait for {} secs for it to finish", name, softTimeOutSecs);
            try {
                if(!runPermit.tryAcquire(softTimeOutSecs, TimeUnit.SECONDS)){
                    //We have now waited enough. So signal the indexer that it should return right away
                    //as soon as it sees the forcedStopFlag
                    log.debug("[{}] [SOFT LIMIT HIT] Indexing found to be in progress for more than [{}]s. Would " +
                            "signal it to now force stop", name, softTimeOutSecs);
                    forcedStopFlag.set(true);
                    if(!runPermit.tryAcquire(hardTimeOut, TimeUnit.SECONDS)){
                        //Index thread did not listened to our advice. So give up now and warn about it
                        log.warn("[{}] Indexing still not found to be complete. Giving up after [{}]s", name, hardTimeOut);
                    }
                } else {
                    log.info("[{}] [CLOSED OK] Async indexing run completed. Closing it now", name);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            log.info("[{}] Closed", name);
        }
        closed = true;
    }

    private void runWhenPermitted() {
        if (indexStats.isPaused()) {
            log.debug("[{}] Ignoring the run as indexing is paused", name);
            return;
        }
        log.debug("[{}] Running background index task", name);

        NodeState root = store.getRoot();
        NodeState async = root.getChildNode(ASYNC);

        if (isLeaseCheckEnabled(leaseTimeOut)) {
            // check for concurrent updates
            long leaseEndTime = async.getLong(leasify(name));
            long currentTime = System.currentTimeMillis();
            if (leaseEndTime > currentTime) {
                long leaseExpMsg = (leaseEndTime - currentTime) / 1000;
                String err = String.format("Another copy of the index update is already running; skipping this update. " +
                        "Time left for lease to expire %d s. Indexing can resume by %tT", leaseExpMsg, leaseEndTime);
                indexStats.failed(new Exception(err, newConcurrentUpdateException()));
                return;
            }
        }

        // start collecting runtime statistics
        preAsyncRunStatsStats(indexStats);

        // find the last indexed state, and check if there are recent changes
        NodeState before;
        String beforeCheckpoint = async.getString(name);
        AsyncUpdateCallback callback = newAsyncUpdateCallback(store,
                name, leaseTimeOut, beforeCheckpoint, indexStats,
                forcedStopFlag);
        if (beforeCheckpoint != null) {
            NodeState state = store.retrieve(beforeCheckpoint);
            if (state == null) {
                // to make sure we're not reading a stale root rev, we're
                // attempting a write+read via the lease-grab mechanics
                try {
                    callback.initLease();
                } catch (CommitFailedException e) {
                    indexStats.failed(e);
                    return;
                }
                root = store.getRoot();
                beforeCheckpoint = root.getChildNode(ASYNC).getString(name);
                if (beforeCheckpoint != null) {
                    state = store.retrieve(beforeCheckpoint);
                    callback.setCheckpoint(beforeCheckpoint);
                }
            }

            if (state == null) {
                log.warn(
                        "[{}] Failed to retrieve previously indexed checkpoint {}; re-running the initial index update",
                        name, beforeCheckpoint);
                beforeCheckpoint = null;
                callback.setCheckpoint(beforeCheckpoint);
                before = MISSING_NODE;
            } else if (noVisibleChanges(state, root) && !switchOnSync) {
                log.debug(
                        "[{}] No changes since last checkpoint; skipping the index update",
                        name);
                postAsyncRunStatsStatus(indexStats);
                return;
            } else {
                before = state;
            }
        } else {
            log.info("[{}] Initial index update", name);
            before = MISSING_NODE;
        }

        // there are some recent changes, so let's create a new checkpoint
        String afterTime = now();
        String oldThreadName = Thread.currentThread().getName();
        boolean threadNameChanged = false;
        String afterCheckpoint = store.checkpoint(lifetime, ImmutableMap.of(
                "creator", AsyncIndexUpdate.class.getSimpleName(),
                "created", afterTime,
                "thread", oldThreadName,
                "name", name));
        NodeState after = store.retrieve(afterCheckpoint);
        if (after == null) {
            log.debug(
                    "[{}] Unable to retrieve newly created checkpoint {}, skipping the index update",
                    name, afterCheckpoint);
            //Do not update the status as technically the run is not complete
            return;
        }

        String checkpointToRelease = afterCheckpoint;
        boolean updatePostRunStatus = false;
        try {
            String newThreadName = "async-index-update-" + name;
            log.trace("Switching thread name to {}", newThreadName);
            threadNameChanged = true;
            Thread.currentThread().setName(newThreadName);
            updatePostRunStatus = updateIndex(before, beforeCheckpoint, after,
                    afterCheckpoint, afterTime, callback);

            // the update succeeded, i.e. it no longer fails
            if (indexStats.didLastIndexingCycleFailed()) {
                indexStats.fixed();
            }

            // the update succeeded, so we can release the earlier checkpoint
            // otherwise the new checkpoint associated with the failed update
            // will get released in the finally block
            checkpointToRelease = beforeCheckpoint;
            indexStats.setReferenceCheckpoint(afterCheckpoint);
            indexStats.setProcessedCheckpoint("");
            indexStats.releaseTempCheckpoint(afterCheckpoint);

        } catch (Exception e) {
            indexStats.failed(e);

        } finally {
            if (threadNameChanged) {
                log.trace("Switching thread name back to {}", oldThreadName);
                Thread.currentThread().setName(oldThreadName);
            }
            // null during initial indexing
            // and skip release if this cp was used in a split operation
            if (checkpointToRelease != null
                    && !checkpointToRelease.equals(taskSplitter
                            .getLastReferencedCp())) {
                if (!store.release(checkpointToRelease)) {
                    log.debug("[{}] Unable to release checkpoint {}", name,
                            checkpointToRelease);
                }
            }
            maybeCleanUpCheckpoints();

            if (updatePostRunStatus) {
                postAsyncRunStatsStatus(indexStats);
            }
        }
    }

    private void markFailingIndexesAsCorrupt(NodeBuilder builder) {
        for (Map.Entry<String, CorruptIndexInfo> index : corruptIndexHandler.getCorruptIndexData(name).entrySet()){
            NodeBuilder indexBuilder = childBuilder(builder, index.getKey());
            CorruptIndexInfo info = index.getValue();
            if (!indexBuilder.hasProperty(IndexConstants.CORRUPT_PROPERTY_NAME)){
                String corruptSince = ISO8601.format(info.getCorruptSinceAsCal());
                indexBuilder.setProperty(
                        PropertyStates.createProperty(IndexConstants.CORRUPT_PROPERTY_NAME, corruptSince, Type.DATE));
                log.info("Marking [{}] as corrupt. The index is failing {}", info.getPath(), info.getStats());
            } else {
                log.debug("Failing index at [{}] is already marked as corrupt. The index is failing {}",
                        info.getPath(), info.getStats());
            }
        }
    }

    private static NodeBuilder childBuilder(NodeBuilder nb, String path) {
        for (String name : PathUtils.elements(checkNotNull(path))) {
            nb = nb.child(name);
        }
        return nb;
    }

    private void maybeCleanUpCheckpoints() {
        // clean up every five minutes
        long currentMinutes = TimeUnit.MILLISECONDS.toMinutes(
                System.currentTimeMillis());
        if (!indexStats.isFailing()
                && cleanupIntervalMinutes > -1
                && lastCheckpointCleanUpTime + cleanupIntervalMinutes < currentMinutes) {
            try {
                cleanUpCheckpoints();
            } catch (Throwable e) {
                log.warn("Checkpoint clean up failed", e);
            }
            lastCheckpointCleanUpTime = currentMinutes;
        }
    }

    void cleanUpCheckpoints() {
        log.debug("[{}] Cleaning up orphaned checkpoints", name);
        Set<String> keep = newHashSet();
        String cp = indexStats.getReferenceCheckpoint();
        if (cp == null) {
            log.warn("[{}] No reference checkpoint set in index stats", name);
            return;
        }
        keep.add(cp);
        keep.addAll(indexStats.tempCps);
        Map<String, String> info = store.checkpointInfo(cp);
        String value = info.get("created");
        if (value != null) {
            // remove unreferenced AsyncIndexUpdate checkpoints:
            // - without 'created' info (checkpoint created before OAK-4826)
            // or
            // - 'created' value older than the current reference and
            //   not within the lease time frame
            long current = ISO8601.parse(value).getTimeInMillis();
            for (String checkpoint : store.checkpoints()) {
                info = store.checkpointInfo(checkpoint);
                String creator = info.get("creator");
                String created = info.get("created");
                String name = info.get("name");
                if (!keep.contains(checkpoint)
                        && this.name.equals(name)
                        && AsyncIndexUpdate.class.getSimpleName().equals(creator)
                        && (created == null || ISO8601.parse(created).getTimeInMillis() + leaseTimeOut < current)) {
                    if (store.release(checkpoint)) {
                        log.info("[{}] Removed orphaned checkpoint '{}' {}",
                                name, checkpoint, info);
                    }
                }
            }
        }
    }

    protected AsyncUpdateCallback newAsyncUpdateCallback(NodeStore store,
                                                         String name, long leaseTimeOut, String beforeCheckpoint,
                                                         AsyncIndexStats indexStats,
                                                         AtomicBoolean stopFlag) {
        AsyncUpdateCallback callback = new AsyncUpdateCallback(store, name, leaseTimeOut,
                beforeCheckpoint, indexStats, stopFlag);
        callback.setValidatorProviders(validatorProviders);
        return callback;
    }

    protected boolean updateIndex(NodeState before, String beforeCheckpoint,
            NodeState after, String afterCheckpoint, String afterTime,
            AsyncUpdateCallback callback) throws CommitFailedException {
        Stopwatch watch = Stopwatch.createStarted();
        boolean updatePostRunStatus = true;
        boolean progressLogged = false;
        // prepare the update callback for tracking index updates
        // and maintaining the update lease
        callback.prepare(afterCheckpoint);

        // check for index tasks split requests, if a split happened, make
        // sure to not delete the reference checkpoint, as the other index
        // task will take care of it
        taskSplitter.maybeSplit(beforeCheckpoint, callback.lease);
        IndexUpdate indexUpdate = null;
        try {
            NodeBuilder builder = store.getRoot().builder();

            markFailingIndexesAsCorrupt(builder);

            CommitInfo info = new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN,
                    ImmutableMap.of(IndexConstants.CHECKPOINT_CREATION_TIME, afterTime));
            indexUpdate =
                    new IndexUpdate(provider, name, after, builder, callback, callback, info, corruptIndexHandler)
                    .withMissingProviderStrategy(missingStrategy);
            configureRateEstimator(indexUpdate);
            CommitFailedException exception =
                    EditorDiff.process(VisibleEditor.wrap(indexUpdate), before, after);
            if (exception != null) {
                throw exception;
            }

            builder.child(ASYNC).setProperty(name, afterCheckpoint);
            builder.child(ASYNC).setProperty(PropertyStates.createProperty(lastIndexedTo, afterTime, Type.DATE));
            if (callback.isDirty() || before == MISSING_NODE) {
                if (switchOnSync) {
                    reindexedDefinitions.addAll(indexUpdate
                            .getReindexedDefinitions());
                    updatePostRunStatus = false;
                } else {
                    updatePostRunStatus = true;
                }
            } else {
                if (switchOnSync) {
                    log.debug(
                            "[{}] No changes detected after diff; will try to switch to synchronous updates on {}",
                            name, reindexedDefinitions);

                    // no changes after diff, switch to sync on the async defs
                    for (String path : reindexedDefinitions) {
                        NodeBuilder c = builder;
                        for (String p : elements(path)) {
                            c = c.getChildNode(p);
                        }
                        if (c.exists() && !c.getBoolean(REINDEX_PROPERTY_NAME)) {
                            c.removeProperty(ASYNC_PROPERTY_NAME);
                        }
                    }
                    reindexedDefinitions.clear();
                    if (store.release(afterCheckpoint)) {
                        builder.child(ASYNC).removeProperty(name);
                        builder.child(ASYNC).removeProperty(lastIndexedTo);
                    } else {
                        log.debug("[{}] Unable to release checkpoint {}", name, afterCheckpoint);
                    }
                }
                updatePostRunStatus = true;
            }
            mergeWithConcurrencyCheck(store, validatorProviders, builder, beforeCheckpoint,
                    callback.lease, name);
            if (indexUpdate.isReindexingPerformed()) {
                log.info("[{}] Reindexing completed for indexes: {} in {} ({} ms)",
                        name, indexUpdate.getReindexStats(), 
                        watch, watch.elapsed(TimeUnit.MILLISECONDS));
                progressLogged = true;
            }

            corruptIndexHandler.markWorkingIndexes(indexUpdate.getUpdatedIndexPaths());
        } finally {
            if (indexUpdate != null) {
                if (updatePostRunStatus) {
                    indexUpdate.commitProgress(IndexCommitCallback.IndexProgress.COMMIT_SUCCEDED);
                } else {
                    indexUpdate.commitProgress(IndexCommitCallback.IndexProgress.COMMIT_FAILED);
                }
            }
            callback.close();
        }

        if (!progressLogged) {
            String msg = "[{}] AsyncIndex update run completed in {}. Indexed {} nodes, {}";
            //Log at info level if time taken is more than 5 min
            if (watch.elapsed(TimeUnit.MINUTES) >= 5) {
                log.info(msg, name, watch, indexStats.getUpdates(), indexUpdate.getIndexingStats());
            } else {
                log.debug(msg, name, watch, indexStats.getUpdates(), indexUpdate.getIndexingStats());
            }
        }

        return updatePostRunStatus;
    }

    private void configureRateEstimator(IndexUpdate indexUpdate) {
        //As metrics is an optional library guard the access with the check
        if (statisticsProvider.getClass().getSimpleName().equals("MetricStatisticsProvider")){
            MetricRegistry registry = ((MetricStatisticsProvider) statisticsProvider).getRegistry();
            indexUpdate.setTraversalRateEstimator(new MetricRateEstimator(name, registry));
        }

        NodeCounterMBeanEstimator estimator = new NodeCounterMBeanEstimator(store);
        indexUpdate.setNodeCountEstimator(estimator);
    }

    public static String leasify(String name) {
        return name + "-lease";
    }

    static String lastIndexedTo(String name) {
        return name + "-LastIndexedTo";
    }

    private static String getTempCpName(String name) {
        return name + "-temp";
    }

    private static boolean isLeaseCheckEnabled(long leaseTimeOut) {
        return leaseTimeOut > 0;
    }

    private static void mergeWithConcurrencyCheck(final NodeStore store, List<ValidatorProvider> validatorProviders,
                                                  NodeBuilder builder, final String checkpoint, final Long lease,
                                                  final String name) throws CommitFailedException {
        CommitHook concurrentUpdateCheck = new CommitHook() {
            @Override @Nonnull
            public NodeState processCommit(
                    NodeState before, NodeState after, CommitInfo info)
                    throws CommitFailedException {
                // check for concurrent updates by this async task
                NodeState async = before.getChildNode(ASYNC);
                if ((checkpoint == null || Objects.equal(checkpoint, async.getString(name)))
                    &&
                    (lease == null      || lease == async.getLong(leasify(name)))) {
                    return after;
                } else {
                    throw newConcurrentUpdateException();
                }
            }
        };
        List<EditorProvider> editorProviders = Lists.newArrayList();
        editorProviders.add(new ConflictValidatorProvider());
        editorProviders.addAll(validatorProviders);
        CompositeHook hooks = new CompositeHook(
                ResetCommitAttributeHook.INSTANCE,
                ConflictHook.of(new AnnotatingConflictHandler()),
                new EditorHook(CompositeEditorProvider.compose(editorProviders)),
                concurrentUpdateCheck);
        try {
            store.merge(builder, hooks, createCommitInfo());
        } catch (CommitFailedException ex) {
            // OAK-2961
            if (ex.isOfType(CommitFailedException.STATE) && ex.getCode() == 1) {
                throw newConcurrentUpdateException();
            } else {
                throw ex;
            }
        }
    }

    private static CommitInfo createCommitInfo() {
        Map<String, Object> info = ImmutableMap.<String, Object>of(CommitContext.NAME, new SimpleCommitContext());
        return new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN, info);
    }

    /**
     * Milliseconds for the timeout
     */
    protected AsyncIndexUpdate setLeaseTimeOut(long leaseTimeOut) {
        this.leaseTimeOut = leaseTimeOut;
        return this;
    }

    protected long getLeaseTimeOut() {
        return leaseTimeOut;
    }

    protected AsyncIndexUpdate setCloseTimeOut(int timeOutInSec) {
        this.softTimeOutSecs = timeOutInSec;
        return this;
    }

    public void setValidatorProviders(List<ValidatorProvider> validatorProviders) {
        this.validatorProviders = checkNotNull(validatorProviders);
    }

    public void setCorruptIndexHandler(TrackingCorruptIndexHandler corruptIndexHandler) {
        this.corruptIndexHandler = checkNotNull(corruptIndexHandler);
    }

    TrackingCorruptIndexHandler getCorruptIndexHandler() {
        return corruptIndexHandler;
    }

    public boolean isClosed(){
        return closed || forcedStopFlag.get();
    }

    boolean isClosing(){
        return runPermit.hasQueuedThreads();
    }

    private static void preAsyncRunStatsStats(AsyncIndexStats stats) {
        stats.start(now());
    }

    private static void postAsyncRunStatsStatus(AsyncIndexStats stats) {
        stats.done(now());
    }

    private static String now() {
        return ISO8601.format(Calendar.getInstance());
    }

    public AsyncIndexStats getIndexStats() {
        return indexStats;
    }

    public boolean isFinished() {
        return indexStats.getStatus() == STATUS_DONE;
    }

    final class AsyncIndexStats extends AnnotatedStandardMBean implements  IndexStatsMBean {

        protected AsyncIndexStats(String name, StatisticsProvider statsProvider) {
            super(IndexStatsMBean.class);
            this.execStats = new ExecutionStats(name, statsProvider);
        }

        private String start = "";
        private String done = "";
        private String status = STATUS_INIT;
        private String referenceCp = "";
        private String processedCp = "";
        private Set<String> tempCps = new HashSet<String>();

        private volatile boolean isPaused;
        private volatile long updates;
        private volatile long nodesRead;
        private final Stopwatch watch = Stopwatch.createUnstarted();
        private final ExecutionStats execStats;

        /** Flag to avoid repeatedly logging failure warnings */
        private volatile boolean failing = false;
        private long latestErrorWarn = 0;

        private String failingSince = "";
        private String latestError = null;
        private String latestErrorTime = "";
        private long consecutiveFailures = 0;

        public void start(String now) {
            status = STATUS_RUNNING;
            start = now;
            done = "";

            if (watch.isRunning()) {
                watch.reset();
            }
            watch.start();
        }

        public void done(String now) {
            if (corruptIndexHandler.isFailing(name)){
                status = STATUS_FAILING;
            } else {
                status = STATUS_DONE;
            }
            done = now;
            if (watch.isRunning()) {
                watch.stop();
            }
            execStats.doneOneCycle(watch.elapsed(TimeUnit.MILLISECONDS), updates);
            watch.reset();
        }

        public void failed(Exception e) {
            if (e == INTERRUPTED){
                status = STATUS_INTERRUPTED;
                log.info("[{}] The index update interrupted", name);
                log.debug("[{}] The index update interrupted", name, e);
                return;
            }

            latestError = getStackTraceAsString(e);
            latestErrorTime = now();
            consecutiveFailures++;
            if (!failing) {
                // first occurrence of a failure
                failing = true;
                // reusing value so value display is consistent
                failingSince = latestErrorTime;
                latestErrorWarn = System.currentTimeMillis();
                log.warn("[{}] The index update failed", name, e);
            } else {
                // subsequent occurrences
                boolean warn = System.currentTimeMillis() - latestErrorWarn > ERROR_WARN_INTERVAL;
                if (warn) {
                    latestErrorWarn = System.currentTimeMillis();
                    log.warn("[{}] The index update is still failing", name, e);
                } else {
                    log.debug("[{}] The index update is still failing", name, e);
                }
            }
        }

        public void fixed() {
            if (corruptIndexHandler.isFailing(name)){
                log.info("[{}] Index update no longer fails but some corrupt indexes have been skipped {}", name,
                        corruptIndexHandler.getCorruptIndexData(name).keySet());
            } else {
                log.info("[{}] Index update no longer fails", name);
            }

            failing = false;
            failingSince = "";
            consecutiveFailures = 0;
            latestErrorWarn = 0;
            latestError = null;
            latestErrorTime = "";
        }

        public boolean isFailing() {
            return failing || corruptIndexHandler.isFailing(name);
        }

        public boolean didLastIndexingCycleFailed(){
            return failing;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getStart() {
            return start;
        }

        @Override
        public String getDone() {
            return done;
        }

        @Override
        public String getStatus() {
            return status;
        }

        @Override
        public String getLastIndexedTime() {
            PropertyState ps = store.getRoot().getChildNode(ASYNC).getProperty(lastIndexedTo);
            return ps != null ? ps.getValue(Type.STRING) : null;
        }

        @Override
        public void pause() {
            log.debug("[{}] Pausing the async indexer", name);
            this.isPaused = true;
        }

        @Override
        public String abortAndPause() {
            //First pause to avoid any race
            pause();
            //Set the forcedStop flag anyway. In resume this would be cleared
            forcedStopFlag.set(true);
            String msg = "";
            //Abort if any indexing run is in progress
            if (runPermit.availablePermits() == 0){
                msg = "Abort request placed for current run. ";
            }
            return msg + "Indexing is paused now. Invoke 'resume' to resume indexing";
        }

        @Override
        public void resume() {
            log.debug("[{}] Resuming the async indexer", name);
            this.isPaused = false;

            //Clear the forcedStop flag as fail safe
            forcedStopFlag.set(false);
        }

        @Override
        public boolean isPaused() {
            return this.isPaused;
        }

        void reset() {
            this.updates = 0;
            this.nodesRead = 0;
        }

        long incUpdates() {
            return ++updates;
        }

        long incTraversal() {
            return ++nodesRead;
        }

        @Override
        public long getUpdates() {
            return updates;
        }

        @Override
        public long getNodesReadCount(){
            return nodesRead;
        }

        void setReferenceCheckpoint(String checkpoint) {
            this.referenceCp = checkpoint;
        }

        @Override
        public String getReferenceCheckpoint() {
            return referenceCp;
        }

        void setProcessedCheckpoint(String checkpoint) {
            this.processedCp = checkpoint;
        }

        @Override
        public String getProcessedCheckpoint() {
            return processedCp;
        }

        void setTempCheckpoints(Set<String> tempCheckpoints) {
            this.tempCps = tempCheckpoints;
        }

        void releaseTempCheckpoint(String tempCheckpoint) {
            this.tempCps.remove(tempCheckpoint);
        }

        @Override
        public String getTemporaryCheckpoints() {
            return tempCps.toString();
        }

        @Override
        public long getTotalExecutionCount() {
            return execStats.getExecutionCounter().getCount();
        }

        @Override
        public CompositeData getExecutionCount() {
            return execStats.getExecutionCount();
        }

        @Override
        public CompositeData getExecutionTime() {
            //Do nothing. Kept for backward compatibility
            return null;
        }

        @Override
        public CompositeData getIndexedNodesCount() {
            return execStats.getIndexedNodesCount();
        }

        @Override
        public CompositeData getConsolidatedExecutionStats() {
            return execStats.getConsolidatedStats();
        }

        @Override
        public void resetConsolidatedExecutionStats() {
            //Do nothing. Kept for backward compatibility
        }

        @Override
        public String toString() {
            return "AsyncIndexStats [start=" + start + ", done=" + done
                    + ", status=" + status + ", paused=" + isPaused
                    + ", failing=" + failing + ", failingSince=" + failingSince
                    + ", consecutiveFailures=" + consecutiveFailures
                    + ", updates=" + updates + ", referenceCheckpoint="
                    + referenceCp + ", processedCheckpoint=" + processedCp
                    + " ,tempCheckpoints=" + tempCps + ", latestErrorTime="
                    + latestErrorTime + ", latestError=" + latestError + " ]";
        }

        ExecutionStats getExecutionStats() {
            return execStats;
        }

        class ExecutionStats {
            public static final String INDEXER_COUNT = "INDEXER_COUNT";
            public static final String INDEXER_NODE_COUNT = "INDEXER_NODE_COUNT";
            private final MeterStats indexerExecutionCountMeter;
            private final MeterStats indexedNodeCountMeter;
            private final TimerStats indexerTimer;
            private final HistogramStats indexedNodePerCycleHisto;
            private StatisticsProvider statisticsProvider;

            private final String[] names = {"Executions", "Nodes"};
            private final String name;
            private CompositeType consolidatedType;

            public ExecutionStats(String name, StatisticsProvider statsProvider) {
                this.name = name;
                this.statisticsProvider = statsProvider;
                indexerExecutionCountMeter = statsProvider.getMeter(stats(INDEXER_COUNT), StatsOptions.DEFAULT);
                indexedNodeCountMeter = statsProvider.getMeter(stats(INDEXER_NODE_COUNT), StatsOptions.DEFAULT);
                indexerTimer = statsProvider.getTimer(stats("INDEXER_TIME"), StatsOptions.METRICS_ONLY);
                indexedNodePerCycleHisto = statsProvider.getHistogram(stats("INDEXER_NODE_COUNT_HISTO"), StatsOptions
                        .METRICS_ONLY);
                try {
                    consolidatedType = new CompositeType("ConsolidatedStats",
                        "Consolidated stats", names,
                        names,
                        new OpenType[] {SimpleType.LONG, SimpleType.LONG});
                } catch (OpenDataException e) {
                    log.warn("[{}] Error in creating CompositeType for consolidated stats", AsyncIndexUpdate.this.name, e);
                }
            }

            public void doneOneCycle(long timeInMillis, long updates){
                indexerExecutionCountMeter.mark();
                indexedNodeCountMeter.mark(updates);
                indexerTimer.update(timeInMillis, TimeUnit.MILLISECONDS);
                indexedNodePerCycleHisto.update(updates);
            }

            public Counting getExecutionCounter() {
                return indexerExecutionCountMeter;
            }

            public Counting getIndexedNodeCount() {
                return indexedNodeCountMeter;
            }

            private CompositeData getExecutionCount() {
                return TimeSeriesStatsUtil.asCompositeData(getTimeSeries(stats(INDEXER_COUNT)),
                        "Indexer Execution Count");
            }

            private CompositeData getIndexedNodesCount() {
                return TimeSeriesStatsUtil.asCompositeData(getTimeSeries(stats(INDEXER_NODE_COUNT)),
                        "Indexer Node Count");
            }

            private CompositeData getConsolidatedStats() {
                try {
                    Long[] values = new Long[]{indexerExecutionCountMeter.getCount(),
                            indexedNodeCountMeter.getCount()};
                    return new CompositeDataSupport(consolidatedType, names, values);
                } catch (Exception e) {
                    log.error("[{}] Error retrieving consolidated stats", name, e);
                    return null;
                }
            }

            private String stats(String suffix){
                return name + "." + suffix;
            }

            private TimeSeries getTimeSeries(String name) {
                return statisticsProvider.getStats().getTimeSeries(name, true);
            }
        }

        @Override
        public void splitIndexingTask(String paths, String newIndexTaskName) {
            splitIndexingTask(newHashSet(Splitter.on(",").trimResults()
                    .omitEmptyStrings().split(paths)), newIndexTaskName);
        }

        private void splitIndexingTask(Set<String> paths,
                String newIndexTaskName) {
            taskSplitter.registerSplit(paths, newIndexTaskName);
        }

        @Override
        public void registerAsyncIndexer(String name, long delayInSeconds) {
            taskSplitter.registerAsyncIndexer(name, delayInSeconds);
        }

        @Override
        public String getFailingSince() {
            return failingSince;
        }

        @Override
        public long getConsecutiveFailedExecutions() {
            return consecutiveFailures;
        }

        @Override
        public String getLatestError() {
            return latestError;
        }

        @Override
        public String getLatestErrorTime() {
            return latestErrorTime;
        }

        @Override
        public TabularData getFailingIndexStats() {
            return corruptIndexHandler.getFailingIndexStats(name);
        }
    }

    /**
     * Checks whether there are no visible changes between the given states.
     */
    private static boolean noVisibleChanges(NodeState before, NodeState after) {
        return after.compareAgainstBaseState(before, new NodeStateDiff() {
            @Override
            public boolean propertyAdded(PropertyState after) {
                return isHidden(after.getName());
            }
            @Override
            public boolean propertyChanged(
                    PropertyState before, PropertyState after) {
                return isHidden(after.getName());
            }
            @Override
            public boolean propertyDeleted(PropertyState before) {
                return isHidden(before.getName());
            }
            @Override
            public boolean childNodeAdded(String name, NodeState after) {
                return isHidden(name);
            }
            @Override
            public boolean childNodeChanged(
                    String name, NodeState before, NodeState after) {
                return isHidden(name)
                        || after.compareAgainstBaseState(before, this);
            }
            @Override
            public boolean childNodeDeleted(String name, NodeState before) {
                return isHidden(name);
            }
        });
    }

    private static boolean isHidden(String name) {
        return name.charAt(0) == ':';
    }

    static class DefaultMissingIndexProviderStrategy extends
            MissingIndexProviderStrategy {

        @Override
        public void onMissingIndex(String type, NodeBuilder definition, String path)
                throws CommitFailedException {
            if (isDisabled(type)) {
                return;
            }
            throw new CommitFailedException("Async", 2,
                    "Missing index provider detected for type [" + type
                            + "] on index [" + path + "]");
        }
    }

    public boolean isFailing() {
        return indexStats.isFailing();
    }

    class IndexTaskSpliter {

        private Set<String> paths = null;
        private String newIndexTaskName = null;
        private String lastReferencedCp;

        private Set<String> registeredTasks = newHashSet();

        void registerSplit(Set<String> paths, String newIndexTaskName) {
            log.info(
                    "[{}] Registered split of following index definitions {} to new async task {}.",
                    name, paths, newIndexTaskName);
            this.paths = newHashSet(paths);
            this.newIndexTaskName = newIndexTaskName;
        }

        void maybeSplit(@CheckForNull String refCheckpoint, Long lease)
                throws CommitFailedException {
            if (paths == null) {
                return;
            }
            split(refCheckpoint, lease);
        }

        private void split(@CheckForNull String refCheckpoint, Long lease) throws CommitFailedException {
            NodeBuilder builder = store.getRoot().builder();
            if (refCheckpoint != null) {
                String tempCpName = getTempCpName(name);
                NodeBuilder async = builder.child(ASYNC);
                // add new reference
                async.setProperty(newIndexTaskName, refCheckpoint);
                // update old 'temp' list: remove refcp so it doesn't get released on next run
                Set<String> temps = newHashSet();
                for (String cp : getStrings(async, tempCpName)) {
                    if (cp.equals(refCheckpoint)) {
                        continue;
                    }
                    temps.add(cp);
                }
                async.setProperty(tempCpName, temps, Type.STRINGS);
                indexStats.setTempCheckpoints(temps);
            }

            // update index defs name => newIndexTaskName
            Set<String> updated = newHashSet();
            for (String path : paths) {
                NodeBuilder c = builder;
                for (String p : elements(path)) {
                    c = c.getChildNode(p);
                }
                if (c.exists() && name.equals(c.getString("async"))) {
                    //TODO Fix this to account for nrt and sync
                    c.setProperty("async", newIndexTaskName);
                    updated.add(path);
                }
            }

            if (!updated.isEmpty()) {
                mergeWithConcurrencyCheck(store, validatorProviders, builder, refCheckpoint, lease, name);
                log.info(
                        "[{}] Successfully split index definitions {} to async task named {} with referenced checkpoint {}.",
                        name, updated, newIndexTaskName, refCheckpoint);
                lastReferencedCp = refCheckpoint;
            }
            paths = null;
            newIndexTaskName = null;
        }

        public String getLastReferencedCp() {
            return lastReferencedCp;
        }

        void registerAsyncIndexer(String newTask, long delayInSeconds) {
            if (registeredTasks.contains(newTask)) {
                // prevent accidental double call
                log.warn("[{}] Task {} is already registered.", name, newTask);
                return;
            }
            if (mbeanRegistration != null) {
                log.info(
                        "[{}] Registering a new indexing task {} running each {} seconds.",
                        name, newTask, delayInSeconds);
                AsyncIndexUpdate task = new AsyncIndexUpdate(newTask, store,
                        provider);
                mbeanRegistration.registerAsyncIndexer(task, delayInSeconds);
                registeredTasks.add(newTask);
            }
        }
    }

    private static Iterable<String> getStrings(NodeBuilder b, String p) {
        PropertyState ps = b.getProperty(p);
        if (ps != null) {
            return ps.getValue(Type.STRINGS);
        }
        return newHashSet();
    }

    IndexTaskSpliter getTaskSplitter() {
        return taskSplitter;
    }

    public void setIndexMBeanRegistration(IndexMBeanRegistration mbeanRegistration) {
        this.mbeanRegistration = mbeanRegistration;
    }

    public String getName() {
        return name;
    }

    private static CommitFailedException newConcurrentUpdateException() {
        return new CommitFailedException("Async", 1, "Concurrent update detected");
    }
}
