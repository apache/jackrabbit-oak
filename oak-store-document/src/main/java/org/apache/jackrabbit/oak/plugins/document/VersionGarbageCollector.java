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

package org.apache.jackrabbit.oak.plugins.document;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.guava.common.base.Function;
import org.apache.jackrabbit.guava.common.base.Joiner;
import org.apache.jackrabbit.guava.common.base.Predicate;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.base.Supplier;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.guava.common.collect.Sets;

import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.util.TimeInterval;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.gc.DelegatingGCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.commons.TimeDurationFormatter;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.round;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.apache.jackrabbit.guava.common.base.StandardSystemProperty.LINE_SEPARATOR;
import static org.apache.jackrabbit.guava.common.base.Stopwatch.createUnstarted;
import static org.apache.jackrabbit.guava.common.collect.Iterables.all;
import static org.apache.jackrabbit.guava.common.collect.Iterators.partition;
import static org.apache.jackrabbit.guava.common.util.concurrent.Atomics.newReference;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MIN_ID_VALUE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.COMMIT_ROOT_ONLY;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_LEAF;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_NO_BRANCH;
import static org.slf4j.helpers.MessageFormatter.arrayFormat;

public class VersionGarbageCollector {

    //Kept less than MongoDocumentStore.IN_CLAUSE_BATCH_SIZE to avoid re-partitioning
    private static final int DELETE_BATCH_SIZE = 450;
    private static final int UPDATE_BATCH_SIZE = 450;
    private static final int PROGRESS_BATCH_SIZE = 10000;
    private static final int DETAILED_GC_BATCH_SIZE = 1000;
    private static final String STATUS_IDLE = "IDLE";
    private static final String STATUS_INITIALIZING = "INITIALIZING";
    private static final Logger log = LoggerFactory.getLogger(VersionGarbageCollector.class);

    /**
     * Split document types which can be safely garbage collected
     */
    private static final Set<NodeDocument.SplitDocType> GC_TYPES = EnumSet.of(
            DEFAULT_LEAF, COMMIT_ROOT_ONLY, DEFAULT_NO_BRANCH);

    /**
     * Document id stored in settings collection that keeps info about version gc
     */
    static final String SETTINGS_COLLECTION_ID = "versionGC";

    /**
     * Property name to timestamp when last gc run happened
     */
    static final String SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP = "lastOldestTimeStamp";

    /**
     * Property name to recommended time interval for next collection run
     */
    static final String SETTINGS_COLLECTION_REC_INTERVAL_PROP = "recommendedIntervalMs";

    /**
     * Property name to timestamp till when last detailed-GC run happened
     */
    static final String SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP = "detailedGCTimeStamp";

    /**
     * Property name to _id till when last detailed-GC run happened
     */
    static final String SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP = "detailedGCId";

    private final DocumentNodeStore nodeStore;
    private final DocumentStore ds;
    private final boolean detailedGCEnabled;
    private final VersionGCSupport versionStore;
    private final AtomicReference<GCJob> collector = newReference();
    private VersionGCOptions options;
    private GCMonitor gcMonitor = GCMonitor.EMPTY;
    private RevisionGCStats gcStats = new RevisionGCStats(StatisticsProvider.NOOP);

    VersionGarbageCollector(DocumentNodeStore nodeStore,
                            VersionGCSupport gcSupport,
                            final boolean detailedGCEnabled) {
        this.nodeStore = nodeStore;
        this.versionStore = gcSupport;
        this.ds = gcSupport.getDocumentStore();
        this.detailedGCEnabled = detailedGCEnabled;
        this.options = new VersionGCOptions();
    }

    void setStatisticsProvider(StatisticsProvider provider) {
        this.gcStats = new RevisionGCStats(provider);
    }

    @NotNull
    RevisionGCStats getRevisionGCStats() {
        return gcStats;
    }

    public VersionGCStats gc(long maxRevisionAge, TimeUnit unit) throws IOException {
        long maxRevisionAgeInMillis = unit.toMillis(maxRevisionAge);
        TimeInterval maxRunTime = new TimeInterval(nodeStore.getClock().getTime(), Long.MAX_VALUE);
        if (options.maxDurationMs > 0) {
            maxRunTime = maxRunTime.startAndDuration(options.maxDurationMs);
        }
        GCJob job = new GCJob(maxRevisionAgeInMillis, options, gcMonitor);
        if (collector.compareAndSet(null, job)) {
            VersionGCStats overall = new VersionGCStats();
            overall.active.start();
            gcStats.started();
            boolean success = false;
            try {
                long averageDurationMs = 0;
                while (maxRunTime.contains(nodeStore.getClock().getTime() + averageDurationMs)) {
                    gcMonitor.info("Start {}. run (avg duration {} sec)",
                            overall.iterationCount + 1, averageDurationMs / 1000.0);
                    VersionGCStats stats = job.run();

                    overall.addRun(stats);
                    if (options.maxIterations > 0 && overall.iterationCount >= options.maxIterations) {
                        break;
                    }
                    if (!overall.needRepeat) {
                        break;
                    }
                    averageDurationMs = ((averageDurationMs * (overall.iterationCount - 1))
                            + stats.active.elapsed(TimeUnit.MILLISECONDS)) / overall.iterationCount;
                }
                success = true;
                return overall;
            } finally {
                overall.active.stop();
                collector.set(null);
                overall.success = success;
                gcStats.finished(overall);
                if (overall.iterationCount > 1) {
                    gcMonitor.info("Revision garbage collection finished after {} iterations - aggregate statistics: {}",
                            overall.iterationCount, overall);
                }
            }
        } else {
            throw new IOException("Revision garbage collection is already running");
        }
    }

    public void cancel() {
        GCJob job = collector.get();
        if (job != null) {
            job.cancel();
        }
    }

    public String getStatus() {
        GCJob job = collector.get();
        if (job == null) {
            return STATUS_IDLE;
        } else {
            return job.getStatus();
        }
    }

    public void setGCMonitor(@NotNull GCMonitor gcMonitor) {
        this.gcMonitor = requireNonNull(gcMonitor);
    }

    public VersionGCOptions getOptions() {
        return this.options;
    }

    public void setOptions(VersionGCOptions options) {
        this.options = options;
    }

    public void reset() {
        ds.remove(SETTINGS, SETTINGS_COLLECTION_ID);
    }

    public VersionGCInfo getInfo(long maxRevisionAge, TimeUnit unit)
            throws IOException {
        long maxRevisionAgeInMillis = unit.toMillis(maxRevisionAge);
        long now = nodeStore.getClock().getTime();
        VersionGCRecommendations rec = new VersionGCRecommendations(maxRevisionAgeInMillis, nodeStore.getCheckpoints(),
                nodeStore.getClock(), versionStore, options, gcMonitor, detailedGCEnabled);
        int estimatedIterations = -1;
        if (rec.suggestedIntervalMs > 0) {
            estimatedIterations = (int)Math.ceil(
                    (now - rec.scope.toMs) / rec.suggestedIntervalMs);
        }
        return new VersionGCInfo(rec.lastOldestTimestamp, rec.scope.fromMs,
                rec.deleteCandidateCount, rec.maxCollect,
                rec.suggestedIntervalMs, rec.scope.toMs, estimatedIterations);
    }

    public static class VersionGCInfo {
        public final long lastSuccess;
        public final long oldestRevisionEstimate;
        public final long revisionsCandidateCount;
        public final long collectLimit;
        public final long recommendedCleanupInterval;
        public final long recommendedCleanupTimestamp;
        public final int estimatedIterations;

        VersionGCInfo(long lastSuccess,
                      long oldestRevisionEstimate,
                      long revisionsCandidateCount,
                      long collectLimit,
                      long recommendedCleanupInterval,
                      long recommendedCleanupTimestamp,
                      int estimatedIterations) {
            this.lastSuccess = lastSuccess;
            this.oldestRevisionEstimate = oldestRevisionEstimate;
            this.revisionsCandidateCount = revisionsCandidateCount;
            this.collectLimit = collectLimit;
            this.recommendedCleanupInterval = recommendedCleanupInterval;
            this.recommendedCleanupTimestamp = recommendedCleanupTimestamp;
            this.estimatedIterations = estimatedIterations;
        }
    }

    public static class VersionGCStats {
        boolean ignoredGCDueToCheckPoint;
        boolean ignoredDetailedGCDueToCheckPoint;
        boolean canceled;
        boolean success = true;
        boolean limitExceeded;
        boolean needRepeat;
        int iterationCount;
        int deletedDocGCCount;
        int deletedLeafDocGCCount;
        int splitDocGCCount;
        int intermediateSplitDocGCCount;
        int updateResurrectedGCCount;
        long oldestModifiedDocTimeStamp;
        String oldestModifiedDocId;
        int updatedDetailedGCDocsCount;
        int deletedPropsGCCount;
        final TimeDurationFormatter df = TimeDurationFormatter.forLogging();
        final Stopwatch active = Stopwatch.createUnstarted();
        final Stopwatch collectDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch checkDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch detailedGCDocs = Stopwatch.createUnstarted();
        final Stopwatch deleteDetailedGCDocs = Stopwatch.createUnstarted();
        final Stopwatch deleteSplitDocs = Stopwatch.createUnstarted();
        final Stopwatch sortDocIds = Stopwatch.createUnstarted();
        final Stopwatch updateResurrectedDocuments = Stopwatch.createUnstarted();
        final Stopwatch deleteDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch collectAndDeleteSplitDocs = Stopwatch.createUnstarted();
        final Stopwatch collectDeletedProps = Stopwatch.createUnstarted();
        final Stopwatch collectDeletedOldRevs = Stopwatch.createUnstarted();
        final Stopwatch collectUnmergedBC = Stopwatch.createUnstarted();
        long activeElapsed, collectDeletedDocsElapsed, checkDeletedDocsElapsed, deleteDeletedDocsElapsed, collectAndDeleteSplitDocsElapsed,
                deleteSplitDocsElapsed, sortDocIdsElapsed, updateResurrectedDocumentsElapsed, detailedGCDocsElapsed, collectDeletedPropsElapsed,
                deleteDetailedGCDocsElapsed, collectDeletedOldRevsElapsed, collectUnmergedBCElapsed;

        @Override
        public String toString() {
            String timings;
            String fmt = "timeToCollectDeletedDocs=%s, timeToCheckDeletedDocs=%s, timeToSortDocIds=%s, timeTakenToUpdateResurrectedDocs=%s, timeTakenToDeleteDeletedDocs=%s, timeTakenToCollectAndDeleteSplitDocs=%s%s";

            // aggregated timings?
            if (iterationCount > 0) {
                String timeDeletingSplitDocs = "";
                if (deleteSplitDocsElapsed > 0) {
                    timeDeletingSplitDocs = String.format(" (of which %s deleting)",
                            df.format(deleteSplitDocsElapsed, MICROSECONDS));
                }
                timings = String.format(fmt, df.format(collectDeletedDocsElapsed, MICROSECONDS),
                        df.format(checkDeletedDocsElapsed, MICROSECONDS), df.format(sortDocIdsElapsed, MICROSECONDS),
                        df.format(updateResurrectedDocumentsElapsed, MICROSECONDS),
                        df.format(deleteDeletedDocsElapsed, MICROSECONDS),
                        df.format(collectAndDeleteSplitDocsElapsed, MICROSECONDS),
                        df.format(detailedGCDocsElapsed, MICROSECONDS),
                        df.format(deleteDetailedGCDocsElapsed, MICROSECONDS),
                        df.format(collectDeletedPropsElapsed, MICROSECONDS),
                        df.format(collectDeletedOldRevsElapsed, MICROSECONDS),
                        df.format(collectUnmergedBCElapsed, MICROSECONDS),
                        timeDeletingSplitDocs);
            } else {
                String timeDeletingSplitDocs = "";
                if (deleteSplitDocs.elapsed(MICROSECONDS) > 0) {
                    timeDeletingSplitDocs = String.format(" (of which %s deleting)",
                            df.format(deleteSplitDocs.elapsed(MICROSECONDS), MICROSECONDS));
                }
                timings = String.format(fmt, df.format(collectDeletedDocs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(checkDeletedDocs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(sortDocIds.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(updateResurrectedDocuments.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(deleteDeletedDocs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(collectAndDeleteSplitDocs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(detailedGCDocs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(deleteDetailedGCDocs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(collectDeletedProps.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(collectDeletedOldRevs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(collectUnmergedBC.elapsed(MICROSECONDS), MICROSECONDS),
                        timeDeletingSplitDocs);
            }

            return "VersionGCStats{" +
                    "ignoredGCDueToCheckPoint=" + ignoredGCDueToCheckPoint +
                    "ignoredDetailedGCDueToCheckPoint=" + ignoredDetailedGCDueToCheckPoint +
                    ", canceled=" + canceled +
                    ", deletedDocGCCount=" + deletedDocGCCount + " (of which leaf: " + deletedLeafDocGCCount + ")" +
                    ", updateResurrectedGCCount=" + updateResurrectedGCCount +
                    ", splitDocGCCount=" + splitDocGCCount +
                    ", intermediateSplitDocGCCount=" + intermediateSplitDocGCCount +
                    ", oldestModifiedDocId=" + oldestModifiedDocId +
                    ", oldestModifiedDocTimeStamp=" + oldestModifiedDocTimeStamp +
                    ", updatedDetailedGCDocsCount=" + updatedDetailedGCDocsCount +
                    ", deletedPropsGCCount=" + deletedPropsGCCount +
                    ", iterationCount=" + iterationCount +
                    ", timeActive=" + df.format(activeElapsed, MICROSECONDS) +
                    ", " + timings + "}";
        }

        void addRun(VersionGCStats run) {
            ++iterationCount;
            this.ignoredGCDueToCheckPoint = run.ignoredGCDueToCheckPoint;
            this.ignoredDetailedGCDueToCheckPoint = run.ignoredDetailedGCDueToCheckPoint;
            this.canceled = run.canceled;
            this.success = run.success;
            this.limitExceeded = run.limitExceeded;
            this.needRepeat = run.needRepeat;
            this.deletedDocGCCount += run.deletedDocGCCount;
            this.deletedLeafDocGCCount += run.deletedLeafDocGCCount;
            this.splitDocGCCount += run.splitDocGCCount;
            this.intermediateSplitDocGCCount += run.intermediateSplitDocGCCount;
            this.updateResurrectedGCCount += run.updateResurrectedGCCount;
            this.oldestModifiedDocTimeStamp = run.oldestModifiedDocTimeStamp;
            this.oldestModifiedDocId = run.oldestModifiedDocId;
            this.updatedDetailedGCDocsCount += run.updatedDetailedGCDocsCount;
            this.deletedPropsGCCount += run.deletedPropsGCCount;
            if (run.iterationCount > 0) {
                // run is cumulative with times in elapsed fields
                this.activeElapsed += run.activeElapsed;
                this.collectDeletedDocsElapsed += run.collectDeletedDocsElapsed;
                this.checkDeletedDocsElapsed += run.checkDeletedDocsElapsed;
                this.deleteDeletedDocsElapsed += run.deleteDeletedDocsElapsed;
                this.collectAndDeleteSplitDocsElapsed += run.collectAndDeleteSplitDocsElapsed;
                this.deleteSplitDocsElapsed += run.deleteSplitDocsElapsed;
                this.sortDocIdsElapsed += run.sortDocIdsElapsed;
                this.updateResurrectedDocumentsElapsed += run.updateResurrectedDocumentsElapsed;
                this.detailedGCDocsElapsed += run.detailedGCDocsElapsed;
                this.deleteDetailedGCDocsElapsed += run.deleteDetailedGCDocsElapsed;
                this.collectDeletedPropsElapsed += run.collectDeletedPropsElapsed;
                this.collectDeletedOldRevsElapsed += run.collectDeletedOldRevsElapsed;
                this.collectUnmergedBCElapsed += run.collectUnmergedBCElapsed;
            } else {
                // single run -> read from stop watches
                this.activeElapsed += run.active.elapsed(MICROSECONDS);
                this.collectDeletedDocsElapsed += run.collectDeletedDocs.elapsed(MICROSECONDS);
                this.checkDeletedDocsElapsed += run.checkDeletedDocs.elapsed(MICROSECONDS);
                this.deleteDeletedDocsElapsed += run.deleteDeletedDocs.elapsed(MICROSECONDS);
                this.collectAndDeleteSplitDocsElapsed += run.collectAndDeleteSplitDocs.elapsed(MICROSECONDS);
                this.deleteSplitDocsElapsed += run.deleteSplitDocs.elapsed(MICROSECONDS);
                this.sortDocIdsElapsed += run.sortDocIds.elapsed(MICROSECONDS);
                this.updateResurrectedDocumentsElapsed += run.updateResurrectedDocuments.elapsed(MICROSECONDS);
                this.detailedGCDocsElapsed += run.detailedGCDocs.elapsed(MICROSECONDS);
                this.deleteDetailedGCDocsElapsed += run.deleteDetailedGCDocs.elapsed(MICROSECONDS);
                this.collectDeletedPropsElapsed += run.collectDeletedProps.elapsed(MICROSECONDS);
                this.collectDeletedOldRevsElapsed += run.collectDeletedOldRevs.elapsed(MICROSECONDS);
                this.collectUnmergedBCElapsed += run.collectUnmergedBC.elapsed(MICROSECONDS);
            }
        }
    }

    private enum GCPhase {
        NONE,
        COLLECTING,
        CHECKING,
        DELETING,
        SORTING,
        SPLITS_CLEANUP,
        DETAILED_GC,
        DETAILED_GC_COLLECT_PROPS,
        DETAILED_GC_COLLECT_OLD_REVS,
        DETAILED_GC_COLLECT_UNMERGED_BC,
        DETAILED_GC_CLEANUP,
        UPDATING
    }

    /**
     * Keeps track of timers when switching GC phases.
     * <p>
     * Could be merged with VersionGCStats, however this way the public class is kept unchanged.
     */
    private static class GCPhases {

        final VersionGCStats stats;
        final Stopwatch elapsed;
        private final GCMonitor monitor;
        private final List<GCPhase> phases = Lists.newArrayList();
        private final Map<GCPhase, Stopwatch> watches = Maps.newHashMap();
        private final AtomicBoolean canceled;

        GCPhases(AtomicBoolean canceled, VersionGCStats stats, GCMonitor monitor) {
            this.stats = stats;
            this.monitor = monitor;
            this.elapsed = Stopwatch.createStarted();
            this.watches.put(GCPhase.NONE, Stopwatch.createStarted());
            this.watches.put(GCPhase.COLLECTING, stats.collectDeletedDocs);
            this.watches.put(GCPhase.CHECKING, stats.checkDeletedDocs);
            this.watches.put(GCPhase.DELETING, stats.deleteDeletedDocs);
            this.watches.put(GCPhase.SORTING, stats.sortDocIds);
            this.watches.put(GCPhase.SPLITS_CLEANUP, stats.collectAndDeleteSplitDocs);
            this.watches.put(GCPhase.UPDATING, stats.updateResurrectedDocuments);
            this.watches.put(GCPhase.DETAILED_GC, stats.detailedGCDocs);
            this.watches.put(GCPhase.DETAILED_GC_COLLECT_PROPS, stats.collectDeletedProps);
            this.watches.put(GCPhase.DETAILED_GC_COLLECT_OLD_REVS, stats.collectDeletedOldRevs);
            this.watches.put(GCPhase.DETAILED_GC_COLLECT_UNMERGED_BC, stats.collectUnmergedBC);
            this.watches.put(GCPhase.DETAILED_GC_CLEANUP, stats.deleteDetailedGCDocs);
            this.canceled = canceled;
        }

        /**
         * Attempts to start a GC phase and tracks the time spent in this phase
         * until {@link #stop(GCPhase)} is called.
         *
         * @param started the GC phase.
         * @return {@code true} if the phase was started or {@code false} if the
         *          revision GC was canceled and the phase should not start.
         */
        public boolean start(GCPhase started) {
            if (canceled.get()) {
                return false;
            }
            suspend(currentWatch());
            this.phases.add(started);
            updateStatus();
            resume(currentWatch());
            return true;
        }

        public void stop(GCPhase phase) {
            if (!phases.isEmpty() && phase == phases.get(phases.size() - 1)) {
                suspend(currentWatch());
                phases.remove(phases.size() - 1);
                updateStatus();
                resume(currentWatch());
            }
        }

        public void close() {
            while (!phases.isEmpty()) {
                suspend(currentWatch());
                phases.remove(phases.size() - 1);
                updateStatus();
            }
            this.elapsed.stop();
        }

        private GCPhase current() {
            return phases.isEmpty() ? GCPhase.NONE : phases.get(phases.size() - 1);
        }

        private Stopwatch currentWatch() {
            return watches.get(current());
        }

        private void resume(Stopwatch w) {
            if (!w.isRunning()) {
                w.start();
            }
        }

        private void suspend(Stopwatch w) {
            if (w.isRunning()) {
                w.stop();
            }
        }

        private void updateStatus() {
            GCPhase p = current();
            if (p != GCPhase.NONE) {
                monitor.updateStatus(p.name());
            }
        }
    }

    private class GCJob {

        private final long maxRevisionAgeMillis;
        private final VersionGCOptions options;
        private final AtomicBoolean cancel = new AtomicBoolean();
        private final GCMonitor monitor;
        private final Supplier<String> status;

        GCJob(long maxRevisionAgeMillis,
              VersionGCOptions options,
              GCMonitor gcMonitor) {
            this.maxRevisionAgeMillis = maxRevisionAgeMillis;
            this.options = options;
            GCMessageTracker vgcm = new GCMessageTracker();
            this.status = vgcm;
            this.monitor = new DelegatingGCMonitor(Lists.newArrayList(vgcm, gcMonitor));
            this.monitor.updateStatus(STATUS_INITIALIZING);
        }

        VersionGCStats run() throws IOException {
            try {
                return gc(maxRevisionAgeMillis);
            } finally {
                monitor.updateStatus(STATUS_IDLE);
            }
        }

        void cancel() {
            monitor.info("Canceling revision garbage collection.");
            cancel.set(true);
        }

        String getStatus() {
            return status.get();
        }

        private VersionGCStats gc(long maxRevisionAgeInMillis) throws IOException {
            VersionGCStats stats = new VersionGCStats();
            stats.active.start();
            VersionGCRecommendations rec = new VersionGCRecommendations(maxRevisionAgeInMillis, nodeStore.getCheckpoints(),
                    nodeStore.getClock(), versionStore, options, gcMonitor, detailedGCEnabled);
            GCPhases phases = new GCPhases(cancel, stats, gcMonitor);
            try {
                if (rec.ignoreDueToCheckPoint) {
                    phases.stats.ignoredGCDueToCheckPoint = true;
                    monitor.skipped("Checkpoint prevented revision garbage collection");
                } else {
                    final RevisionVector headRevision = nodeStore.getHeadRevision();
                    final RevisionVector sweepRevisions = nodeStore.getSweepRevisions();
                    monitor.info("Looking at revisions in {}", rec.scope);

                    collectDeletedDocuments(phases, headRevision, rec);
                    collectSplitDocuments(phases, sweepRevisions, rec);
                }

                // now run detailed GC if enabled
                if (detailedGCEnabled) {
                    if (rec.ignoreDetailedGCDueToCheckPoint) {
                        phases.stats.ignoredDetailedGCDueToCheckPoint = true;
                        monitor.skipped("Checkpoint prevented detailed revision garbage collection");
                    } else {
                        final RevisionVector headRevision = nodeStore.getHeadRevision();
                        monitor.info("Looking at revisions in {} for detailed GC", rec.scopeDetailedGC);
                        collectDetailedGarbage(phases, headRevision, rec);
                    }
                }

                if ((detailedGCEnabled && rec.ignoreDetailedGCDueToCheckPoint) || rec.ignoreDueToCheckPoint) {
                    cancel.set(true);
                }

            } catch (LimitExceededException ex) {
                stats.limitExceeded = true;
            } finally {
                phases.close();
                stats.canceled = cancel.get();
            }

            rec.evaluate(stats);
            monitor.info("Revision garbage collection finished in {}. {}",
                    TimeDurationFormatter.forLogging().format(phases.elapsed.elapsed(MICROSECONDS), MICROSECONDS), stats);
            stats.active.stop();
            return stats;
        }

        /**
         * "Detailed garbage" refers to additional garbage identified as part of OAK-10199
         * et al: essentially garbage that in earlier versions of Oak were ignored. This
         * includes: deleted properties, revision information within documents, branch
         * commit related garbage.
         * <p/>
         * TODO: limit this to run only on a singleton instance, eg the cluster leader
         * <p/>
         * The "detailed garbage" collector can be instructed to do a full repository scan
         * - or incrementally based on where it last left off. When doing a full
         * repository scan (but not limited to that), it executes in (small) batches
         * followed by voluntary paused (aka throttling) to avoid excessive load on the
         * system. The full repository scan does not have to finish particularly fast,
         * it is okay that it takes a considerable amount of time.
         *
         * @param phases {@link GCPhases}
         * @param headRevision the current head revision of node store
         * @param rec {@link VersionGCRecommendations} to recommend GC operation
         */
        private void collectDetailedGarbage(final GCPhases phases, final RevisionVector headRevision, final VersionGCRecommendations rec)
                throws IOException {

            final long oldestModifiedMs = rec.scopeDetailedGC.fromMs;
            final long toModifiedMs = rec.scopeDetailedGC.toMs;
            final String oldestModifiedDocId = rec.detailedGCId;

            int docsTraversed = 0;
            boolean foundDoc = true;
            long oldModifiedMs = oldestModifiedMs;

            try (DetailedGC gc = new DetailedGC(headRevision, monitor, cancel)) {
                long fromModified = oldestModifiedMs;
                String fromId = ofNullable(oldestModifiedDocId).orElse(MIN_ID_VALUE);
                NodeDocument lastDoc;
                if (phases.start(GCPhase.DETAILED_GC)) {
                    while (foundDoc && fromModified < toModifiedMs && docsTraversed < PROGRESS_BATCH_SIZE) {
                        // set foundDoc to false to allow exiting the while loop
                        foundDoc = false;
                        lastDoc = null;
                        Iterable<NodeDocument> itr = versionStore.getModifiedDocs(fromModified, toModifiedMs, DETAILED_GC_BATCH_SIZE, fromId);
                        try {
                            for (NodeDocument doc : itr) {
                                foundDoc = true;
                                // continue with GC?
                                if (cancel.get()) {
                                    foundDoc = false; // to exit while loop as well
                                    log.info("Received GC cancel call. Terminating the GC Operation.");
                                    break;
                                }
                                docsTraversed++;
                                if (docsTraversed % 100 == 0) {
                                    monitor.info("Iterated through {} documents so far. {} had detailed garbage",
                                            docsTraversed, gc.getGarbageDocsCount());
                                }

                                lastDoc = doc;
                                // collect the data to delete in next step
                                if (phases.start(GCPhase.COLLECTING)) {
                                    gc.collectGarbage(doc, phases);
                                    phases.stop(GCPhase.COLLECTING);
                                }

                                final Long modified = lastDoc.getModified();
                                if (modified == null) {
                                    monitor.warn("collectDetailedGarbage : document has no _modified property : {}",
                                            doc.getId());
                                } else if (SECONDS.toMillis(modified) < fromModified) {
                                    monitor.warn(
                                            "collectDetailedGarbage : document has older _modified than query boundary : {} (from: {}, to: {})",
                                            modified, fromModified, toModifiedMs);
                                }
                            }
                            // now remove the garbage in one go, if any
                            if (gc.hasGarbage() && phases.start(GCPhase.DETAILED_GC_CLEANUP)) {
                                gc.removeGarbage(phases.stats);
                                phases.stop(GCPhase.DETAILED_GC_CLEANUP);
                            }
                            if (lastDoc != null) {
                                fromModified = lastDoc.getModified() == null ? oldModifiedMs : SECONDS.toMillis(lastDoc.getModified());
                                fromId = lastDoc.getId();
                            }
                        } finally {
                            Utils.closeIfCloseable(itr);
                            phases.stats.oldestModifiedDocTimeStamp = fromModified;
                            if (fromModified > oldModifiedMs) {
                                // we have moved ahead, now we can reset oldestModifiedId to min value
                                fromId = MIN_ID_VALUE;
                                phases.stats.oldestModifiedDocId = MIN_ID_VALUE;
                            } else {
                                // there are still documents pending at oldest Modified timestamp,
                                // save the last _id traversed to avoid re-fetching of ids
                                phases.stats.oldestModifiedDocId = fromId;
                            }
                            oldModifiedMs = fromModified;
                        }
                        // if we didn't find any document i.e. either we are already at last document
                        // of current timeStamp or there is no document for this timeStamp
                        // we need to reset fromId & increment fromModified and check again
                        if (!foundDoc && !Objects.equals(fromId, MIN_ID_VALUE)) {
                            fromId = MIN_ID_VALUE;
                            fromModified = fromModified + SECONDS.toMillis(5);
                            foundDoc = true; // to run while loop again
                        }
                    }
                    phases.stop(GCPhase.DETAILED_GC);
                }
            } finally {
                if (docsTraversed < PROGRESS_BATCH_SIZE) {
                    // we have traversed all the docs within given time range and nothing is left
                    // lets set oldModifiedDocTimeStamp to upper limit of this cycle
                    phases.stats.oldestModifiedDocTimeStamp = toModifiedMs;
                    phases.stats.oldestModifiedDocId = MIN_ID_VALUE;
                }
            }
        }



        private void collectSplitDocuments(GCPhases phases,
                                           RevisionVector sweepRevisions,
                                           VersionGCRecommendations rec) {
            if (phases.start(GCPhase.SPLITS_CLEANUP)) {
                int splitDocGCCount = phases.stats.splitDocGCCount;
                int intermediateSplitDocGCCount = phases.stats.intermediateSplitDocGCCount;
                versionStore.deleteSplitDocuments(GC_TYPES, sweepRevisions, rec.scope.toMs, phases.stats);
                gcStats.splitDocumentsDeleted(phases.stats.splitDocGCCount - splitDocGCCount);
                gcStats.intermediateSplitDocumentsDeleted(phases.stats.intermediateSplitDocGCCount - intermediateSplitDocGCCount);
                phases.stop(GCPhase.SPLITS_CLEANUP);
            }
        }

        private void collectDeletedDocuments(GCPhases phases,
                                             RevisionVector headRevision,
                                             VersionGCRecommendations rec)
                throws IOException, LimitExceededException {
            int docsTraversed = 0;
            try (DeletedDocsGC gc = new DeletedDocsGC(headRevision, cancel, options, monitor)) {
                if (phases.start(GCPhase.COLLECTING)) {
                    Iterable<NodeDocument> itr = versionStore.getPossiblyDeletedDocs(rec.scope.fromMs, rec.scope.toMs);
                    try {
                        for (NodeDocument doc : itr) {
                            // continue with GC?
                            if (cancel.get()) {
                                break;
                            }
                            // Check if node is actually deleted at current revision
                            // As node is not modified since oldestRevTimeStamp then
                            // this node has not be revived again in past maxRevisionAge
                            // So deleting it is safe
                            docsTraversed++;
                            if (docsTraversed % PROGRESS_BATCH_SIZE == 0) {
                                monitor.info("Iterated through {} documents so far. {} found to be deleted",
                                        docsTraversed, gc.getNumDocuments());
                            }
                            if (phases.start(GCPhase.CHECKING)) {
                                gc.possiblyDeleted(doc);
                                phases.stop(GCPhase.CHECKING);
                            }
                            if (rec.maxCollect > 0 && gc.docIdsToDelete.getSize() > rec.maxCollect) {
                                throw new LimitExceededException();
                            }
                            if (gc.hasLeafBatch()) {
                                if (phases.start(GCPhase.DELETING)) {
                                    gc.removeLeafDocuments(phases.stats);
                                    phases.stop(GCPhase.DELETING);
                                }
                            }
                            if (gc.hasRescurrectUpdateBatch()) {
                                if (phases.start(GCPhase.UPDATING)) {
                                    gc.updateResurrectedDocuments(phases.stats);
                                    phases.stop(GCPhase.UPDATING);
                                }
                            }
                        }
                    } finally {
                        Utils.closeIfCloseable(itr);
                    }
                    phases.stop(GCPhase.COLLECTING);
                }

                if (gc.getNumDocuments() != 0) {
                    if (phases.start(GCPhase.DELETING)) {
                        gc.removeLeafDocuments(phases.stats);
                        phases.stop(GCPhase.DELETING);
                    }

                    if (phases.start(GCPhase.SORTING)) {
                        gc.ensureSorted();
                        phases.stop(GCPhase.SORTING);
                    }

                    if (phases.start(GCPhase.DELETING)) {
                        gc.removeDocuments(phases.stats);
                        phases.stop(GCPhase.DELETING);
                    }
                }

                if (phases.start(GCPhase.UPDATING)) {
                    gc.updateResurrectedDocuments(phases.stats);
                    phases.stop(GCPhase.UPDATING);
                }
            }
        }
    }

    private class DetailedGC implements Closeable {

        private final RevisionVector headRevision;
        private final GCMonitor monitor;
        private final AtomicBoolean cancel;
        private final Stopwatch timer;
        private final List<UpdateOp> updateOpList;

        private final Map<String, Integer> deletedPropsCountMap;
        private int garbageDocsCount;
        private int totalGarbageDocsCount;

        public DetailedGC(@NotNull RevisionVector headRevision, @NotNull GCMonitor monitor, @NotNull AtomicBoolean cancel) {
            this.headRevision = requireNonNull(headRevision);
            this.monitor = monitor;
            this.cancel = cancel;
            this.updateOpList = new ArrayList<>();
            this.deletedPropsCountMap = new HashMap<>();
            this.timer = createUnstarted();
        }

        public void collectGarbage(final NodeDocument doc, final GCPhases phases) {

            monitor.info("Collecting Detailed Garbage for doc [{}]", doc.getId());

            final UpdateOp op = new UpdateOp(requireNonNull(doc.getId()), false);
            op.equals(MODIFIED_IN_SECS, doc.getModified());

            collectDeletedProperties(doc, phases, op);
            collectUnmergedBranchCommitDocument(doc, phases, op);
            collectOldRevisions(doc, phases, op);
            // only add if there are changes for this doc
            if (op.hasChanges()) {
                garbageDocsCount++;
                totalGarbageDocsCount++;
                monitor.info("Collected [{}] garbage for doc [{}]", op.getChanges().size(), doc.getId());
                updateOpList.add(op);
            }
        }

        private boolean hasGarbage() {
            return garbageDocsCount > 0;
        }

        private void collectUnmergedBranchCommitDocument(final NodeDocument doc, final GCPhases phases, final UpdateOp updateOp) {
            if (phases.start(GCPhase.DETAILED_GC_COLLECT_UNMERGED_BC)){
                // TODO add umerged BC collection logic
                phases.stop(GCPhase.DETAILED_GC_COLLECT_UNMERGED_BC);
            }

        }

        private void collectDeletedProperties(final NodeDocument doc, final GCPhases phases, final UpdateOp updateOp) {

            // get Map of all properties along with their values
            if (phases.start(GCPhase.DETAILED_GC_COLLECT_PROPS)) {
                final Set<String> properties = doc.getPropertyNames();

                // find all the properties which can be removed from document.
                // All the properties whose value is null in head revision are
                // eligible to be garbage collected.

                final Set<String> retainPropSet = ofNullable(doc.getNodeAtRevision(nodeStore, headRevision, null))
                        .map(DocumentNodeState::getAllBundledProperties)
                        .map(Map::keySet)
                        .map(p -> p.stream().map(Utils::escapePropertyName).collect(toSet()))
                        .orElse(emptySet());

                final int deletedPropsGCCount = properties.stream()
                        .filter(p -> !retainPropSet.contains(p))
                        .mapToInt(x -> {
                            updateOp.remove(x);
                            return 1;})
                        .sum();

                deletedPropsCountMap.put(doc.getId(), deletedPropsGCCount);

                if (log.isDebugEnabled()) {
                    log.debug("Collected {} deleted properties for document {}", deletedPropsGCCount, doc.getId());
                }
                phases.stop(GCPhase.DETAILED_GC_COLLECT_PROPS);
            }
        }

        private void collectOldRevisions(NodeDocument doc, GCPhases phases, UpdateOp updateOp) {

            if (phases.start(GCPhase.DETAILED_GC_COLLECT_OLD_REVS)){
                // TODO add old rev collection logic
                phases.stop(GCPhase.DETAILED_GC_COLLECT_OLD_REVS);
            }

        }

        int getGarbageDocsCount() {
            return totalGarbageDocsCount;
        }

        @Override
        public void close() {
            totalGarbageDocsCount = 0;
        }

        public void removeGarbage(final VersionGCStats stats) {

            if (updateOpList.isEmpty()) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipping removal of detailed garbage, cause no garbage detected");
                }
                return;
            }

            int updatedDocs;

            monitor.info("Proceeding to update [{}] documents", updateOpList.size());

            if (log.isDebugEnabled()) {
                String collect = updateOpList.stream().map(UpdateOp::getId).collect(joining(","));
                log.debug("Performing batch update of documents with following id's [{}]", collect);
            }

            if (cancel.get()) {
                log.info("Aborting the removal of detailed garbage since RGC had been cancelled");
                return;
            }

            timer.reset().start();
            try {
                List<NodeDocument> oldDocs = ds.findAndUpdate(NODES, updateOpList);
                int deletedProps = oldDocs.stream().filter(Objects::nonNull).mapToInt(d -> deletedPropsCountMap.getOrDefault(d.getId(), 0)).sum();
                updatedDocs = (int) oldDocs.stream().filter(Objects::nonNull).count();
                stats.updatedDetailedGCDocsCount += updatedDocs;
                stats.deletedPropsGCCount += deletedProps;
                log.info("Updated [{}] documents, deleted [{}] properties", updatedDocs, deletedProps);
                // now reset delete metadata
                updateOpList.clear();
                deletedPropsCountMap.clear();
                garbageDocsCount = 0;
            } finally {
                delayOnModifications(timer.stop().elapsed(MILLISECONDS), cancel);
            }
        }
    }
    private void delayOnModifications(final long durationMs, final AtomicBoolean cancel) {
        long delayMs = round(durationMs * options.delayFactor);
        if (!cancel.get() && delayMs > 0) {
            try {
                Clock clock = nodeStore.getClock();
                clock.waitUntil(clock.getTime() + delayMs);
            }
            catch (InterruptedException ex) {
                /* ignore */
            }
        }
    }

    /**
     * A helper class to remove document for deleted nodes.
     */
    private class DeletedDocsGC implements Closeable {

        private final RevisionVector headRevision;
        private final AtomicBoolean cancel;
        private final List<String> leafDocIdsToDelete = Lists.newArrayList();
        private final List<String> resurrectedIds = Lists.newArrayList();
        private final StringSort docIdsToDelete;
        private final StringSort prevDocIdsToDelete;
        private final Set<String> exclude = Sets.newHashSet();
        private boolean sorted = false;
        private final Stopwatch timer;
        private final VersionGCOptions options;
        private final GCMonitor monitor;

        public DeletedDocsGC(@NotNull RevisionVector headRevision,
                             @NotNull AtomicBoolean cancel,
                             @NotNull VersionGCOptions options,
                             @NotNull GCMonitor monitor) {
            this.headRevision = requireNonNull(headRevision);
            this.cancel = requireNonNull(cancel);
            this.timer = Stopwatch.createUnstarted();
            this.options = options;
            this.monitor = monitor;
            this.docIdsToDelete = newStringSort(options);
            this.prevDocIdsToDelete = newStringSort(options);
        }

        /**
         * @return the number of documents gathers so far that have been
         * identified as garbage via {@link #possiblyDeleted(NodeDocument)}.
         * This number does not include the previous documents.
         */
        long getNumDocuments() {
            return docIdsToDelete.getSize() + leafDocIdsToDelete.size();
        }

        /**
         * Informs the GC that the given document is possibly deleted. The
         * implementation will check if the node still exists at the head
         * revision passed to the constructor to this GC. The implementation
         * will keep track of documents representing deleted nodes and remove
         * them together with associated previous document
         *
         * @param doc the candidate document.
         * @return true iff document is scheduled for deletion
         */
        boolean possiblyDeleted(NodeDocument doc)
                throws IOException {
            gcStats.documentRead();
            // construct an id that also contains
            // the _modified time of the document
            String id = doc.getId() + "/" + doc.getModified();
            // check if id is valid
            try {
                Utils.getDepthFromId(id);
            } catch (IllegalArgumentException e) {
                monitor.warn("Invalid GC id {} for document {}", id, doc);
                return false;
            }
            if (doc.getNodeAtRevision(nodeStore, headRevision, null) == null) {
                // Collect id of all previous docs also
                Iterator<String> previousDocs = previousDocIdsFor(doc);
                if (!doc.hasChildren() && !previousDocs.hasNext()) {
                    addLeafDocument(id);
                } else {
                    addDocument(id);
                    addPreviousDocuments(previousDocs, doc.getId());
                }
                return true;
            } else {
                addNonDeletedDocument(id);
            }
            return false;
        }

        /**
         * Removes the documents that have been identified as garbage. This
         * also includes previous documents. This method will only remove
         * documents that have not been modified since they were passed to
         * {@link #possiblyDeleted(NodeDocument)}.
         *
         * @param stats to track the number of removed documents.
         */
        void removeDocuments(VersionGCStats stats) throws IOException {
            removeLeafDocuments(stats);
            stats.deletedDocGCCount += removeDeletedDocuments(
                    getDocIdsToDelete(), getDocIdsToDeleteSize(), false, "(other)");
            // FIXME: this is incorrect because that method also removes intermediate docs
            stats.splitDocGCCount += removeDeletedPreviousDocuments();
        }

        boolean hasLeafBatch() {
            return leafDocIdsToDelete.size() >= DELETE_BATCH_SIZE;
        }

        boolean hasRescurrectUpdateBatch() {
            return resurrectedIds.size() >= UPDATE_BATCH_SIZE;
        }

        void removeLeafDocuments(VersionGCStats stats) throws IOException {
            int removeCount = removeDeletedDocuments(
                    getLeafDocIdsToDelete(), getLeafDocIdsToDeleteSize(), true, "(leaf)");
            leafDocIdsToDelete.clear();
            stats.deletedLeafDocGCCount += removeCount;
            stats.deletedDocGCCount += removeCount;
        }

        void updateResurrectedDocuments(VersionGCStats stats) throws IOException {
            if (resurrectedIds.isEmpty()) {
                return;
            }
            int updateCount = resetDeletedOnce(resurrectedIds);
            resurrectedIds.clear();
            stats.updateResurrectedGCCount += updateCount;
        }

        public void close() {
            try {
                docIdsToDelete.close();
            } catch (IOException e) {
                monitor.warn("Failed to close docIdsToDelete: {}", e);
            }
            try {
                prevDocIdsToDelete.close();
            } catch (IOException e) {
                monitor.warn("Failed to close prevDocIdsToDelete: {}", e);
            }
        }

        //------------------------------< internal >----------------------------

        private Iterator<String> previousDocIdsFor(NodeDocument doc) {
            Map<Revision, Range> prevRanges = doc.getPreviousRanges(true);
            if (prevRanges.isEmpty()) {
                return Collections.emptyIterator();
            } else if (all(prevRanges.values(), FIRST_LEVEL)) {
                // all previous document ids can be constructed from the
                // previous ranges map. this works for first level previous
                // documents only.
                final Path path = doc.getPath();
                return Iterators.transform(prevRanges.entrySet().iterator(),
                        new Function<Map.Entry<Revision, Range>, String>() {
                    @Override
                    public String apply(Map.Entry<Revision, Range> input) {
                        int h = input.getValue().getHeight();
                        return Utils.getPreviousIdFor(path, input.getKey(), h);
                    }
                });
            } else {
                // need to fetch the previous documents to get their ids
                return Iterators.transform(doc.getAllPreviousDocs(),
                        new Function<NodeDocument, String>() {
                    @Override
                    public String apply(NodeDocument input) {
                        return input.getId();
                    }
                });
            }
        }

        private void addDocument(String id) throws IOException {
            docIdsToDelete.add(id);
        }

        private void addLeafDocument(String id) throws IOException {
            leafDocIdsToDelete.add(id);
        }

        private void addNonDeletedDocument(String id) throws IOException {
            resurrectedIds.add(id);
        }

        private long getNumPreviousDocuments() {
            return prevDocIdsToDelete.getSize() - exclude.size();
        }

        private void addPreviousDocuments(Iterator<String> ids,
                                          String mainDocId) throws IOException {
            while (ids.hasNext()) {
                String id = ids.next();
                if (id != null) {
                    prevDocIdsToDelete.add(id);
                } else {
                    log.debug("addPreviousDocuments: null id found via mainDocId={}", mainDocId);
                }
            }
        }

        private Iterator<String> getDocIdsToDelete() throws IOException {
            ensureSorted();
            return docIdsToDelete.getIds();
        }

        private long getDocIdsToDeleteSize() {
            return docIdsToDelete.getSize();
        }

        private Iterator<String> getLeafDocIdsToDelete() throws IOException {
            return leafDocIdsToDelete.iterator();
        }

        private long getLeafDocIdsToDeleteSize() {
            return leafDocIdsToDelete.size();
        }

        private void concurrentModification(NodeDocument doc) {
            Iterator<NodeDocument> it = doc.getAllPreviousDocs();
            while (it.hasNext()) {
                exclude.add(it.next().getId());
            }
        }

        private Iterator<String> getPrevDocIdsToDelete() throws IOException {
            ensureSorted();
            return Iterators.filter(prevDocIdsToDelete.getIds(),
                    new Predicate<String>() {
                @Override
                public boolean apply(String input) {
                    return !exclude.contains(input);
                }
            });
        }

        private int removeDeletedDocuments(Iterator<String> docIdsToDelete,
                                           long numDocuments,
                                           boolean leaves,
                                           String label) throws IOException {
            if (numDocuments == 0) {
                return 0;
            }
            monitor.info("Proceeding to delete [{}] documents [{}]", numDocuments, label);

            Iterator<List<String>> idListItr = partition(docIdsToDelete, DELETE_BATCH_SIZE);
            int deletedCount = 0;
            int lastLoggedCount = 0;
            int recreatedCount = 0;
            while (idListItr.hasNext() && !cancel.get()) {
                Map<String, Long> deletionBatch = Maps.newLinkedHashMap();
                for (String s : idListItr.next()) {
                    Map.Entry<String, Long> parsed;
                    try {
                        parsed = parseEntry(s);
                    } catch (IllegalArgumentException e) {
                        monitor.warn("Invalid _modified suffix for {}", s);
                        continue;
                    }
                    deletionBatch.put(parsed.getKey(), parsed.getValue());
                }

                if (log.isTraceEnabled()) {
                    StringBuilder sb = new StringBuilder("Performing batch deletion of documents with following ids. \n");
                    Joiner.on(LINE_SEPARATOR.value()).appendTo(sb, deletionBatch.keySet());
                    log.trace(sb.toString());
                }

                timer.reset().start();
                try {
                    int nRemoved = ds.remove(NODES, deletionBatch);

                    if (nRemoved < deletionBatch.size()) {
                        // some nodes were re-created while GC was running
                        // find the document that still exist
                        for (String id : deletionBatch.keySet()) {
                            NodeDocument d = ds.find(NODES, id);
                            if (d != null) {
                                concurrentModification(d);
                            }
                        }
                        recreatedCount += (deletionBatch.size() - nRemoved);
                    }

                    deletedCount += nRemoved;
                    log.debug("Deleted [{}] documents so far", deletedCount);
                    if (leaves) {
                        gcStats.leafDocumentsDeleted(deletedCount);
                    } else {
                        gcStats.documentsDeleted(deletedCount);
                    }

                    if (deletedCount + recreatedCount - lastLoggedCount >= PROGRESS_BATCH_SIZE) {
                        lastLoggedCount = deletedCount + recreatedCount;
                        double progress = lastLoggedCount * 1.0 / getNumDocuments() * 100;
                        String msg = String.format("Deleted %d (%1.2f%%) documents so far", deletedCount, progress);
                        monitor.info(msg);
                    }
                } finally {
                    delayOnModifications(timer.stop().elapsed(TimeUnit.MILLISECONDS), cancel);
                }
            }
            return deletedCount;
        }

        private int resetDeletedOnce(List<String> resurrectedDocuments) throws IOException {
            monitor.info("Proceeding to reset [{}] _deletedOnce flags", resurrectedDocuments.size());

            int updateCount = 0;
            timer.reset().start();
            try {
                for (String s : resurrectedDocuments) {
                    if (!cancel.get()) {
                        try {
                            Map.Entry<String, Long> parsed = parseEntry(s);
                            UpdateOp up = new UpdateOp(parsed.getKey(), false);
                            up.equals(MODIFIED_IN_SECS, parsed.getValue());
                            up.remove(NodeDocument.DELETED_ONCE);
                            NodeDocument r = ds.findAndUpdate(Collection.NODES, up);
                            if (r != null) {
                                updateCount += 1;
                                gcStats.deletedOnceFlagReset();
                            }
                        } catch (IllegalArgumentException ex) {
                            monitor.warn("Invalid _modified suffix for {}", s);
                        } catch (DocumentStoreException ex) {
                            monitor.warn("updating {}: {}", s, ex.getMessage());
                        }
                    }
                }
            }
            finally {
                delayOnModifications(timer.stop().elapsed(TimeUnit.MILLISECONDS), cancel);
            }
            return updateCount;
        }

        private int removeDeletedPreviousDocuments() throws IOException {
            long num = getNumPreviousDocuments();
            if (num == 0) {
                return 0;
            }
            monitor.info("Proceeding to delete [{}] previous documents", num);

            int deletedCount = 0;
            int lastLoggedCount = 0;
            Iterator<List<String>> idListItr =
                    partition(getPrevDocIdsToDelete(), DELETE_BATCH_SIZE);
            while (idListItr.hasNext() && !cancel.get()) {
                List<String> deletionBatch = idListItr.next();
                deletedCount += deletionBatch.size();

                if (log.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder("Performing batch deletion of previous documents with following ids. \n");
                    Joiner.on(LINE_SEPARATOR.value()).appendTo(sb, deletionBatch);
                    log.debug(sb.toString());
                }

                ds.remove(NODES, deletionBatch);

                log.debug("Deleted [{}] previous documents so far", deletedCount);
                gcStats.splitDocumentsDeleted(deletedCount);

                if (deletedCount - lastLoggedCount >= PROGRESS_BATCH_SIZE) {
                    lastLoggedCount = deletedCount;
                    double progress = deletedCount * 1.0 / (prevDocIdsToDelete.getSize() - exclude.size()) * 100;
                    String msg = String.format("Deleted %d (%1.2f%%) previous documents so far", deletedCount, progress);
                    monitor.info(msg);
                }
            }
            return deletedCount;
        }

        private void ensureSorted() throws IOException {
            if (!sorted) {
                docIdsToDelete.sort();
                prevDocIdsToDelete.sort();
                sorted = true;
            }
        }

        /**
         * Parses an id/modified entry and returns the two components as a
         * Map.Entry.
         *
         * @param entry the id/modified String.
         * @return the parsed components.
         * @throws IllegalArgumentException if the entry is malformed.
         */
        private Map.Entry<String, Long> parseEntry(String entry) throws IllegalArgumentException {
            int idx = entry.lastIndexOf('/');
            if (idx == -1) {
                throw new IllegalArgumentException(entry);
            }
            String id = entry.substring(0, idx);
            long modified;
            try {
                modified = Long.parseLong(entry.substring(idx + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(entry);
            }
            return Maps.immutableEntry(id, modified);
        }
    }

    @NotNull
    private StringSort newStringSort(VersionGCOptions options) {
        return new StringSort(options.overflowToDiskThreshold, NodeDocumentIdComparator.INSTANCE);
    }

    private static final Predicate<Range> FIRST_LEVEL = new Predicate<Range>() {
        @Override
        public boolean apply(@Nullable Range input) {
            return input != null && input.height == 0;
        }
    };

    /**
     * GCMessageTracker is a partial implementation of GCMonitor. We use it to
     * keep track of the last message issued by the GC job.
     */
    private static class GCMessageTracker
            extends GCMonitor.Empty
            implements Supplier<String> {

        private volatile String lastMessage = STATUS_INITIALIZING;

        @Override
        public void info(String message, Object... arguments) {
            lastMessage = arrayFormat(message, arguments).getMessage();
        }

        @Override
        public void warn(String message, Object... arguments) {
            lastMessage = arrayFormat(message, arguments).getMessage();
        }

        @Override
        public void error(String message, Exception e) {
            lastMessage = message + " (" + e.getMessage() + ")";
        }

        @Override
        public String get() {
            return lastMessage;
        }
    }

    private static final class LimitExceededException extends Exception {
        private static final long serialVersionUID = 6578586397629516408L;
    }
}
