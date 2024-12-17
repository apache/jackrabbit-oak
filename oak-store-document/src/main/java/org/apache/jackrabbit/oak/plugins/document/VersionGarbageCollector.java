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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

import org.apache.jackrabbit.guava.common.base.Stopwatch;

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Maps;

import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type;
import org.apache.jackrabbit.oak.plugins.document.util.TimeInterval;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.gc.DelegatingGCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.TimeDurationFormatter;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import static java.lang.Math.round;
import static java.lang.String.join;
import static java.util.Collections.emptySet;
import static java.util.List.of;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import static org.apache.jackrabbit.guava.common.collect.Iterables.all;
import static org.apache.jackrabbit.guava.common.collect.Iterators.partition;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.apache.jackrabbit.oak.plugins.document.Document.ID;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FGC_BATCH_SIZE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FGC_PROGRESS_SIZE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FULL_GC_MODE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.BRANCH_COMMITS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COLLISIONS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COMMIT_ROOT;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MIN_ID_VALUE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.REVISIONS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.COMMIT_ROOT_ONLY;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_LEAF;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_NO_BRANCH;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.FullGCMode.EMPTYPROPS;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.FullGCMode.GAP_ORPHANS;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.FullGCMode.GAP_ORPHANS_EMPTYPROPS;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.FullGCMode.NONE;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.timestampToString;
import static org.apache.jackrabbit.oak.stats.StatisticsProvider.NOOP;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isCommitted;
import static org.slf4j.LoggerFactory.getLogger;
import static org.slf4j.helpers.MessageFormatter.arrayFormat;

public class VersionGarbageCollector {

    //Kept less than MongoDocumentStore.IN_CLAUSE_BATCH_SIZE to avoid re-partitioning
    private static final int DELETE_BATCH_SIZE = 450;
    private static final int UPDATE_BATCH_SIZE = 450;
    private static final int PROGRESS_BATCH_SIZE = 10000;
    private static final int FULL_GC_MISSING_DOCS_TYPE_CACHE_SIZE = 64;
    private static final String STATUS_IDLE = "IDLE";
    private static final String STATUS_INITIALIZING = "INITIALIZING";
    private static final Logger log = getLogger(VersionGarbageCollector.class);
    private static final Logger AUDIT_LOG = getLogger(VersionGarbageCollector.class.getName() + ".auditFGC");

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
     * Property name to timestamp till when last full-GC run happened
     */
    static final String SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP = "fullGCTimeStamp";

    /**
     * Property name to _id till when last full-GC run happened
     */
    static final String SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP = "fullGCId";

    /**
     * Property name to timestamp till when last full-GC run happened in dryRun mode only
     */
    static final String SETTINGS_COLLECTION_FULL_GC_DRY_RUN_TIMESTAMP_PROP = "fullGCDryRunTimeStamp";

    /**
     * Property name to _id till when last full-GC run happened in dryRun mode only
     */
    static final String SETTINGS_COLLECTION_FULL_GC_DRY_RUN_DOCUMENT_ID_PROP = "fullGCDryRunId";

    /**
     * During hardening of FullGC one can choose level type of garbage should be cleaned up.
     * Ultimately the goal is to clean up all possible garbage. After hardening these modes
     * might no longer be supported.
     */
    enum FullGCMode {
        /** no full GC is done at all */
        NONE,
        /** GC only empty properties */
        EMPTYPROPS,
        /** GC only orphaned nodes with gaps in ancestor docs */
        GAP_ORPHANS,
        /** GC orphaned nodes with gaps in ancestor docs, plus empty properties */
        GAP_ORPHANS_EMPTYPROPS,
        /** GC any kind of orphaned nodes, plus empty properties */
        ALL_ORPHANS_EMPTYPROPS,
        /**
         * GC any kind of orphaned nodes, empty properties plus keep 1 (== keep
         * traversed) revision, applied to user properties only
         */
        ORPHANS_EMPTYPROPS_KEEP_ONE_USER_PROPS,
        /**
         * GC any kind of orphaned nodes, empty properties plus keep 1 (== keep
         * traversed) revision, applied to all properties
         */
        ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS,
        /**
         * GC any kind of orphaned nodes, empty properties plus cleanup unmerged BCs
         */
        ORPHANS_EMPTYPROPS_UNMERGED_BC,
        /**
         * GC any kind of orphaned nodes, empty properties plus cleanup revisions, also
         * between checkpoints
         */
        ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_NO_UNMERGED_BC,
        /**
         * GC any kind of orphaned nodes, empty properties, cleanup revisions, also
         * between checkpoints, plus cleanup unmerged BCs
         */
        ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC
    }

    private static FullGCMode fullGcMode = GAP_ORPHANS_EMPTYPROPS;

    static FullGCMode getFullGcMode() {
        return fullGcMode;
    }

    /**
     * Set the full GC mode to be used according to the provided configuration value.
     * The configuration value will be ignored and fullGCMode will be reset to NONE
     * if it is set to any other values than the supported ones.
     * @param fullGcMode configuration value for full GC mode
     */
    static void setFullGcMode(int fullGcMode) {
        switch (fullGcMode) {
            case 0:
                VersionGarbageCollector.fullGcMode = NONE;
                break;
            case 1:
                VersionGarbageCollector.fullGcMode = EMPTYPROPS;
                break;
            case 2:
                VersionGarbageCollector.fullGcMode = GAP_ORPHANS;
                break;
            case 3:
                VersionGarbageCollector.fullGcMode = GAP_ORPHANS_EMPTYPROPS;
                break;
            default:
                log.warn("Unsupported full GC mode configuration value: {}. Resetting to NONE", fullGcMode);
                VersionGarbageCollector.fullGcMode = NONE;
        }
    }

    private final DocumentNodeStore nodeStore;
    private final DocumentStore ds;
    private final boolean fullGCEnabled;
    private final boolean isFullGCDryRun;
    private final boolean embeddedVerification;
    private final double fullGCDelayFactor;
    private final int fullGCBatchSize;
    private final int fullGCProgressSize;
    private Set<String> fullGCIncludePaths = Collections.emptySet();
    private Set<String> fullGCExcludePaths = Collections.emptySet();
    private final VersionGCSupport versionStore;
    private final AtomicReference<GCJob> collector = new AtomicReference<>();
    private VersionGCOptions options;
    private GCMonitor gcMonitor = GCMonitor.EMPTY;
    private RevisionGCStats gcStats = new RevisionGCStats(NOOP);
    private FullGCStatsCollector fullGCStats = new FullGCStatsCollectorImpl(NOOP);

    VersionGarbageCollector(DocumentNodeStore nodeStore,
                            VersionGCSupport gcSupport,
                            final boolean fullGCEnabled,
                            final boolean isFullGCDryRun,
                            final boolean embeddedVerification) {
        this(nodeStore, gcSupport, fullGCEnabled, isFullGCDryRun, embeddedVerification, DEFAULT_FULL_GC_MODE,
                0, DEFAULT_FGC_BATCH_SIZE, DEFAULT_FGC_PROGRESS_SIZE);
    }

    VersionGarbageCollector(DocumentNodeStore nodeStore,
                            VersionGCSupport gcSupport,
                            final boolean fullGCEnabled,
                            final boolean isFullGCDryRun,
                            final boolean embeddedVerification,
                            final int fullGCMode,
                            final double fullGCDelayFactor,
                            final int fullGCBatchSize,
                            final int fullGCProgressSize) {
        this.nodeStore = nodeStore;
        this.versionStore = gcSupport;
        this.ds = gcSupport.getDocumentStore();
        this.fullGCEnabled = fullGCEnabled;
        this.isFullGCDryRun = isFullGCDryRun;
        this.embeddedVerification = embeddedVerification;
        this.fullGCDelayFactor = fullGCDelayFactor;
        this.fullGCBatchSize = Math.min(fullGCBatchSize, fullGCProgressSize);
        this.fullGCProgressSize = fullGCProgressSize;
        this.options = new VersionGCOptions();

        setFullGcMode(fullGCMode);
        AUDIT_LOG.info("<init> VersionGarbageCollector created with fullGcMode: {}, batchSize: {}, progressSize: {}, delayFactor: {}",
                fullGcMode, fullGCBatchSize, fullGCProgressSize, fullGCDelayFactor);
    }

    /**
     * Please note that at the moment the includes do not
     * take long paths into account. That is, if a long path was
     * supposed to be included via an include, it is not.
     * Reason for this is that long paths would require
     * the mongo query to include a '_path' condition - which disallows
     * mongo from using the '_modified_id' index. IOW long paths
     * would result in full scans - which results in bad performance.
     */
    void setFullGCPaths(@NotNull Set<String> includes, @NotNull Set<String> excludes) {
        this.fullGCIncludePaths = requireNonNull(includes);
        this.fullGCExcludePaths = requireNonNull(excludes);
        AUDIT_LOG.info("Full GC paths set to include: {} and exclude: {} in mode {}", includes, excludes, fullGcMode);
    }

    public void setStatisticsProvider(StatisticsProvider provider) {
        this.gcStats = new RevisionGCStats(provider);
        this.fullGCStats = new FullGCStatsCollectorImpl(provider);
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
            if (fullGCEnabled) {
                fullGCStats.started();
            }
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
                if (fullGCEnabled) {
                    fullGCStats.finished(overall);
                }
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

    public void resetFullGC() {
        UpdateOp op = new UpdateOp(SETTINGS_COLLECTION_ID, false);
        op.remove(SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP);
        op.remove(SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP);
        ds.findAndUpdate(SETTINGS, op);
    }

    public void resetDryRun() {
        UpdateOp op = new UpdateOp(SETTINGS_COLLECTION_ID, false);
        op.remove(SETTINGS_COLLECTION_FULL_GC_DRY_RUN_TIMESTAMP_PROP);
        op.remove(SETTINGS_COLLECTION_FULL_GC_DRY_RUN_DOCUMENT_ID_PROP);
        ds.findAndUpdate(SETTINGS, op);
    }

    public VersionGCInfo getInfo(long maxRevisionAge, TimeUnit unit)
            throws IOException {
        long maxRevisionAgeInMillis = unit.toMillis(maxRevisionAge);
        long now = nodeStore.getClock().getTime();
        VersionGCRecommendations rec = new VersionGCRecommendations(maxRevisionAgeInMillis, nodeStore.getCheckpoints(),
                !nodeStore.isReadOnlyMode(), nodeStore.getClock(), versionStore, options, gcMonitor, fullGCEnabled,
                isFullGCDryRun);
        int estimatedIterations = -1;
        if (rec.suggestedIntervalMs > 0) {
            estimatedIterations = (int)Math.ceil((double) (now - rec.scope.toMs) / rec.suggestedIntervalMs);
        }
        return new VersionGCInfo(rec.lastOldestTimestamp, rec.scope.fromMs,
                rec.deleteCandidateCount, rec.maxCollect,
                rec.suggestedIntervalMs, rec.scope.toMs, estimatedIterations,
                rec.scopeFullGC.fromMs);
    }

    public static class VersionGCInfo {
        public final long lastSuccess;
        public final long oldestRevisionEstimate;
        public final long revisionsCandidateCount;
        public final long collectLimit;
        public final long recommendedCleanupInterval;
        public final long recommendedCleanupTimestamp;
        public final int estimatedIterations;
        public final long oldestFullGCRevisionEstimate;

        VersionGCInfo(long lastSuccess,
                      long oldestRevisionEstimate,
                      long revisionsCandidateCount,
                      long collectLimit,
                      long recommendedCleanupInterval,
                      long recommendedCleanupTimestamp,
                      int estimatedIterations,
                      long oldestFullGCRevisionEstimate) {
            this.lastSuccess = lastSuccess;
            this.oldestRevisionEstimate = oldestRevisionEstimate;
            this.revisionsCandidateCount = revisionsCandidateCount;
            this.collectLimit = collectLimit;
            this.recommendedCleanupInterval = recommendedCleanupInterval;
            this.recommendedCleanupTimestamp = recommendedCleanupTimestamp;
            this.estimatedIterations = estimatedIterations;
            this.oldestFullGCRevisionEstimate = oldestFullGCRevisionEstimate;
        }
    }

    public static class VersionGCStats {
        boolean ignoredGCDueToCheckPoint;
        boolean fullGCDryRunMode;
        boolean ignoredFullGCDueToCheckPoint;
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
        int updatedFullGCDocsCount;
        int skippedFullGCDocsCount;
        int deletedPropsCount;
        int deletedInternalPropsCount;
        int deletedPropRevsCount;
        int deletedInternalPropRevsCount;
        int deletedUnmergedBCCount;
        int deletedOrphanNodesCount;
        final TimeDurationFormatter df = TimeDurationFormatter.forLogging();
        final Stopwatch active = Stopwatch.createUnstarted();
        final Stopwatch fullGCActive = Stopwatch.createUnstarted();
        final Stopwatch collectDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch checkDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch deleteDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch collectAndDeleteSplitDocs = Stopwatch.createUnstarted();
        final Stopwatch deleteSplitDocs = Stopwatch.createUnstarted();
        final Stopwatch sortDocIds = Stopwatch.createUnstarted();
        final Stopwatch updateResurrectedDocuments = Stopwatch.createUnstarted();
        final Stopwatch fullGCDocs = Stopwatch.createUnstarted();
        final Stopwatch deleteFullGCDocs = Stopwatch.createUnstarted();
        final Stopwatch collectFullGC = Stopwatch.createUnstarted();
        final Stopwatch collectOrphanNodes = Stopwatch.createUnstarted();
        final Stopwatch collectDeletedProps = Stopwatch.createUnstarted();
        final Stopwatch collectDeletedOldRevs = Stopwatch.createUnstarted();
        final Stopwatch collectUnmergedBC = Stopwatch.createUnstarted();
        long activeElapsed, fullGCActiveElapsed, collectDeletedDocsElapsed, checkDeletedDocsElapsed, deleteDeletedDocsElapsed,
                collectAndDeleteSplitDocsElapsed, deleteSplitDocsElapsed, sortDocIdsElapsed, updateResurrectedDocumentsElapsed,
                fullGCDocsElapsed, collectFullGCElapsed, collectOrphanNodesElapsed, collectDeletedPropsElapsed,
                deleteFullGCDocsElapsed, collectDeletedOldRevsElapsed, collectUnmergedBCElapsed;

        @Override
        public String toString() {
            String timings;
            String fmt = "timeToCollectDeletedDocs=%s, timeToCheckDeletedDocs=%s, timeToSortDocIds=%s, timeTakenToUpdateResurrectedDocs=%s, timeTakenToDeleteDeletedDocs=%s, timeTakenToCollectAndDeleteSplitDocs=%s%s, " +
                    "timeToRunFullGC=%s, which includes [timeToDeleteFullGC=%s and timeToCollectFullGC=%s, (of which timeToCollectOrphanNodes=%s timeToCollectDeletedProps=%s, timeToCollectOldRevs=%s, timeToCollectUnmergedBranchCommits=%s)]";

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
                        timeDeletingSplitDocs,
                        df.format(fullGCDocsElapsed, MICROSECONDS),
                        df.format(deleteFullGCDocsElapsed, MICROSECONDS),
                        df.format(collectFullGCElapsed, MICROSECONDS),
                        df.format(collectOrphanNodesElapsed, MICROSECONDS),
                        df.format(collectDeletedPropsElapsed, MICROSECONDS),
                        df.format(collectDeletedOldRevsElapsed, MICROSECONDS),
                        df.format(collectUnmergedBCElapsed, MICROSECONDS));
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
                        timeDeletingSplitDocs,
                        df.format(fullGCDocs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(deleteFullGCDocs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(collectFullGC.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(collectOrphanNodes.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(collectDeletedProps.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(collectDeletedOldRevs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(collectUnmergedBC.elapsed(MICROSECONDS), MICROSECONDS));
            }

            return "VersionGCStats{" +
                    "ignoredGCDueToCheckPoint=" + ignoredGCDueToCheckPoint +
                    ", fullGCDryRunMode=" + fullGCDryRunMode +
                    ", ignoredFullGCDueToCheckPoint=" + ignoredFullGCDueToCheckPoint +
                    ", canceled=" + canceled +
                    ", deletedDocGCCount=" + deletedDocGCCount + " (of which leaf: " + deletedLeafDocGCCount + ")" +
                    ", updateResurrectedGCCount=" + updateResurrectedGCCount +
                    ", splitDocGCCount=" + splitDocGCCount +
                    ", intermediateSplitDocGCCount=" + intermediateSplitDocGCCount +
                    ", oldestModifiedDocId=" + oldestModifiedDocId +
                    ", oldestModifiedDocTimeStamp=" + timestampToString(oldestModifiedDocTimeStamp) +
                    ", updatedFullGCDocsCount=" + updatedFullGCDocsCount +
                    ", skippedFullGCDocsCount=" + skippedFullGCDocsCount +
                    ", deletedPropsCount=" + deletedPropsCount +
                    ", deletedInternalPropsCount=" + deletedInternalPropsCount +
                    ", deletedPropRevsCount=" + deletedPropRevsCount +
                    ", deletedInternalPropRevsCount=" + deletedInternalPropRevsCount +
                    ", deletedUnmergedBCCount=" + deletedUnmergedBCCount +
                    ", deletedOrphanNodesCount=" + deletedOrphanNodesCount +
                    ", iterationCount=" + iterationCount +
                    ", timeFullGCActive=" + df.format(fullGCActiveElapsed, MICROSECONDS) +
                    ", timeActive=" + df.format(activeElapsed, MICROSECONDS) +
                    ", " + timings + "}";
        }

        void addRun(VersionGCStats run) {
            ++iterationCount;
            this.ignoredGCDueToCheckPoint = run.ignoredGCDueToCheckPoint;
            this.fullGCDryRunMode = run.fullGCDryRunMode;
            this.ignoredFullGCDueToCheckPoint = run.ignoredFullGCDueToCheckPoint;
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
            this.updatedFullGCDocsCount += run.updatedFullGCDocsCount;
            this.skippedFullGCDocsCount += run.skippedFullGCDocsCount;
            this.deletedPropsCount += run.deletedPropsCount;
            this.deletedInternalPropsCount += run.deletedInternalPropsCount;
            this.deletedPropRevsCount += run.deletedPropRevsCount;
            this.deletedInternalPropRevsCount += run.deletedInternalPropRevsCount;
            this.deletedUnmergedBCCount += run.deletedUnmergedBCCount;
            this.deletedOrphanNodesCount += run.deletedOrphanNodesCount;
            if (run.iterationCount > 0) {
                // run is cumulative with times in elapsed fields
                this.activeElapsed += run.activeElapsed;
                this.fullGCActiveElapsed += run.fullGCActiveElapsed;
                this.collectDeletedDocsElapsed += run.collectDeletedDocsElapsed;
                this.checkDeletedDocsElapsed += run.checkDeletedDocsElapsed;
                this.deleteDeletedDocsElapsed += run.deleteDeletedDocsElapsed;
                this.collectAndDeleteSplitDocsElapsed += run.collectAndDeleteSplitDocsElapsed;
                this.deleteSplitDocsElapsed += run.deleteSplitDocsElapsed;
                this.sortDocIdsElapsed += run.sortDocIdsElapsed;
                this.updateResurrectedDocumentsElapsed += run.updateResurrectedDocumentsElapsed;
                this.fullGCDocsElapsed += run.fullGCDocsElapsed;
                this.deleteFullGCDocsElapsed += run.deleteFullGCDocsElapsed;
                this.collectFullGCElapsed += run.collectFullGCElapsed;
                this.collectOrphanNodesElapsed += run.collectOrphanNodesElapsed;
                this.collectDeletedPropsElapsed += run.collectDeletedPropsElapsed;
                this.collectDeletedOldRevsElapsed += run.collectDeletedOldRevsElapsed;
                this.collectUnmergedBCElapsed += run.collectUnmergedBCElapsed;
            } else {
                // single run -> read from stop watches
                this.activeElapsed += run.active.elapsed(MICROSECONDS);
                this.fullGCActiveElapsed += run.fullGCActive.elapsed(MICROSECONDS);
                this.collectDeletedDocsElapsed += run.collectDeletedDocs.elapsed(MICROSECONDS);
                this.checkDeletedDocsElapsed += run.checkDeletedDocs.elapsed(MICROSECONDS);
                this.deleteDeletedDocsElapsed += run.deleteDeletedDocs.elapsed(MICROSECONDS);
                this.collectAndDeleteSplitDocsElapsed += run.collectAndDeleteSplitDocs.elapsed(MICROSECONDS);
                this.deleteSplitDocsElapsed += run.deleteSplitDocs.elapsed(MICROSECONDS);
                this.sortDocIdsElapsed += run.sortDocIds.elapsed(MICROSECONDS);
                this.updateResurrectedDocumentsElapsed += run.updateResurrectedDocuments.elapsed(MICROSECONDS);
                this.fullGCDocsElapsed += run.fullGCDocs.elapsed(MICROSECONDS);
                this.deleteFullGCDocsElapsed += run.deleteFullGCDocs.elapsed(MICROSECONDS);
                this.collectFullGCElapsed += run.collectFullGC.elapsed(MICROSECONDS);
                this.collectOrphanNodesElapsed += run.collectOrphanNodes.elapsed(MICROSECONDS);
                this.collectDeletedPropsElapsed += run.collectDeletedProps.elapsed(MICROSECONDS);
                this.collectDeletedOldRevsElapsed += run.collectDeletedOldRevs.elapsed(MICROSECONDS);
                this.collectUnmergedBCElapsed += run.collectUnmergedBC.elapsed(MICROSECONDS);
            }
        }
    }

    enum GCPhase {
        NONE,
        COLLECTING,
        CHECKING,
        DELETING,
        SORTING,
        SPLITS_CLEANUP,
        FULL_GC,
        FULL_GC_COLLECT_GARBAGE,
        FULL_GC_COLLECT_ORPHAN_NODES,
        FULL_GC_COLLECT_PROPS,
        FULL_GC_COLLECT_OLD_REVS,
        FULL_GC_COLLECT_UNMERGED_BC,
        FULL_GC_CLEANUP,
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
        private final List<GCPhase> phases = new ArrayList<>();
        private final Map<GCPhase, Stopwatch> watches = new HashMap<>();
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
            this.watches.put(GCPhase.FULL_GC, stats.fullGCDocs);
            this.watches.put(GCPhase.FULL_GC_COLLECT_GARBAGE, stats.collectFullGC);
            this.watches.put(GCPhase.FULL_GC_COLLECT_ORPHAN_NODES, stats.collectOrphanNodes);
            this.watches.put(GCPhase.FULL_GC_COLLECT_PROPS, stats.collectDeletedProps);
            this.watches.put(GCPhase.FULL_GC_COLLECT_OLD_REVS, stats.collectDeletedOldRevs);
            this.watches.put(GCPhase.FULL_GC_COLLECT_UNMERGED_BC, stats.collectUnmergedBC);
            this.watches.put(GCPhase.FULL_GC_CLEANUP, stats.deleteFullGCDocs);
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
            this.monitor = new DelegatingGCMonitor(List.of(vgcm, gcMonitor));
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
                    !nodeStore.isReadOnlyMode(), nodeStore.getClock(), versionStore, options, gcMonitor, fullGCEnabled,
                    isFullGCDryRun);
            GCPhases phases = new GCPhases(cancel, stats, gcMonitor);
            try {
                if (!isFullGCDryRun) {
                    // only run if not fullGC dryRun mode
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
                } else {
                    phases.stats.fullGCDryRunMode = true;
                }

                // now run full GC if enabled
                if (fullGCEnabled) {
                    stats.fullGCActive.start();
                    if (rec.ignoreFullGCDueToCheckPoint) {
                        phases.stats.ignoredFullGCDueToCheckPoint = true;
                        monitor.skipped("Checkpoint prevented Full garbage collection");
                    } else {
                        final RevisionVector headRevision = nodeStore.getHeadRevision();
                        monitor.info("Looking at revisions in {} for full GC", rec.scopeFullGC);
                        collectFullGC(phases, headRevision, rec);
                    }
                }

                if ((!fullGCEnabled || rec.ignoreFullGCDueToCheckPoint) && rec.ignoreDueToCheckPoint) {
                    // cancel if both are stopped by checkpoint
                    // otherwise we need to continue
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
            if (fullGCEnabled && stats.fullGCActive.isRunning()) {
                // probably limitExceeded == true case
                stats.fullGCActive.stop();
            }
            stats.active.stop();
            return stats;
        }

        /**
         * "Full garbage" refers to additional garbage identified as part of OAK-10199
         * et al: essentially garbage that in earlier versions of Oak were ignored. This
         * includes: deleted properties, revision information within documents, branch
         * commit related garbage.
         * <p>
         * The "Full garbage" collector can be instructed to do a full repository scan
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
        private void collectFullGC(final GCPhases phases, final RevisionVector headRevision, final VersionGCRecommendations rec) {

            final long oldestModifiedMs = rec.scopeFullGC.fromMs;
            final long toModifiedMs = rec.scopeFullGC.toMs;
            final String oldestModifiedDocId = rec.fullGCId;
            final Stopwatch timer = Stopwatch.createUnstarted();

            int docsTraversed = 0;
            boolean foundDoc = true;
            long oldModifiedMs = oldestModifiedMs;
            final LinkedHashMap<Path, Boolean> missingDocsTypes = new LinkedHashMap<>() {
                @Override
                protected boolean removeEldestEntry(Entry<Path, Boolean> eldest) {
                    return size() > FULL_GC_MISSING_DOCS_TYPE_CACHE_SIZE;
                }
            };

            try (FullGC gc = new FullGC(headRevision, toModifiedMs, missingDocsTypes, monitor, cancel)) {
                long fromModifiedMs = oldestModifiedMs;
                String fromId = ofNullable(oldestModifiedDocId).orElse(MIN_ID_VALUE);
                NodeDocument lastDoc;
                if (phases.start(GCPhase.FULL_GC)) {
                    while (foundDoc && fromModifiedMs < toModifiedMs && docsTraversed < fullGCProgressSize) {
                        // set foundDoc to false to allow exiting the while loop
                        foundDoc = false;
                        lastDoc = null;
                        if (log.isDebugEnabled()) {
                            log.debug("Fetching docs from [{}] to [{}] with Id starting from [{}]", timestampToString(fromModifiedMs), timestampToString(toModifiedMs), fromId);
                        }
                        // start timer to record time taken by each batch
                        timer.reset().start();
                        Iterable<NodeDocument> itr = versionStore.getModifiedDocs(fromModifiedMs, toModifiedMs, fullGCBatchSize, fromId, fullGCIncludePaths, fullGCExcludePaths);
                        try {
                            for (NodeDocument doc : itr) {
                                foundDoc = true;
                                // continue with GC?
                                if (cancel.get()) {
                                    foundDoc = false; // to exit while loop as well
                                    AUDIT_LOG.info("<Terminating> Received GC cancel call");
                                    break;
                                }
                                docsTraversed++;
                                if (docsTraversed % 100 == 0) {
                                    monitor.info("Iterated through {} documents so far. {} had Full garbage", docsTraversed, gc.getGarbageCount());
                                }

                                lastDoc = doc;
                                // collect the data to delete in next step
                                if (phases.start(GCPhase.FULL_GC_COLLECT_GARBAGE)) {
                                    if (Utils.isIncluded(doc.getPath(), Collections.emptySet(), fullGCExcludePaths)) {
                                        gc.collectGarbage(doc, phases);
                                    } else {
                                        // MongoVersionGCSupport doesn't take long paths into consideration
                                        // for neither includes nor excludes. If isIncluded returns false here,
                                        // that can only be due to an excluded long path.
                                        // in which case, we can actually honor that and skip this
                                        if (AUDIT_LOG.isDebugEnabled()){
                                            AUDIT_LOG.debug("<Skipping> document with excluded path: {}", doc.getPath());
                                        }
                                    }
                                    phases.stop(GCPhase.FULL_GC_COLLECT_GARBAGE);
                                }

                                final Long modified = lastDoc.getModified();
                                if (modified == null) {
                                    monitor.warn("collectFullGC : document has no _modified property : {}", doc.getId());
                                } else if (modified < MILLISECONDS.toSeconds(fromModifiedMs)) {
                                    monitor.warn("collectFullGC : document has older _modified than query boundary : {} (from: {} [{}], to: {} [{}])",
                                            modified, fromModifiedMs, timestampToString(fromModifiedMs), toModifiedMs, timestampToString(toModifiedMs));
                                }
                            }
                            // now remove the garbage in one go, if any
                            if (gc.hasGarbage() && phases.start(GCPhase.FULL_GC_CLEANUP)) {
                                gc.removeGarbage(phases.stats);
                                phases.stop(GCPhase.FULL_GC_CLEANUP);
                            } else {
                                if (log.isDebugEnabled()) {
                                    log.debug("No garbage found to delete from [{}] to [{}] with Id starting from [{}]", timestampToString(fromModifiedMs), timestampToString(toModifiedMs), fromId);
                                }
                            }
                            if (lastDoc != null) {
                                fromModifiedMs = lastDoc.getModified() == null ? oldModifiedMs : SECONDS.toMillis(lastDoc.getModified());
                                fromId = lastDoc.getId();
                            }
                        } finally {
                            Utils.closeIfCloseable(itr);
                            phases.stats.oldestModifiedDocTimeStamp = fromModifiedMs;
                            phases.stats.oldestModifiedDocId = fromId;
                            oldModifiedMs = fromModifiedMs;
                            if (log.isDebugEnabled()) {
                                log.debug("Fetched docs till [{}] with Id [{}]", timestampToString(fromModifiedMs), fromId);
                            }
                            delayOnModifications(timer.stop().elapsed(MILLISECONDS), cancel, fullGCDelayFactor);
                        }
                        // if we didn't find any document i.e. either we are already at last document
                        // of current timeStamp or there is no document for this timeStamp
                        // we need to reset fromId & increment fromModifiedMs and check again
                        if (!foundDoc && !Objects.equals(fromId, MIN_ID_VALUE)) {
                            fromId = MIN_ID_VALUE;
                            fromModifiedMs = fromModifiedMs + SECONDS.toMillis(5);
                            foundDoc = true; // to run while loop again
                        }
                    }
                    phases.stop(GCPhase.FULL_GC);
                }
            } finally {
                if (docsTraversed < fullGCProgressSize) {
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
                            if (docsTraversed % fullGCProgressSize == 0) {
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

    private class FullGC implements Closeable {

        private final long toModifiedMs;
        private final GCMonitor monitor;
        private final AtomicBoolean cancel;
        private final List<UpdateOp> updateOpList;

        /** contains the list of _ids of orphan or deleted documents to be removed in the current batch **/
        private final Map<String, Long> orphanOrDeletedRemovalMap;

        /** contains the list of _ids of orphan or deleted documents to be removed, mapped to their path */
        private final Map<String, Path> orphanOrDeletedRemovalPathMap;

        /**
         * Map of documentId => total no. of deleted properties.
         * <p>
         *
         * The document can be updated or property might get recreated between collecting and deletion phases.
         * This would lead to document not getting deleted (since now modified date & mod count would have changed)
         * SO the Bulk API wouldn't update this doc.
         * <p>
         * In order to calculate the correct no. of updated documents & deleted properties, we save them in a map
         */
        private final Map<String, Integer> deletedPropsCountMap;
        private final Map<String, Integer> deletedInternalPropsCountMap;

        /**
         * Map of documentId => total no. of deleted property revisions.
         * <p>
         *
         * The document can be updated between collecting and deletion phases.
         * This would lead to document not getting deleted (since now modified date & mod count would have changed)
         * SO the Bulk API wouldn't update this doc.
         * <p>
         * In order to calculate the correct no. of updated documents & deleted property revisions, we save them in a map
         */
        private final Map<String, Integer> deletedPropRevsCountMap;
        private final Map<String, Integer> deletedInternalPropRevsCountMap;

        /**
         * {@link Set} of unmergedBranchCommit Revisions to calculate the no. of unmergedBranchCommits that would be
         * removed in this iteration of FullGC.
         */
        private final Set<Revision> deletedUnmergedBCSet;
        private int garbageDocsCount;
        private int totalGarbageDocsCount;
        private final Revision revisionForModified;
        private final DocumentNodeState root;

        /** small cache for classification of missing nodes : documents that do not exist vs deleted nodes */
        private final LinkedHashMap<Path, Boolean> missingDocsTypes;

        public FullGC(@NotNull RevisionVector headRevision, long toModifiedMs,
                      LinkedHashMap<Path, Boolean> missingDocsTypes, @NotNull GCMonitor monitor,
                      @NotNull AtomicBoolean cancel) {
            this.toModifiedMs = toModifiedMs;
            this.missingDocsTypes = missingDocsTypes;
            this.monitor = monitor;
            this.cancel = cancel;
            this.updateOpList = new ArrayList<>();
            this.orphanOrDeletedRemovalMap = new HashMap<>();
            this.orphanOrDeletedRemovalPathMap = new HashMap<>();
            this.deletedPropsCountMap = new HashMap<>();
            this.deletedInternalPropsCountMap = new HashMap<>();
            this.deletedPropRevsCountMap = new HashMap<>();
            this.deletedInternalPropRevsCountMap = new HashMap<>();
            this.deletedUnmergedBCSet = new HashSet<>();
            // clusterId is not used
            this.revisionForModified = Revision.newRevision(0);
            this.root = nodeStore.getRoot(headRevision);
        }

        public void collectGarbage(final NodeDocument doc, final GCPhases phases) {
            if (fullGcMode == NONE) {
                monitor.warn("Skipping FullGC. No Mode has been selected.");
                return;
            }

            fullGCStats.documentRead();
            monitor.info("Collecting Full Garbage for doc [{}]", doc.getId());

            if (AUDIT_LOG.isTraceEnabled()) {
                AUDIT_LOG.trace("<Collecting> Garbage in doc [{}]", doc.getId());
            }

            final UpdateOp op = new UpdateOp(requireNonNull(doc.getId()), false);

            // traversed state == state of node at doc.id based on head revision
            NodeState traversedState = root;
            Path greatestExistingAncestorOrSelf = root.getPath();
            for (String name : doc.getPath().elements()) {
                traversedState = traversedState.getChildNode(name);
                if (traversedState.exists()) {
                    greatestExistingAncestorOrSelf = new Path(
                            greatestExistingAncestorOrSelf, name);
                }
            }
            
            if (fullGcMode == EMPTYPROPS) {
                if (!traversedState.exists()) {
                    // doc is an orphan, this mode skips orphans
                    if (AUDIT_LOG.isDebugEnabled()){
                        AUDIT_LOG.debug("Skipping orphaned document [{}] for mode [{}]", doc.getId(), fullGcMode);
                    }
                    return;
                }
                collectDeletedProperties(doc, phases, op, traversedState);
            } else if (!isDeletedOrOrphanedNode(traversedState, greatestExistingAncestorOrSelf, phases, doc)) {
                // here the node is not orphaned which means that we can reach the node from root
                switch(fullGcMode) {
                    case NONE : {
                        // shouldn't be reached
                        return;
                    }
                    case GAP_ORPHANS : {
                        // this mode does neither unusedproprev, nor unmergedBC
                        break;
                    }
                    case GAP_ORPHANS_EMPTYPROPS :
                    case ALL_ORPHANS_EMPTYPROPS : {
                        collectDeletedProperties(doc, phases, op, traversedState);
                        // this mode does neither unusedproprev, nor unmergedBC
                        break;
                    }
                    case ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS : {
                        collectDeletedProperties(doc, phases, op, traversedState);
                        collectUnusedPropertyRevisions(doc, phases, op, (DocumentNodeState) traversedState, false);
                        combineInternalPropRemovals(doc, op);
                        break;
                    }
                    case ORPHANS_EMPTYPROPS_KEEP_ONE_USER_PROPS : {
                        collectDeletedProperties(doc, phases, op, traversedState);
                        collectUnusedPropertyRevisions(doc, phases, op, (DocumentNodeState) traversedState, true);
                        combineInternalPropRemovals(doc, op);
                        break;
                    }
                    case ORPHANS_EMPTYPROPS_UNMERGED_BC : {
                        collectDeletedProperties(doc, phases, op, traversedState);
                        collectUnmergedBranchCommits(doc, phases, op, toModifiedMs);
                        break;
                    }
                    case ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC : {
                        collectDeletedProperties(doc, phases, op, traversedState);
                        collectUnmergedBranchCommits(doc, phases, op, toModifiedMs);
                        collectRevisionsOlderThan24hAndBetweenCheckpoints(doc, toModifiedMs, phases, op);
                        break;
                    }
                    case ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_NO_UNMERGED_BC : {
                        collectDeletedProperties(doc, phases, op, traversedState);
                        collectRevisionsOlderThan24hAndBetweenCheckpoints(doc, toModifiedMs, phases, op);
                        break;
                    }
                }
            }
            // only add if there are changes for this doc
            if (op.hasChanges()) {
                op.equals(MODIFIED_IN_SECS, doc.getModified());
                garbageDocsCount++;
                totalGarbageDocsCount++;
                monitor.info("Collected [{}] garbage count in [{}]", op.getChanges().size(), doc.getId());
                AUDIT_LOG.info("<Collected> [{}] garbage count in [{}]", op.getChanges().size(), doc.getId());
                updateOpList.add(op);
            }
            if (log.isTraceEnabled() && op.hasChanges()) {
                // only log in case of changes & debug level enabled
                log.trace("UpdateOp for [{}] is [{}]", doc.getId(), op);
            }
        }

        private void combineInternalPropRemovals(final NodeDocument doc, final UpdateOp op) {
            // now for any of the handled system properties (the normal properties would
            // already be cleaned up by cleanupDeletedProperties), the resulting
            // sub document could in theory become empty after removing all unmerged branch
            // commit revisions is done later.
            // so we need to go through all of them and check if we'd have removed
            // the entirety - and then, instead of individually remove revisions, just
            // delete the entire property.
            if (op.hasChanges()) {
                final int deletedSystemPropsCount = getSystemRemoveMapEntryCounts(op)
                        .entrySet().stream()
                        .filter(e -> filterEmptyProps(doc, e.getKey(), e.getValue()))
                        .mapToInt(e -> {
                            final String prop = e.getKey();
                            int countBefore = op.getChanges().entrySet().size();
                            boolean removed = op.getChanges().entrySet().removeIf(opEntry -> Objects.equals(prop, opEntry.getKey().getName()));
                            int countAfter = op.getChanges().entrySet().size();
                            if (removed) {
                                if (prop.startsWith("_")) {
                                    deletedInternalPropRevsCountMap.merge(doc.getId(), (countAfter - countBefore), Integer::sum);
                                } else {
                                    deletedPropRevsCountMap.merge(doc.getId(), (countAfter - countBefore), Integer::sum);
                                }
                            }
                            op.remove(prop);
                            return 1;})
                        .sum();

                // update the deleted properties count Map to calculate the total no. of deleted properties
                int totalDeletedSystemPropsCount = deletedInternalPropsCountMap.merge(doc.getId(), deletedSystemPropsCount, Integer::sum);
                if (AUDIT_LOG.isDebugEnabled() && totalDeletedSystemPropsCount > 0) {
                    AUDIT_LOG.debug("<Collected> [{}] internal prop revs in [{}] mode [{}]", totalDeletedSystemPropsCount, doc.getId(), fullGcMode);
                }
            }
        }

        /**
         * Check if the node represented by the given doc and traversedState is
         * <i>orphaned</i>. A node is considered orphaned if it does not have a visible
         * parent node. But from a GC point of view this also includes regular
         * deletion cases that have not otherwise been deleted already (eg by DeletedDocsGC).
         *
         * @param traversedState the state of current doc when we start traversing from root via lastRef
         * @param greatestExistingAncestorOrSelf ancestor furthest up the tree (==greatest) that exists when traversing states
         * @param phases GC phases
         * @param doc current document node to check whether it is orphan or not
         * @return true if the node is orphaned (and/or can be removed), false
         *         otherwise
         */
        private boolean isDeletedOrOrphanedNode(final NodeState traversedState, final Path greatestExistingAncestorOrSelf,
                                                final GCPhases phases, final NodeDocument doc) {

            if (!phases.start(GCPhase.FULL_GC_COLLECT_ORPHAN_NODES)) {
                // gc has been cancelled
                return false;
            }

            // several different cases here, but ultimately they all lead back to:
            boolean isOrphan = !traversedState.exists();
            // if the node when reading at current headRevision (rather than traversed)
            // does not exist, then this is rather a regular deletion, nothing special.
            // that is usually handled in DeletedDocsGC - but if FullGC sees this,
            // it seems risky to not delete it right away.

            // if the node when reading at current headRevision (rather than traversed)
            // does exist, then it could be either due to the parent node having
            // been deleted (true orphan) - or the node itself got late-write-added.
            // in both of these cases we should now delete it. That's all with above return.
            if (!isOrphan) {
                // nothing to do here
                if (AUDIT_LOG.isTraceEnabled()) {
                    AUDIT_LOG.trace("<Skipping> non-orphaned node [{}]", doc.getId());
                }
                phases.stop(GCPhase.FULL_GC_COLLECT_ORPHAN_NODES);
                return false;
            }
            if (fullGcMode == GAP_ORPHANS || fullGcMode == GAP_ORPHANS_EMPTYPROPS) {
                // check the ancestor docs for gaps
                final Path docPath = doc.getPath();
                final Path geaChildPath = docPath.getAncestor(docPath.getDepth() - greatestExistingAncestorOrSelf.getDepth() - 1);
                Boolean missingType = missingDocsTypes.get(geaChildPath);
                if (missingType == null) {
                    // we don't have it cached yet - so do the potentially expensive find
                    missingType = versionStore.getDocument(getIdFromPath(geaChildPath), of(ID)).isEmpty();
                    missingDocsTypes.put(geaChildPath, missingType);
                }
                if (!missingType) {
                    if (AUDIT_LOG.isDebugEnabled()) {
                        AUDIT_LOG.debug("<Skipping> orphaned node [{}] due to non-gap in ancestor [{}]", doc.getId(), geaChildPath);
                    }
                    // then it is not a gap orphan
                    // nothing to do here then
                    // (even though somewhere along descendants
                    // there might be a gap, it is too expensive to traverse)
                    phases.stop(GCPhase.FULL_GC_COLLECT_ORPHAN_NODES);
                    // return true here as this is an orphan but we decided not to
                    // GC it - to prevent caller from attempting other GC types
                    return true;
                }
            }

            // if this is an orphaned node, all that is needed is its removal
            garbageDocsCount++;
            totalGarbageDocsCount++;
            monitor.info("Deleted orphaned or deleted doc [{}]", doc.getId());
            orphanOrDeletedRemovalMap.put(doc.getId(), doc.getModified());
            orphanOrDeletedRemovalPathMap.put(doc.getId(), doc.getPath());
            fullGCStats.candidateDocuments(GCPhase.FULL_GC_COLLECT_ORPHAN_NODES, 1);

            if (AUDIT_LOG.isDebugEnabled()) {
                AUDIT_LOG.debug("<Collected> [{}] orphaned node", doc.getId());
            }

            phases.stop(GCPhase.FULL_GC_COLLECT_ORPHAN_NODES);
            return true;

        }

        private boolean hasGarbage() {
            return garbageDocsCount > 0;
        }

        private void collectDeletedProperties(final NodeDocument doc, final GCPhases phases, final UpdateOp updateOp,
                                              final NodeState traversedState) {

            // get Map of all properties along with their values
            if (phases.start(GCPhase.FULL_GC_COLLECT_PROPS)) {
                final Set<String> properties = doc.getPropertyNames();

                // find all the properties which can be removed from document.
                // All the properties whose value is null in head revision are
                // eligible to be garbage collected.

                final Set<String> retainPropSet = ofNullable(traversedState instanceof DocumentNodeState ? (DocumentNodeState)traversedState : null)
                        .map(DocumentNodeState::getAllBundledProperties)
                        .map(Map::keySet)
                        .map(p -> p.stream().map(Utils::escapePropertyName).collect(toSet()))
                        .orElse(emptySet());

                final int deletedPropsCount = properties.stream()
                        .filter(p -> !retainPropSet.contains(p))
                        .mapToInt(x -> {
                            updateOp.remove(x);
                            return 1;})
                        .sum();

                deletedPropsCountMap.put(doc.getId(), deletedPropsCount);
                fullGCStats.candidateProperties(GCPhase.FULL_GC_COLLECT_PROPS, deletedPropsCount);

                if (AUDIT_LOG.isDebugEnabled() && deletedPropsCount > 0) {
                    AUDIT_LOG.debug("<Collected> [{}] deleted props in [{}]", deletedPropsCount, doc.getId());
                }
                phases.stop(GCPhase.FULL_GC_COLLECT_PROPS);
            }
        }

        private void collectUnmergedBranchCommits(final NodeDocument doc, final GCPhases phases, final UpdateOp updateOp,
                                                  final long toModifiedMs) {
            if (!phases.start(GCPhase.FULL_GC_COLLECT_UNMERGED_BC)) {
                // GC was cancelled, stop
                return;
            }

            // from https://jackrabbit.apache.org/oak/docs/nodestore/documentmk.html#previous-documents
            // "branch commits are not moved to previous documents until the branch is merged."
            // i.e. if we're looking for unmerged branch commits, they cannot be in
            // previous documents, they have to be in the main one - hence we have to use
            // getLocalBranchCommits here
            final Set<Revision> olderUnmergedBranchCommits = doc.getLocalBranchCommits().stream()
                    .filter(bcRevision -> isRevisionOlderThan(bcRevision, toModifiedMs))
                    .filter(bcRevision -> !isCommitted(nodeStore.getCommitValue(bcRevision, doc)))
                    .collect(toSet());

            if (olderUnmergedBranchCommits.isEmpty()) {
                // nothing to do then
                phases.stop(GCPhase.FULL_GC_COLLECT_UNMERGED_BC);
                return;
            }

            // !Note, the _bc sub-document was introduced with Oak 1.8 and is not present in older versions.
            // The branch commit revision is added to _bc whenever a change is done on a document with a
            // branch commit. This helps the DocumentNodeStore to more easily identify branch commit changes."
            // The current implementation of "collectUnmergedBranchCommitDocument" only
            // supports branch commits that are created after Oak 1.8
            
            olderUnmergedBranchCommits.forEach(bcRevision -> removeUnmergedBCRevision(bcRevision, doc, updateOp));
            deletedUnmergedBCSet.addAll(olderUnmergedBranchCommits);

            if (AUDIT_LOG.isDebugEnabled()) {
                AUDIT_LOG.debug("<Collected> [{}] unmerged branch commits in [{}]", olderUnmergedBranchCommits.size(), doc.getId());
            }

            // now for any of the handled system properties (the normal properties would
            // already be cleaned up by cleanupDeletedProperties), the resulting
            // sub document could in theory become empty after removing all unmerged branch
            // commit revisions is done later.
            // so we need to go through all of them and check if we'd have removed
            // the entirety - and then, instead of individually remove revisions, just
            // delete the entire property.
            if (updateOp.hasChanges()) {
                final int deletedSystemPropsCount = getSystemRemoveMapEntryCounts(updateOp)
                        .entrySet().stream()
                        .filter(e -> filterEmptyProps(doc, e.getKey(), e.getValue()))
                        .mapToInt(e -> {
                            final String prop = e.getKey();
                            final int origCount = updateOp.getChanges().size();
                            updateOp.getChanges().entrySet().removeIf(opEntry -> Objects.equals(prop, opEntry.getKey().getName()));
                            final int diff = origCount - updateOp.getChanges().size();
                            //TODO: beautify once this is in FullGC branch
                            if (diff > 0) {
                                deletedInternalPropRevsCountMap.merge(doc.getId(), -diff, Integer::sum);
                            }
                            updateOp.remove(prop);
                            return 1;})
                        .sum();

                // update the deleted properties count Map to calculate the total no. of deleted properties
                int totalDeletedSystemPropsCount = deletedInternalPropsCountMap.merge(doc.getId(), deletedSystemPropsCount, Integer::sum);
                fullGCStats.candidateInternalRevisions(GCPhase.FULL_GC_COLLECT_UNMERGED_BC, totalDeletedSystemPropsCount);

                if (AUDIT_LOG.isDebugEnabled() && totalDeletedSystemPropsCount > 0) {
                    AUDIT_LOG.debug("<Collected> [{}] internal prop revs in [{}] mode [{}]", totalDeletedSystemPropsCount, doc.getId(), fullGcMode);
                }
            }
            phases.stop(GCPhase.FULL_GC_COLLECT_UNMERGED_BC);
        }

        /**
         * Filter all would be empty system properties (after cleanup operation).
         * <p>
         * It verifies this by comparing the size of sub-document with given <code>value</code>
         *
         * @param doc {@link NodeDocument} on whose properties needs to be checked
         * @param prop Name of sub-document which needs to checked whether it would be empty after cleanup or not
         * @param value expected no. of entries
         * @return true if sub-document would eventually be empty or not
         */
        private boolean filterEmptyProps(final NodeDocument doc, final String prop, final int value) {
            final Object d = doc.data.get(prop);
            if (d instanceof Map) {
                @SuppressWarnings("rawtypes") final Map m = (Map) d;
                // then we're not removing all revisions - so cannot clean up
                return m.size() == value;
            } else {
                // unexpected and would likely indicate a bug, hence log.error
                log.error("collectUnmergedBranchCommitDocument: property without sub-document as expected. " +
                        "id={}, prop={}", doc.getId(), prop);
                return false;
            }
        }

        /** small helper to count number of REMOVE_MAP_ENTRY per system property */
        private Map<String, Integer> getSystemRemoveMapEntryCounts(final UpdateOp updateOp) {
            final Map<String, Integer> propMap = new HashMap<>();

            updateOp.getChanges().entrySet().stream()
                    .filter(e -> e.getValue().type == Type.REMOVE_MAP_ENTRY)
                    .map(e -> e.getKey().getName())
                    .filter(propName -> propName.startsWith("_"))
                    .forEach(propName -> propMap.merge(propName, 1, Integer::sum));

            return propMap;
        }

        /** small helper to check if the revision is older than the timestamp */
        private boolean isRevisionOlderThan(Revision revision, long toModifiedMillis) {
            long time = revision.getTimestamp();
            return time <= toModifiedMillis;
        }

        /**
         * Clean up one uncommitted branch commit in the provided document. The caller
         * establishes whether a branch commit revision is committed or not - this is no
         * longer checked in this method. The resulting operations are added to the
         * provided updateOp.
         * <p/>
         * The actions depend on the exacty property key - here's the comprehensive list
         * of system properties and how they are handled:
         * <ul>
         * <li>"_id" and "_path" are not affected</li>
         * <li>"_prev" and "stalePrev" are not affected</li>
         * <li>"_lastRev" and "_sweepRev" are not affected</li>
         * <li>"_bc" is cleaned up</li>
         * <li>"_revisions" is cleaned up</li>
         * <li>"_commitRoot" is cleaned up</li>
         * <li>"_collisions" might be a special case but is also cleaned up</li>
         * <li>"_modified" and "_deletedOnce" are just single values and thus not
         * generally affected, but they are updated when "_deleted" is cleaned up</li>
         * <li>"_deleted" is cleaned up - plus "_modified" and "_deletedOnce" are also
         * updated for this, to have classic GC later potentially delete the
         * document</li>
         * <li>"_children" is only ever set to true, never back - hence no need for
         * cleanup/modification by FullGC</li>
         * <li>"_bin" is only ever set to 1, never back - hence no need for
         * cleanup/modification by FullGC</li>
         * </ul>
         *
         * @param unmergedBCRevision the unmerged branch commit revision - the caller
         *                           makes sure this revision is indeed not merged and
         *                           old enough to be removed
         * @param doc                the document from which the uncommittedBCRevision
         *                           should be removed
         * @param updateOp           the resulting operations yet to be applied
         */
        private void removeUnmergedBCRevision(final Revision unmergedBCRevision, final NodeDocument doc,
                                              final UpdateOp updateOp) {
            int internalRevEntriesCount = 0;
            int revEntriesCount = 0;
            // caller ensures the provided revision is an unmerged branch commit
            if (doc.getLocalBranchCommits().contains(unmergedBCRevision)) {
                internalRevEntriesCount++;
            }
            NodeDocument.removeBranchCommit(updateOp, unmergedBCRevision);

            // phase 1 : remove unmerged bc revisions from _deleted - unmerged branch
            // commits can only be in the local set
            final String unmergedDeleted = doc.getLocalDeleted().get(unmergedBCRevision);
            if (unmergedDeleted != null) {
                internalRevEntriesCount++;
                NodeDocument.removeDeleted(updateOp, unmergedBCRevision);

                // phase 2: the document could now effectively be "deleted" the actual
                // removal from the document from the store is left to DeletedDocsGC (to
                // avoid code duplication)

                // if unmergedDeleted is "false", then it was created with this branch
                // commit, but that was never merged. when we now remove that, it could be
                // that it is then deleted.

                // to know whether the node is actually deleted, would potentially
                // require several commit value lookups.
                // in order to keep the execution time of fullGC in this regard small,
                // the code here stops with any further checks and just sets
                // "_deletedOnce" to true and updates "_modified" - the DeletedDocsGC
                // later would either resurrect or delete the document properly (eg
                // including previous docs)
                if ("false".equals(unmergedDeleted)) {
                    if (!doc.wasDeletedOnce()) {
                        NodeDocument.setDeletedOnce(updateOp);
                    }
                    NodeDocument.setModified(updateOp, revisionForModified);
                }
            }
            // phase 3 : go through other system properties
            if (doc.getLocalCommitRoot().containsKey(unmergedBCRevision)) {
                internalRevEntriesCount++;
                NodeDocument.removeCommitRoot(updateOp, unmergedBCRevision);
            }
            if (doc.getLocalRevisions().containsKey(unmergedBCRevision)) {
                internalRevEntriesCount++;
                NodeDocument.removeRevision(updateOp, unmergedBCRevision);
            }
            if (doc.getLocalMap(COLLISIONS).containsKey(unmergedBCRevision)) {
                internalRevEntriesCount++;
                NodeDocument.removeCollision(updateOp, unmergedBCRevision);
            }
            // phase 4 : go through normal properties
            for (String propName : doc.getPropertyNames()) {
                // first check if this property might have been (flagged to be)
                // entirely removed by the collectDeletedProperties - in which case
                // there is a corresponding UpdateOp
                final Operation op = updateOp.getChanges().get(new Key(propName, null));
                if (op != null && op.type == Type.REMOVE) {
                    // ignore this property then, it will be removed entirely
                    continue;
                }
                if (doc.getLocalMap(propName).containsKey(unmergedBCRevision)) {
                    updateOp.removeMapEntry(propName, unmergedBCRevision);
                    revEntriesCount++;
                }
            };
            if (internalRevEntriesCount > 0) {
                deletedInternalPropRevsCountMap.merge(doc.getId(), internalRevEntriesCount, Integer::sum);
            }
            if (revEntriesCount > 0) {
                deletedPropRevsCountMap.merge(doc.getId(), revEntriesCount, Integer::sum);
            }
            fullGCStats.candidateRevisions(GCPhase.FULL_GC_COLLECT_UNMERGED_BC, revEntriesCount);
            fullGCStats.candidateInternalRevisions(GCPhase.FULL_GC_COLLECT_UNMERGED_BC, internalRevEntriesCount);

            if (AUDIT_LOG.isDebugEnabled() && (revEntriesCount > 0 || internalRevEntriesCount > 0)) {
                AUDIT_LOG.debug("<Collected> [{}] prop revs, [{}] internal prop revs in [{}] mode [{}]", revEntriesCount, internalRevEntriesCount, doc.getId(), fullGcMode);
            }

        }

        private void collectRevisionsOlderThan24hAndBetweenCheckpoints(final NodeDocument doc, final long toModifiedMs,
                                                              final GCPhases phases, final UpdateOp updateOp) {
            if (phases.start(GCPhase.FULL_GC_COLLECT_OLD_REVS)){
                NodeDocumentRevisionCleaner cleaner = new NodeDocumentRevisionCleaner(nodeStore, doc, toModifiedMs);
                final int beforeRevs = countRevs(updateOp, false);
                final int beforeIntRevs = countRevs(updateOp, true);
                cleaner.collectOldRevisions(updateOp);
                final int revsDiff = countRevs(updateOp, false) - beforeRevs;
                if (revsDiff > 0) {
                    deletedPropRevsCountMap.merge(doc.getId(), revsDiff, Integer::sum);
                }
                final int intRevsDiff = countRevs(updateOp, true) - beforeIntRevs;
                if (intRevsDiff > 0) {
                    deletedInternalPropRevsCountMap.merge(doc.getId(), intRevsDiff, Integer::sum);
                }
                if (AUDIT_LOG.isDebugEnabled() && (revsDiff > 0 || intRevsDiff > 0)) {
                    AUDIT_LOG.debug("<Collected> [{}] prop revs, [{}] internal prop revs in [{}] mode [{}]", revsDiff, intRevsDiff, doc.getId(), fullGcMode);
                }
                fullGCStats.candidateRevisions(GCPhase.FULL_GC_COLLECT_OLD_REVS, revsDiff);
                fullGCStats.candidateInternalRevisions(GCPhase.FULL_GC_COLLECT_OLD_REVS, intRevsDiff);

                phases.stop(GCPhase.FULL_GC_COLLECT_OLD_REVS);
            }
        }

        private int countRevs(UpdateOp updateOp, boolean internalProps) {
            return updateOp.getChanges().entrySet().stream()
                    .filter(e -> (e.getValue().type == Type.REMOVE_MAP_ENTRY))
                    .filter(e -> (e.getKey().getName().startsWith("_") == internalProps))
                    .mapToInt(x -> 1).sum();
        }

        /**
         * Remove all property revisions in the local document that are no longer used.
         * This includes bundled properties. It also includes related entries that
         * become obsolete as a result - i.e. _commitRoot and _bc (though the latter
         * is never removed on root)
         */
        private void collectUnusedPropertyRevisions(final NodeDocument doc,
                final GCPhases phases, final UpdateOp updateOp,
                final DocumentNodeState traversedMainNode,
                final boolean ignoreInternalProperties) {

            if (!phases.start(GCPhase.FULL_GC_COLLECT_OLD_REVS)){
                // cancelled
                return;
            }
            final Set<Revision> allKeepRevs = new HashSet<>();
            // phase A : collectUnusedUserPropertyRevisions
            int deletedTotalRevsCount = collectUnusedUserPropertyRevisions(doc, updateOp, traversedMainNode, allKeepRevs);
            int deletedUserRevsCount = deletedTotalRevsCount;
            // phase B : collectUnusedInternalPropertyRevisions
            if (!ignoreInternalProperties) {
                deletedTotalRevsCount = collectUnusedInternalPropertyRevisions(doc, updateOp, allKeepRevs, deletedTotalRevsCount);
            }

            // then some accounting...
            int deletedInternalRevsCount = deletedTotalRevsCount - deletedUserRevsCount;
            if (deletedUserRevsCount != 0) {
                deletedPropRevsCountMap.merge(doc.getId(), deletedUserRevsCount, Integer::sum);
            }
            if (deletedInternalRevsCount != 0) {
                deletedInternalPropRevsCountMap.merge(doc.getId(), deletedInternalRevsCount, Integer::sum);
            }

            if (AUDIT_LOG.isDebugEnabled() && deletedTotalRevsCount > 0) {
                AUDIT_LOG.debug("<Collected> [{}] prop revs, [{}] internal prop revs in [{}] mode [{}]", deletedUserRevsCount, deletedInternalRevsCount, doc.getId(), fullGcMode);
            }
            fullGCStats.candidateRevisions(GCPhase.FULL_GC_COLLECT_OLD_REVS, deletedUserRevsCount);
            fullGCStats.candidateInternalRevisions(GCPhase.FULL_GC_COLLECT_OLD_REVS, deletedInternalRevsCount);

            phases.stop(GCPhase.FULL_GC_COLLECT_OLD_REVS);
        }

        private int collectUnusedUserPropertyRevisions(final NodeDocument doc, final UpdateOp updateOp,
                                                       final DocumentNodeState traversedMainNode, final Set<Revision> allKeepRevs) {
            // phase 1 : non bundled nodes only
            int deletedRevsCount = StreamSupport.stream(
                    traversedMainNode.getProperties().spliterator(), false)
                            .map(p -> Utils.escapePropertyName(p.getName()))
                            .mapToInt(p -> removeUnusedPropertyEntries(doc,
                                    traversedMainNode, updateOp, p,
                                    r -> updateOp.removeMapEntry(p, r),
                                    allKeepRevs))
                            .sum();

            // phase 2 : bundled nodes only
            final Map<Path, DocumentNodeState> bundledNodeStates = StreamSupport.stream(
                    traversedMainNode.getAllBundledNodesStates().spliterator(), false)
                            .collect(toMap(DocumentNodeState::getPath, identity()));
            // remember that getAllBundledProperties returns unescaped keys
            for (String propName : traversedMainNode.getAllBundledProperties().keySet()) {
                final int lastSlash = propName.lastIndexOf("/");
                if (lastSlash == -1) {
                    // then it is an unbundled property which was already handled in phase 1
                    continue;
                }
                final String escapedPropName = Utils.escapePropertyName(propName);
                // bundled values are of format sub/tree/propertyKey
                // to extract this we need the last index of "/"
                final String unbundledSubtreeName = propName.substring(0, lastSlash);
                final String unbundledPropName = propName.substring(lastSlash + 1);
                final String unbundledPath = traversedMainNode.getPath().toString() + "/" + unbundledSubtreeName;
                final DocumentNodeState traversedNode = bundledNodeStates.get(Path.fromString(unbundledPath));
                if (traversedNode == null) {
                    log.error("collectUnusedPropertyRevisions : could not find traversed node for bundled key [{}] unbundledPath [{}] in doc [{}]",
                            propName, unbundledPath, doc.getId());
                    continue;
                }
                final PropertyState traversedProperty = traversedNode.getProperty(unbundledPropName);
                if (traversedProperty == null) {
                    log.error("collectUnusedPropertyRevisions : could not get property [{}] from traversed node [{}]",
                            unbundledPropName, traversedNode.getPath());
                    continue;
                }
                deletedRevsCount += removeUnusedPropertyEntries(doc, traversedNode, updateOp, escapedPropName,
                        (r) -> updateOp.removeMapEntry(escapedPropName, r),
                        allKeepRevs);
            }

            // phase 3 : "_deleted"
            int numDeleted = removeUnusedPropertyEntries(doc, traversedMainNode,
                    updateOp, NodeDocument.DELETED,
                    (r) -> NodeDocument.removeDeleted(updateOp, r),
                    allKeepRevs);
            deletedRevsCount += numDeleted;
            return deletedRevsCount;
        }

        private int collectUnusedInternalPropertyRevisions(final NodeDocument doc, final UpdateOp updateOp,
                                                           final Set<Revision> toKeepUserPropRevs, int deletedRevsCount) {
            boolean hasUnmergedBranchCommits = doc.getLocalBranchCommits().stream()
                    .anyMatch(r -> !isCommitted(nodeStore.getCommitValue(r, doc)));
            if (deletedRevsCount == 0 && !hasUnmergedBranchCommits) {
                return deletedRevsCount;
            }
            // if we did some rev deletion OR there are unmerged BCs, then let's deep-dive
            Set<Revision> allRequiredRevs = new HashSet<>(toKeepUserPropRevs);
            // "_collisions"
            deletedRevsCount += getDeletedRevsCount(
                    doc.getLocalMap(COLLISIONS).keySet(), updateOp,
                    allRequiredRevs, COLLISIONS,
                    NodeDocument::removeCollision);
            // "_revisions"
            for (Entry<Revision, String> e : doc.getLocalRevisions().entrySet()) {
                Revision revision = e.getKey();
                if (allRequiredRevs.contains(revision)) {
                    // if it is still referenced locally, keep it
                    continue;
                }
                final boolean isRoot = doc.getId().equals(getIdFromPath(Path.ROOT));
                // local bcs only considered for removal
                final boolean isBC = doc.getLocalBranchCommits().contains(revision);
                final boolean newerThanSweep = nodeStore.getSweepRevisions().isRevisionNewer(revision);
                if (newerThanSweep) {
                    if (isBC) {
                        // analysed further down, we can remove them if unmerged
                    } else {
                        // note that ideally we wouldn't ever get to handle a revision
                        // that is younger than sweep, as sweep is usually only a few
                        // seconds behind and GC usually 24h.
                        // in edge cases however, it could happen.
                        // hence we could try to avoid even that it happens at all
                        // by taking sweep into account in the original query.
                        // Except, that's not feasible with the current state of the document,
                        // where "_modified" is cross-cluster, but sweep is per-clusterId.
                        // So which ever value we'd choose in the original query, it wouldn't
                        // precisely match the sweep revision vector in any case.
                        // thus we should rather accept that we could in theory
                        // get a revision that is newer than sweep, and act accordingly.
                        // one way would be to try to distinguish branch commit vs normal commits.
                        // another, simpler, way however is to simply skip these.
                        // that, yes, opens the possibility of some garbage not being
                        // cleaned up, should the document remain unchanged ever after.
                        // but that seems low probability and justifyable.
                        // so:
                        // for normal commits, we need to keep them -> also add to allRequiredRevs
                        allRequiredRevs.add(revision);
                        continue;
                    }
                }
                // committed revisions are hard to delete, as determining
                // whether or not they are orphaned is difficult.
                // child nodes could be having _commitRoots pointing to this doc
                // and this revision - in which case it wouldn't be orphaned.
                // without scanning the children (or having knowledge about
                // which revisions are referenced by children) it is unsafe
                // to delete committed revisions.
                // if they are uncommitted, then they are garbage, as then they
                // are either normal commits (thus a not finished commit)
                // or an unmerged branch commit. In both cases they are garbage.
                boolean isCommitted = isCommitted(nodeStore.getCommitValue(revision, doc));
                if (isCommitted) {
                    if (isRoot) {
                        // root : cannot remove since it could be referenced and required by a child
                        // also add to allRequiredRevs
                        allRequiredRevs.add(revision);
                        continue;
                    } else if (!isBC) {
                        // non root and normal : cannot remove, same as above,
                        // it could be referenced and required by a child 
                        // also add to allRequiredRevs
                        allRequiredRevs.add(revision);
                        continue;
                    } else {
                        // non root and bc : can remove, as non root bc cannot be referenced by child
                    }
                }
                Operation has = updateOp.getChanges().get(new Key(REVISIONS, revision));
                if (has != null) {
                    // then skip
                    continue;
                }
                NodeDocument.removeRevision(updateOp, revision);
                deletedRevsCount++;
            }
            // "_commitRoot"
            deletedRevsCount += getDeletedRevsCount(doc.getLocalCommitRoot().keySet(),
                    updateOp, allRequiredRevs, COMMIT_ROOT, NodeDocument::removeCommitRoot);
            // "_bc"
            deletedRevsCount += getDeletedRevsCount(doc.getLocalBranchCommits(), updateOp,
                    allRequiredRevs, BRANCH_COMMITS, NodeDocument::removeBranchCommit);
            return deletedRevsCount;
        }

        private int getDeletedRevsCount(Set<Revision> revisionSet, UpdateOp updateOp, Set<Revision> allRequiredRevs, String updateOpKey, BiConsumer<UpdateOp, Revision> op) {
            return revisionSet.stream().filter(r -> !allRequiredRevs.contains(r))
                    .mapToInt(r -> {
                        Operation has = updateOp.getChanges().get(new Key(updateOpKey, r));
                        if (has != null) {
                            // then skip
                            return 0;
                        }

                        op.accept(updateOp, r);
                        return 1;
                    }).sum();
        }

        private int removeUnusedPropertyEntries(NodeDocument doc,
                DocumentNodeState traversedMainNode, UpdateOp updateOp,
                String propertyKey, Consumer<Revision> removeRevision,
                Set<Revision> allKeepRevs) {
            // we need to use the traversedNode.getLastRevision() as the readRevision,
            // as that is what was originally used in getNodeAtRevision when traversing
            final Revision keepCommitRev = doc.localCommitRevisionOfProperty(nodeStore,
                    traversedMainNode.getLastRevision(), propertyKey);
            if (keepCommitRev == null) {
                // could be due to node not existing or current value being in a split
                // doc - while the former is unexpected, the latter might happen.
                // in both cases let's skip this property
                if (AUDIT_LOG.isTraceEnabled()) {
                    AUDIT_LOG.trace("<remove> : [0] visible revision for property [{}] in [{}]", propertyKey, doc.getId());
                }
                return 0;
            }
            // if we get a revision it is from the local map.
            // paranoia check that
            final SortedMap<Revision, String> localMap = doc.getLocalMap(propertyKey);
            if (!localMap.containsKey(keepCommitRev)) {
                // this is unexpected - log and skip this property
                log.error("removeUnusedPropertyEntries : revision [{}] for property [{}] not found in doc [{}]", keepCommitRev, propertyKey, doc.getId());
                return 0;
            }
            int count = 0;
            // in this case we are good to delete all but the keepRevision
            for (Revision localRev : localMap.keySet()) {
                if (!keepCommitRev.equals(localRev)) {
                    // if the localRev is a branch commit, it might be unmerged,
                    // in which case it might already have been marked for removal
                    // via collectUnmergedBranchCommits. Checking for that next.
                    Operation c = updateOp.getChanges().get(new Key(propertyKey, localRev));
                    if (c == null) {
                        if (AUDIT_LOG.isTraceEnabled()) {
                            AUDIT_LOG.trace("<remove> : property key [{}] revision [{}] in [{}]", propertyKey, localRev, doc.getId());
                        }
                        removeRevision.accept(localRev);
                        count++;
                    }
                }
            }
            allKeepRevs.add(keepCommitRev);
            return count;
        }


        int getGarbageCount() {
            return totalGarbageDocsCount;
        }

        @Override
        public void close() {
            totalGarbageDocsCount = 0;
        }


        public void removeGarbage(final VersionGCStats stats) {

            if (updateOpList.isEmpty() && orphanOrDeletedRemovalMap.isEmpty()) {
                if (log.isDebugEnabled() || isFullGCDryRun) {
                    log.debug("Skipping removal of Full garbage, cause no garbage detected");
                }
                return;
            }

            monitor.info("Proceeding to update [{}] documents", updateOpList.size());

            if (AUDIT_LOG.isDebugEnabled() || isFullGCDryRun) {
                String updateIds = updateOpList.stream().map(UpdateOp::getId).collect(joining(", "));
                String orphanIds = join(", ", orphanOrDeletedRemovalMap.keySet());
                log.debug("Performing batch update of ids [{}] and removal of orphan ids [{}]", updateIds, orphanIds);
            }

            if (cancel.get()) {
                log.info("Aborting the removal of Full garbage since RGC had been cancelled");
                return;
            }

            try {
                if (embeddedVerification) {
                    // embedded verification is done completely independent of DocumentStore
                    // also, it's done irrespective of dry-run or not
                    final Iterator<UpdateOp> it = updateOpList.iterator();
                    while(it.hasNext()) {
                        final UpdateOp update = it.next();
                        NodeDocument oldDoc = ds.find(Collection.NODES, update.getId());
                        if (oldDoc == null) {
                            log.error("removeGarbage.verify : no document found for update with id [{}]", update.getId());
                            continue;
                        }
                        NodeState traversedParent = null;
                        NodeState traversedState = root;
                        for (String name : oldDoc.getPath().elements()) {
                            traversedParent = traversedState;
                            traversedState = traversedState.getChildNode(name);
                        }

                        NodeDocument newDoc = Collection.NODES.newDocument(ds);
                        oldDoc.deepCopy(newDoc);
                        UpdateUtils.applyChanges(newDoc, update);
                        // for the time being, verify both with classic and traversed
                        if (!verifyViaTraversedState(traversedState, traversedParent, newDoc)) {
                            // verification failure
                            // let's skip this document
                            if (log.isDebugEnabled()) {
                                log.debug("removeGarbage.verify : verifyViaTraversedState failed for [{}]", newDoc.getId());
                            }
                            it.remove();
                            stats.skippedFullGCDocsCount++;
                        }
                    }
                    for (Entry<String, Long> e : orphanOrDeletedRemovalMap.entrySet()) {
                        NodeState traversedState = root;
                        final String id = e.getKey();
                        final Path path = orphanOrDeletedRemovalPathMap.get(id);
                        if (path == null) {
                            // rather a bug, so let's skip it
                            log.error("removeGarbage.verify : no path available for id : [{}]", id);
                            it.remove();
                            stats.skippedFullGCDocsCount++;
                            continue;
                        }
                        for (String name : path.elements()) {
                            traversedState = traversedState.getChildNode(name);
                        }
                        if (!verifyDeletion(traversedState)) {
                            // verification failure
                            // let's skip this document
                            if (log.isDebugEnabled()) {
                                log.debug("removeGarbage.verify : verifyDeletion failed for [{}]", e.getKey());
                            }
                            it.remove();
                            stats.skippedFullGCDocsCount++;
                        }
                    }
                }
                if (!isFullGCDryRun) {
                    // only delete these in case it is not a dryRun

                    if (!orphanOrDeletedRemovalMap.isEmpty()) {
                        // use remove() with the modified check to rule
                        // out any further race-condition where this removal
                        // races with a un-orphan/re-creation as a result of which
                        // the node should now not be removed. The modified check
                        // ensures a node would then not be removed
                        // (and as a result the removedSize != map.size())
                        final int removedSize = ds.remove(NODES, orphanOrDeletedRemovalMap);
                        stats.updatedFullGCDocsCount += removedSize;
                        stats.deletedDocGCCount += removedSize;
                        stats.deletedOrphanNodesCount += removedSize;

                        if (AUDIT_LOG.isDebugEnabled()) {
                            AUDIT_LOG.debug("<delete> [{}] documents (from intended {})", removedSize, orphanOrDeletedRemovalMap.size());
                        }

                        // save stats
                        fullGCStats.documentsUpdated(removedSize);
                        fullGCStats.orphanNodesDeleted(removedSize);
                        gcStats.documentsDeleted(removedSize);
                        // fix for sonar : converted to long before operation
                        fullGCStats.documentsUpdateSkipped((long)orphanOrDeletedRemovalMap.size() - removedSize);
                    }

                    if (!updateOpList.isEmpty()) {
                        List<NodeDocument> oldDocs = ds.findAndUpdate(NODES, updateOpList);


                        int deletedProps = oldDocs.stream().filter(Objects::nonNull).mapToInt(d -> deletedPropsCountMap.getOrDefault(d.getId(), 0)).sum();
                        int deletedInternalProps = oldDocs.stream().filter(Objects::nonNull).mapToInt(d -> deletedInternalPropsCountMap.getOrDefault(d.getId(), 0)).sum();
                        int deletedRevEntriesCount = oldDocs.stream().filter(Objects::nonNull).mapToInt(d -> deletedPropRevsCountMap.getOrDefault(d.getId(), 0)).sum();
                        int deletedInternalRevEntriesCount = oldDocs.stream().filter(Objects::nonNull).mapToInt(d -> deletedInternalPropRevsCountMap.getOrDefault(d.getId(), 0)).sum();
                        int updatedDocs = (int) oldDocs.stream().filter(Objects::nonNull).count();
                        stats.updatedFullGCDocsCount += updatedDocs;
                        stats.deletedPropsCount += deletedProps;
                        stats.deletedInternalPropsCount += deletedInternalProps;
                        stats.deletedPropRevsCount += deletedRevEntriesCount;
                        stats.deletedInternalPropRevsCount += deletedInternalRevEntriesCount;
                        stats.deletedUnmergedBCCount += deletedUnmergedBCSet.size();

                        if (log.isDebugEnabled()) {
                            log.debug("Updated [{}] docs, deleted [{}] props, deleted [{}] unmergedBCs, deleted [{}] internal Props, deleted [{}] prop revs, deleted [{}] internal prop revs",
                                    updatedDocs, deletedProps, deletedUnmergedBCSet.size(), deletedInternalProps, deletedRevEntriesCount, deletedInternalRevEntriesCount);
                        }

                        // save stats
                        fullGCStats.propertiesDeleted(deletedProps);
                        fullGCStats.unmergedBranchCommitsDeleted(deletedUnmergedBCSet.size());
                        fullGCStats.documentsUpdated(updatedDocs);
                        // fix for sonar : converted to long before operation
                        fullGCStats.documentsUpdateSkipped((long)oldDocs.size() - updatedDocs);
                    }
                } else {
                    // collect approx stats only in case of dryRun by assuming everything would succeed

                    // for deleted properties, oldRevs & unmergedBC
                    stats.updatedFullGCDocsCount += updateOpList.size();
                    stats.deletedPropsCount += deletedPropsCountMap.values().stream().reduce(0, Integer::sum);
                    stats.deletedInternalPropsCount += deletedInternalPropsCountMap.values().stream().reduce(0, Integer::sum);
                    stats.deletedPropRevsCount += deletedPropRevsCountMap.values().stream().reduce(0, Integer::sum);
                    stats.deletedInternalPropRevsCount += deletedInternalPropRevsCountMap.values().stream().reduce(0, Integer::sum);
                    stats.deletedUnmergedBCCount += deletedUnmergedBCSet.size();

                    // for orphan nodes.
                    stats.updatedFullGCDocsCount += orphanOrDeletedRemovalMap.size();
                    stats.deletedDocGCCount += orphanOrDeletedRemovalMap.size();
                    stats.deletedOrphanNodesCount += orphanOrDeletedRemovalMap.size();
                }
            } finally {
                // now reset delete metadata
                updateOpList.clear();
                orphanOrDeletedRemovalMap.clear();
                orphanOrDeletedRemovalPathMap.clear();
                deletedPropsCountMap.clear();
                deletedInternalPropsCountMap.clear();
                deletedPropRevsCountMap.clear();
                deletedInternalPropRevsCountMap.clear();
                deletedUnmergedBCSet.clear();
                garbageDocsCount = 0;
            }
        }

        private boolean verifyViaTraversedState(NodeState traversedState, NodeState traversedParent, NodeDocument newDoc) {
            final Path path = newDoc.getPath();
            final Revision lastRevision = nodeStore.getPendingModifications().get(path);
            if (traversedParent == null && !newDoc.getPath().isRoot()) {
                log.error("verify : no parent but not root for path : [{}]", newDoc.getPath());
                return false;
            }
            final RevisionVector lastRev;
            if (traversedParent == null && newDoc.getPath().isRoot()) {
                if (!(traversedState instanceof DocumentNodeState)) {
                    log.error("verify : traversedState not a DocumentNodeState : [{}]", traversedState.getClass());
                    return false;
                }
                lastRev = ((DocumentNodeState) traversedState).getLastRevision();
            } else {
                // in case we are reaching here, it means that node is none of parents are null
                if (!traversedParent.exists()) {
                    // if the parent doesn't exist we shouldn't reach this point at all
                    log.error("verify : no parent but not marked for removal for path : {}", newDoc.getPath());
                    return false;
                }
                if (!(traversedParent instanceof DocumentNodeState)) {
                    log.error("verify : traversedParent not a DocumentNodeState : {}", traversedParent.getClass());
                    return false;
                }
                lastRev = ((DocumentNodeState) traversedParent).getLastRevision();
            }
            final NodeState actual = newDoc.getNodeAtRevision(nodeStore, lastRev, lastRevision);
            // use more thorough version of equals to ensure properties are checked
            // (the faster state.equals() would stop if lastRev matches,
            // but as we're fiddling with immutability rule of a document,
            // we need to do a full check)
            return nonNull(actual) && AbstractNodeState.equals(traversedState, actual);
        }

        private boolean verifyDeletion(NodeState traversedState) {
            return !traversedState.exists();
        }
    }
    private void delayOnModifications(final long durationMs, final AtomicBoolean cancel, final double delayFactor) {
        long delayMs = round(durationMs * delayFactor);
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

    public void collectGarbageOnDocument(DocumentNodeStore store, NodeDocument doc, boolean verbose) {
        VersionGCStats stats = new VersionGCStats();
        stats.active.start();
        AtomicBoolean cancel = new AtomicBoolean();
        GCPhases phases = new GCPhases(cancel, stats, gcMonitor);

        final LinkedHashMap<Path, Boolean> missingDocsTypes = new LinkedHashMap<>();
        final RevisionVector headRevision = store.getHeadRevision();
        try (FullGC gc = new FullGC(headRevision, 0, missingDocsTypes, gcMonitor, cancel)) {

            if (phases.start(GCPhase.FULL_GC_COLLECT_GARBAGE)) {
                gc.collectGarbage(doc, phases);
                phases.stop(GCPhase.FULL_GC_COLLECT_GARBAGE);
            }

            if (verbose) {
                gcMonitor.info("GarbageCollector will run [{}] operations", gc.updateOpList.size());
                for (UpdateOp update : gc.updateOpList) {
                    gcMonitor.info(update.toString());
                }
            }

            if (gc.hasGarbage() && phases.start(GCPhase.FULL_GC_CLEANUP)) {
                gc.removeGarbage(phases.stats);
                phases.stop(GCPhase.FULL_GC_CLEANUP);
            }
        }
    }

    public String getFullGCStatsReport() {
        return fullGCStats.toString();
    }

    /**
     * A helper class to remove document for deleted nodes.
     */
    private class DeletedDocsGC implements Closeable {

        private final RevisionVector headRevision;
        private final AtomicBoolean cancel;
        private final List<String> leafDocIdsToDelete = new ArrayList<>();
        private final List<String> resurrectedIds = new ArrayList<>();
        private final StringSort docIdsToDelete;
        private final StringSort prevDocIdsToDelete;
        private final Set<String> exclude = new HashSet<>();
        private boolean sorted = false;
        private final Stopwatch timer;
        @SuppressWarnings("unused")
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
            } else if (all(prevRanges.values(), FIRST_LEVEL::test)) {
                // all previous document ids can be constructed from the
                // previous ranges map. this works for first level previous
                // documents only.
                final Path path = doc.getPath();
                return Iterators.transform(prevRanges.entrySet().iterator(),
                        input -> Utils.getPreviousIdFor(path, input.getKey(), input.getValue().getHeight()));
            } else {
                // need to fetch the previous documents to get their ids
                return Iterators.transform(doc.getAllPreviousDocs(),
                        input -> input.getId());
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
                    input -> !exclude.contains(input));
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
                Map<String, Long> deletionBatch = new LinkedHashMap<>();
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
                    sb.append(String.join(System.getProperty("line.separator"), deletionBatch.keySet()));
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
                    delayOnModifications(timer.stop().elapsed(TimeUnit.MILLISECONDS), cancel, options.delayFactor);
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
                delayOnModifications(timer.stop().elapsed(TimeUnit.MILLISECONDS), cancel, options.delayFactor);
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
                    sb.append(String.join(System.getProperty("line.separator"), deletionBatch));
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

    private static final Predicate<Range> FIRST_LEVEL = input -> input != null && input.height == 0;

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
