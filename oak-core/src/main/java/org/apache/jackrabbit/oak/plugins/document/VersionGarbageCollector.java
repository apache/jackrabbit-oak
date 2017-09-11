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
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.util.TimeInterval;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.gc.DelegatingGCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.commons.TimeDurationFormatter;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.StandardSystemProperty.LINE_SEPARATOR;
import static com.google.common.collect.Iterables.all;
import static com.google.common.collect.Iterators.partition;
import static com.google.common.util.concurrent.Atomics.newReference;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.COMMIT_ROOT_ONLY;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_LEAF;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType.DEFAULT_NO_BRANCH;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition.newEqualsCondition;
import static org.slf4j.helpers.MessageFormatter.arrayFormat;

public class VersionGarbageCollector {

    //Kept less than MongoDocumentStore.IN_CLAUSE_BATCH_SIZE to avoid re-partitioning
    private static final int DELETE_BATCH_SIZE = 450;
    private static final int UPDATE_BATCH_SIZE = 450;
    private static final int PROGRESS_BATCH_SIZE = 10000;
    private static final Key KEY_MODIFIED = new Key(MODIFIED_IN_SECS, null);
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
    private static final String SETTINGS_COLLECTION_ID = "versionGC";

    /**
     * Property name to timestamp when last gc run happened
     */
    private static final String SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP = "lastOldestTimeStamp";

    /**
     * Property name to recommended time interval for next collection run
     */
    private static final String SETTINGS_COLLECTION_REC_INTERVAL_PROP = "recommendedIntervalMs";

    private final DocumentNodeStore nodeStore;
    private final DocumentStore ds;
    private final VersionGCSupport versionStore;
    private final AtomicReference<GCJob> collector = newReference();
    private VersionGCOptions options;
    private GCMonitor gcMonitor = GCMonitor.EMPTY;
    private RevisionGCStats gcStats = new RevisionGCStats(StatisticsProvider.NOOP);

    VersionGarbageCollector(DocumentNodeStore nodeStore,
                            VersionGCSupport gcSupport) {
        this.nodeStore = nodeStore;
        this.versionStore = gcSupport;
        this.ds = gcSupport.getDocumentStore();
        this.options = new VersionGCOptions();
    }

    void setStatisticsProvider(StatisticsProvider provider) {
        this.gcStats = new RevisionGCStats(provider);
    }

    @Nonnull
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
                gcStats.finished(overall);
                return overall;
            } finally {
                overall.active.stop();
                collector.set(null);
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

    public void setGCMonitor(@Nonnull GCMonitor gcMonitor) {
        this.gcMonitor = checkNotNull(gcMonitor);
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
        Recommendations rec = new Recommendations(maxRevisionAgeInMillis, options);
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
        boolean canceled;
        boolean limitExceeded;
        boolean needRepeat;
        int iterationCount;
        int deletedDocGCCount;
        int deletedLeafDocGCCount;
        int splitDocGCCount;
        int intermediateSplitDocGCCount;
        int updateResurrectedGCCount;
        final TimeDurationFormatter df = TimeDurationFormatter.forLogging();
        final Stopwatch active = Stopwatch.createUnstarted();
        final Stopwatch collectDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch checkDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch deleteDeletedDocs = Stopwatch.createUnstarted();
        final Stopwatch collectAndDeleteSplitDocs = Stopwatch.createUnstarted();
        final Stopwatch sortDocIds = Stopwatch.createUnstarted();
        final Stopwatch updateResurrectedDocuments = Stopwatch.createUnstarted();
        long collectDeletedDocsElapsed, checkDeletedDocsElapsed, deleteDeletedDocsElapsed, collectAndDeleteSplitDocsElapsed,
                sortDocIdsElapsed, updateResurrectedDocumentsElapsed;

        @Override
        public String toString() {
            String timings;
            String fmt = "timeToCollectDeletedDocs=%s, timeToCheckDeletedDocs=%s, timeToSortDocIds=%s, timeTakenToUpdateResurrectedDocs=%s, timeTakenToDeleteDeletedDocs=%s, timeTakenToCollectAndDeleteSplitDocs=%s";

            // aggregated timings?
            if (iterationCount > 0) {
                timings = String.format(fmt, df.format(collectDeletedDocsElapsed, MICROSECONDS),
                        df.format(checkDeletedDocsElapsed, MICROSECONDS), df.format(sortDocIdsElapsed, MICROSECONDS),
                        df.format(updateResurrectedDocumentsElapsed, MICROSECONDS),
                        df.format(deleteDeletedDocsElapsed, MICROSECONDS),
                        df.format(collectAndDeleteSplitDocsElapsed, MICROSECONDS));
            } else {
                timings = String.format(fmt, df.format(collectDeletedDocs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(checkDeletedDocs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(sortDocIds.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(updateResurrectedDocuments.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(deleteDeletedDocs.elapsed(MICROSECONDS), MICROSECONDS),
                        df.format(collectAndDeleteSplitDocs.elapsed(MICROSECONDS), MICROSECONDS));
            }

            return "VersionGCStats{" +
                    "ignoredGCDueToCheckPoint=" + ignoredGCDueToCheckPoint +
                    ", canceled=" + canceled +
                    ", deletedDocGCCount=" + deletedDocGCCount + " (of which leaf: " + deletedLeafDocGCCount + ")" +
                    ", updateResurrectedGCCount=" + updateResurrectedGCCount +
                    ", splitDocGCCount=" + splitDocGCCount +
                    ", intermediateSplitDocGCCount=" + intermediateSplitDocGCCount +
                    ", iterationCount=" + iterationCount +
                    ", timeActive=" + df.format(active.elapsed(MICROSECONDS), MICROSECONDS) +
                    ", " + timings + "}";
        }

        void addRun(VersionGCStats run) {
            ++iterationCount;
            this.ignoredGCDueToCheckPoint = run.ignoredGCDueToCheckPoint;
            this.canceled = run.canceled;
            this.limitExceeded = run.limitExceeded;
            this.needRepeat = run.needRepeat;
            this.deletedDocGCCount += run.deletedDocGCCount;
            this.deletedLeafDocGCCount += run.deletedLeafDocGCCount;
            this.splitDocGCCount += run.splitDocGCCount;
            this.intermediateSplitDocGCCount += run.intermediateSplitDocGCCount;
            this.updateResurrectedGCCount += run.updateResurrectedGCCount;
            if (run.iterationCount > 0) {
                // run is cumulative with times in elapsed fields
                this.collectDeletedDocsElapsed += run.collectDeletedDocsElapsed;
                this.checkDeletedDocsElapsed += run.checkDeletedDocsElapsed;
                this.deleteDeletedDocsElapsed += run.deleteDeletedDocsElapsed;
                this.collectAndDeleteSplitDocsElapsed += run.collectAndDeleteSplitDocsElapsed;
                this.sortDocIdsElapsed += run.sortDocIdsElapsed;
                this.updateResurrectedDocumentsElapsed += run.updateResurrectedDocumentsElapsed;
            } else {
                // single run -> read from stop watches
                this.collectDeletedDocsElapsed += run.collectDeletedDocs.elapsed(MICROSECONDS);
                this.checkDeletedDocsElapsed += run.checkDeletedDocs.elapsed(MICROSECONDS);
                this.deleteDeletedDocsElapsed += run.deleteDeletedDocs.elapsed(MICROSECONDS);
                this.collectAndDeleteSplitDocsElapsed += run.collectAndDeleteSplitDocs.elapsed(MICROSECONDS);
                this.sortDocIdsElapsed += run.sortDocIds.elapsed(MICROSECONDS);
                this.updateResurrectedDocumentsElapsed += run.updateResurrectedDocuments.elapsed(MICROSECONDS);
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
            Recommendations rec = new Recommendations(maxRevisionAgeInMillis, options);
            GCPhases phases = new GCPhases(cancel, stats, gcMonitor);
            try {
                if (rec.ignoreDueToCheckPoint) {
                    phases.stats.ignoredGCDueToCheckPoint = true;
                    monitor.skipped("Checkpoint prevented revision garbage collection");
                    cancel.set(true);
                } else {
                    final RevisionVector headRevision = nodeStore.getHeadRevision();
                    final RevisionVector sweepRevisions = nodeStore.getSweepRevisions();
                    monitor.info("Looking at revisions in {}", rec.scope);

                    collectDeletedDocuments(phases, headRevision, rec);
                    collectSplitDocuments(phases, sweepRevisions, rec);
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

        private void collectSplitDocuments(GCPhases phases,
                                           RevisionVector sweepRevisions,
                                           Recommendations rec) {
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
                                             Recommendations rec)
                throws IOException, LimitExceededException {
            int docsTraversed = 0;
            DeletedDocsGC gc = new DeletedDocsGC(headRevision, cancel, options, monitor);
            try {
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
            } finally {
                gc.close();
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

        public DeletedDocsGC(@Nonnull RevisionVector headRevision,
                             @Nonnull AtomicBoolean cancel,
                             @Nonnull VersionGCOptions options,
                             @Nonnull GCMonitor monitor) {
            this.headRevision = checkNotNull(headRevision);
            this.cancel = checkNotNull(cancel);
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
                    addPreviousDocuments(previousDocs);
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

        private void delayOnModifications(long durationMs) {
            long delayMs = Math.round(durationMs * options.delayFactor);
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

        private Iterator<String> previousDocIdsFor(NodeDocument doc) {
            Map<Revision, Range> prevRanges = doc.getPreviousRanges(true);
            if (prevRanges.isEmpty()) {
                return Iterators.emptyIterator();
            } else if (all(prevRanges.values(), FIRST_LEVEL)) {
                // all previous document ids can be constructed from the
                // previous ranges map. this works for first level previous
                // documents only.
                final String path = doc.getPath();
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

        private void addPreviousDocuments(Iterator<String> ids) throws IOException {
            while (ids.hasNext()) {
                prevDocIdsToDelete.add(ids.next());
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
                Map<String, Map<Key, Condition>> deletionBatch = Maps.newLinkedHashMap();
                for (String s : idListItr.next()) {
                    Map.Entry<String, Long> parsed;
                    try {
                        parsed = parseEntry(s);
                    } catch (IllegalArgumentException e) {
                        monitor.warn("Invalid _modified suffix for {}", s);
                        continue;
                    }
                    deletionBatch.put(parsed.getKey(), singletonMap(KEY_MODIFIED, newEqualsCondition(parsed.getValue())));
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
                    delayOnModifications(timer.stop().elapsed(TimeUnit.MILLISECONDS));
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
                delayOnModifications(timer.stop().elapsed(TimeUnit.MILLISECONDS));
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

    @Nonnull
    private StringSort newStringSort(VersionGCOptions options) {
        return new StringSort(options.overflowToDiskThreshold, NodeDocumentIdComparator.INSTANCE);
    }

    private static final Predicate<Range> FIRST_LEVEL = new Predicate<Range>() {
        @Override
        public boolean apply(@Nullable Range input) {
            return input != null && input.height == 0;
        }
    };

    private class Recommendations {
        final boolean ignoreDueToCheckPoint;
        final TimeInterval scope;
        final long maxCollect;
        final long deleteCandidateCount;
        final long lastOldestTimestamp;

        private final long precisionMs;
        private final long suggestedIntervalMs;
        private final boolean scopeIsComplete;

        /**
         * Gives a recommendation about parameters for the next revision garbage collection run.
         * <p>
         * With the given maximum age of revisions to keep (earliest time in the past to collect),
         * the desired precision in which times shall be sliced and the given limit on the number
         * of collected documents in one run, calculate <ol>
         *     <li>if gc shall run at all (ignoreDueToCheckPoint)</li>
         *     <li>in which time interval documents shall be collected (scope)</li>
         *     <li>if collection should fail if it reaches maxCollect documents, maxCollect will specify
         *     the limit or be 0 if no limit shall be enforced.</li>
         * </ol>
         * After a run, recommendations evaluate the result of the gc to update its persisted recommendations
         * for future runs.
         * <p>
         * In the settings collection, recommendations keeps "revisionsOlderThan" from the last successful run.
         * It also updates the time interval recommended for the next run.
         *
         * @param maxRevisionAgeMs the minimum age for revisions to be collected
         * @param options options for running the gc
         */
        Recommendations(long maxRevisionAgeMs, VersionGCOptions options) {
            TimeInterval keep = new TimeInterval(nodeStore.getClock().getTime() - maxRevisionAgeMs, Long.MAX_VALUE);
            boolean ignoreDueToCheckPoint = false;
            long deletedOnceCount = 0;
            long suggestedIntervalMs;
            long oldestPossible;
            long collectLimit = options.collectLimit;

            Map<String, Long> settings = getLongSettings();
            lastOldestTimestamp = settings.get(SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP);
            if (lastOldestTimestamp == 0) {
                log.debug("No lastOldestTimestamp found, querying for the oldest deletedOnce candidate");
                oldestPossible = versionStore.getOldestDeletedOnceTimestamp(nodeStore.getClock(), options.precisionMs) - 1;
                log.debug("lastOldestTimestamp found: {}", Utils.timestampToString(oldestPossible));
            } else {
                oldestPossible = lastOldestTimestamp - 1;
            }

            TimeInterval scope = new TimeInterval(oldestPossible, Long.MAX_VALUE);
            scope = scope.notLaterThan(keep.fromMs);

            suggestedIntervalMs = settings.get(SETTINGS_COLLECTION_REC_INTERVAL_PROP);
            if (suggestedIntervalMs > 0) {
                suggestedIntervalMs = Math.max(suggestedIntervalMs, options.precisionMs);
                if (suggestedIntervalMs < scope.getDurationMs()) {
                    scope = scope.startAndDuration(suggestedIntervalMs);
                    log.debug("previous runs recommend a {} sec duration, scope now {}",
                            TimeUnit.MILLISECONDS.toSeconds(suggestedIntervalMs), scope);
                }
            } else {
                /* Need to guess. Count the overall number of _deletedOnce documents. If those
                 * are more than we want to collect in a single run, reduce the time scope so
                 * that we likely see a fitting fraction of those documents.
                 */
                try {
                    long preferredLimit = Math.min(collectLimit, (long)Math.ceil(options.overflowToDiskThreshold * 0.95));
                    deletedOnceCount = versionStore.getDeletedOnceCount();
                    if (deletedOnceCount > preferredLimit) {
                        double chunks = ((double) deletedOnceCount) / preferredLimit;
                        suggestedIntervalMs = (long) Math.floor((scope.getDurationMs() + maxRevisionAgeMs) / chunks);
                        if (suggestedIntervalMs < scope.getDurationMs()) {
                            scope = scope.startAndDuration(suggestedIntervalMs);
                            log.debug("deletedOnce candidates: {} found, {} preferred, scope now {}",
                                    deletedOnceCount, preferredLimit, scope);
                        }
                    }
                } catch (UnsupportedOperationException ex) {
                    log.debug("check on upper bounds of delete candidates not supported, skipped");
                }
            }

            //Check for any registered checkpoint which prevent the GC from running
            Revision checkpoint = nodeStore.getCheckpoints().getOldestRevisionToKeep();
            if (checkpoint != null && scope.endsAfter(checkpoint.getTimestamp())) {
                TimeInterval minimalScope = scope.startAndDuration(options.precisionMs);
                if (minimalScope.endsAfter(checkpoint.getTimestamp())) {
                    log.warn("Ignoring RGC run because a valid checkpoint [{}] exists inside minimal scope {}.",
                            checkpoint.toReadableString(), minimalScope);
                    ignoreDueToCheckPoint = true;
                } else {
                    scope = scope.notLaterThan(checkpoint.getTimestamp() - 1);
                    log.debug("checkpoint at [{}] found, scope now {}",
                            Utils.timestampToString(checkpoint.getTimestamp()), scope);
                }
            }

            if (scope.getDurationMs() <= options.precisionMs) {
                // If we have narrowed the collect time interval down as much as we can, no
                // longer enforce a limit. We need to get through this.
                collectLimit = 0;
                log.debug("time interval <= precision ({} ms), disabling collection limits", options.precisionMs);
            }

            this.precisionMs = options.precisionMs;
            this.ignoreDueToCheckPoint = ignoreDueToCheckPoint;
            this.scope = scope;
            this.scopeIsComplete = scope.toMs >= keep.fromMs;
            this.maxCollect = collectLimit;
            this.suggestedIntervalMs = suggestedIntervalMs;
            this.deleteCandidateCount = deletedOnceCount;
        }

        /**
         * Evaluate the results of the last run. Update recommendations for future runs.
         * Will set {@link VersionGCStats#needRepeat} if collection needs to run another
         * iteration for collecting documents up to "now".
         *
         * @param stats the statistics from the last run
         */
        public void evaluate(VersionGCStats stats) {
            if (stats.limitExceeded) {
                // if the limit was exceeded, slash the recommended interval in half.
                long nextDuration = Math.max(precisionMs, scope.getDurationMs() / 2);
                gcMonitor.info("Limit {} documents exceeded, reducing next collection interval to {} seconds",
                        this.maxCollect, TimeUnit.MILLISECONDS.toSeconds(nextDuration));
                setLongSetting(SETTINGS_COLLECTION_REC_INTERVAL_PROP, nextDuration);
                stats.needRepeat = true;
            } else if (!stats.canceled && !stats.ignoredGCDueToCheckPoint) {
                // success, we would not expect to encounter revisions older than this in the future
                setLongSetting(SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP, scope.toMs);

                if (maxCollect <= 0) {
                    log.debug("successful run without effective limit, keeping recommendations");
                } else if (scope.getDurationMs() == suggestedIntervalMs) {
                    int count = stats.deletedDocGCCount - stats.deletedLeafDocGCCount;
                    double used = count / (double) maxCollect;
                    if (used < 0.66) {
                        long nextDuration = (long) Math.ceil(suggestedIntervalMs * 1.5);
                        log.debug("successful run using {}% of limit, raising recommended interval to {} seconds",
                                Math.round(used*1000)/10.0, TimeUnit.MILLISECONDS.toSeconds(nextDuration));
                        setLongSetting(SETTINGS_COLLECTION_REC_INTERVAL_PROP, nextDuration);
                    }
                } else {
                    log.debug("successful run not following recommendations, keeping them");
                }
                stats.needRepeat = !scopeIsComplete;
            }
        }

        private Map<String, Long> getLongSettings() {
            Document versionGCDoc = ds.find(Collection.SETTINGS, SETTINGS_COLLECTION_ID, 0);
            Map<String, Long> settings = Maps.newHashMap();
            // default values
            settings.put(SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP, 0L);
            settings.put(SETTINGS_COLLECTION_REC_INTERVAL_PROP, 0L);
            if (versionGCDoc != null) {
                for (String k : versionGCDoc.keySet()) {
                    Object value = versionGCDoc.get(k);
                    if (value instanceof Number) {
                        settings.put(k, ((Number) value).longValue());
                    }
                }
            }
            return settings;
        }

        private void setLongSetting(String propName, long val) {
            UpdateOp updateOp = new UpdateOp(SETTINGS_COLLECTION_ID, true);
            updateOp.set(propName, val);
            ds.createOrUpdate(Collection.SETTINGS, updateOp);
        }
    }

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
