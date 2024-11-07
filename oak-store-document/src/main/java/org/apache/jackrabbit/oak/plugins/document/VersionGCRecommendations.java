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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.TimeInterval;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Long.MAX_VALUE;
import static java.util.Map.of;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MIN_ID_VALUE;
import static org.apache.jackrabbit.oak.plugins.document.VersionGCRecommendations.GcType.FGC;
import static org.apache.jackrabbit.oak.plugins.document.VersionGCRecommendations.GcType.RGC;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_FULL_GC_DRY_RUN_DOCUMENT_ID_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_FULL_GC_DRY_RUN_TIMESTAMP_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_ID;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_REC_INTERVAL_PROP;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.timestampToString;

/**
 * Gives a recommendation about parameters for the next revision garbage collection run.
 */
public class VersionGCRecommendations {

    private static final Logger log = LoggerFactory.getLogger(VersionGCRecommendations.class);

    private static final long IGNORED_GC_WARNING_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5); // 5min
    private static long lastIgnoreWarning = 0;

    private final VersionGCSupport vgc;
    private final GCMonitor gcmon;

    final boolean ignoreDueToCheckPoint;
    final boolean ignoreFullGCDueToCheckPoint;
    final TimeInterval scope;
    final TimeInterval scopeFullGC;
    final long maxCollect;
    final long deleteCandidateCount;
    final long lastOldestTimestamp;
    final String fullGCId;
    final long originalCollectLimit;
    private final long precisionMs;
    final long suggestedIntervalMs;
    private final boolean scopeIsComplete;
    private final boolean fullGCScopeIsComplete;
    private final boolean fullGCEnabled;

    // whether fullGC is running in dryRun or not. Please note that this mode is to be run only
    // either via command line i.e. oak-run or as management bean command.
    // It will also run only if fullGC is not running.
    private final boolean isFullGCDryRun;

    /**
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
     * @param maxRevisionAgeMs   the minimum age for revisions to be collected
     * @param checkpoints        checkpoints from {@link DocumentNodeStore}
     * @param clock              clock from {@link DocumentNodeStore}
     * @param vgc                VersionGC support class
     * @param options            options for running the gc
     * @param gcMonitor          monitor class for messages
     * @param fullGCEnabled      whether fullGC is enabled or not
     * @param isFullGCDryRun     whether fullGC is running in dryRun mode or not
     */
    VersionGCRecommendations(long maxRevisionAgeMs, Checkpoints checkpoints, boolean checkpointCleanup, Clock clock,
                                    VersionGCSupport vgc, VersionGCOptions options, GCMonitor gcMonitor,
                                    boolean fullGCEnabled, boolean isFullGCDryRun) {
        boolean ignoreDueToCheckPoint;
        boolean ignoreFullGCDueToCheckPoint;
        long deletedOnceCount = 0;
        long suggestedIntervalMs;
        long oldestPossible;
        final AtomicLong oldestModifiedDocTimeStamp = new AtomicLong();
        final AtomicLong oldestModifiedDryRunDocTimeStamp = new AtomicLong();
        String oldestModifiedDocId;
        String oldestModifiedDryRunDocId;
        long collectLimit = options.collectLimit;

        this.vgc = vgc;
        this.gcmon = gcMonitor;
        this.originalCollectLimit = options.collectLimit;
        this.fullGCEnabled = fullGCEnabled;
        this.isFullGCDryRun = isFullGCDryRun;

        TimeInterval keep = new TimeInterval(clock.getTime() - maxRevisionAgeMs, Long.MAX_VALUE);

        Map<String, Object> settings = getVGCSettings();
        lastOldestTimestamp = (long) settings.get(SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP);
        if (lastOldestTimestamp == 0) {
            log.info("No lastOldestTimestamp found, querying for the oldest deletedOnce candidate");
            oldestPossible = vgc.getOldestDeletedOnceTimestamp(clock, options.precisionMs) - 1;
            log.info("lastOldestTimestamp found: {}", Utils.timestampToString(oldestPossible));
        } else {
            oldestPossible = lastOldestTimestamp - 1;
        }

        TimeInterval scope = new TimeInterval(oldestPossible, Long.MAX_VALUE);
        scope = scope.notLaterThan(keep.fromMs);

        final long fullGCTimestamp = (long) settings.get(SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP);
        oldestModifiedDocId = (String) settings.get(SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP);

        final long fullGCDryRunTimestamp = (long) settings.get(SETTINGS_COLLECTION_FULL_GC_DRY_RUN_TIMESTAMP_PROP);
        oldestModifiedDryRunDocId = (String) settings.get(SETTINGS_COLLECTION_FULL_GC_DRY_RUN_DOCUMENT_ID_PROP);

        if (log.isDebugEnabled()) {
            if (isFullGCDryRun) {
                log.debug("lastOldestTimestamp: {}, fullGCDryRunTimestamp: {}, oldestModifiedDryRunDocId: {}",
                        timestampToString(lastOldestTimestamp), timestampToString(fullGCDryRunTimestamp), oldestModifiedDryRunDocId);
            } else {
                log.debug("lastOldestTimestamp: {}, fullGCTimestamp: {}, oldestModifiedDocId: {}",
                        timestampToString(lastOldestTimestamp), timestampToString(fullGCTimestamp), oldestModifiedDocId);
            }
        }

        if (fullGCEnabled && isFullGCDryRun) {
            if (fullGCDryRunTimestamp == 0) {
                // it will only happen for the very first time, we run this fullGC in dry run mode
                log.info("No fullGCDryRunTimestamp found, querying for the oldest modified candidate");
                vgc.getOldestModifiedDoc(clock).ifPresentOrElse(
                        d -> oldestModifiedDryRunDocTimeStamp.set(SECONDS.toMillis(ofNullable(d.getModified()).orElse(0L))),
                        () -> oldestModifiedDryRunDocTimeStamp.set(0L));
                oldestModifiedDryRunDocId = MIN_ID_VALUE;
                log.info("fullGCDryRunTimestamp found: {}", timestampToString(oldestModifiedDryRunDocTimeStamp.get()));
            } else {
                oldestModifiedDryRunDocTimeStamp.set(fullGCDryRunTimestamp);
            }
        } else if (fullGCEnabled) {
            if (fullGCTimestamp == 0) {
                // it will only happen for the very first time, we run this fullGC
                log.info("No fullGCTimestamp found, querying for the oldest modified candidate");
                vgc.getOldestModifiedDoc(clock).ifPresentOrElse(
                        d -> oldestModifiedDocTimeStamp.set(SECONDS.toMillis(ofNullable(d.getModified()).orElse(0L))),
                        () -> oldestModifiedDocTimeStamp.set(0L));
                oldestModifiedDocId = MIN_ID_VALUE;
                log.info("fullGCTimestamp found: {}", timestampToString(oldestModifiedDocTimeStamp.get()));
                // initialize the fullGC database variables i.e. fullGCTimestamp and fullGCId
                setVGCSetting(of(SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP, oldestModifiedDocTimeStamp.get(),
                        SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP, oldestModifiedDocId));
            } else {
                oldestModifiedDocTimeStamp.set(fullGCTimestamp);
            }
        }

        TimeInterval scopeFullGC = new TimeInterval(isFullGCDryRun ? oldestModifiedDryRunDocTimeStamp.get() :
                oldestModifiedDocTimeStamp.get(), MAX_VALUE);
        scopeFullGC = scopeFullGC.notLaterThan(keep.fromMs);

        suggestedIntervalMs = (long) settings.get(SETTINGS_COLLECTION_REC_INTERVAL_PROP);
        if (suggestedIntervalMs > 0) {
            suggestedIntervalMs = Math.max(suggestedIntervalMs, options.precisionMs);
            if (suggestedIntervalMs < scope.getDurationMs()) {
                scope = scope.startAndDuration(suggestedIntervalMs);
                log.debug("previous runs recommend a {} sec duration, scope now {}",
                        TimeUnit.MILLISECONDS.toSeconds(suggestedIntervalMs), scope);
            }
        } else if (scope.getDurationMs() <= options.precisionMs) {
            // the scope is smaller than the minimum precision
            // -> no need to refine the scope
            log.debug("scope <= precision ({} ms)", options.precisionMs);
        } else {
            /* Need to guess. Count the overall number of _deletedOnce documents. If those
             * are more than we want to collect in a single run, reduce the time scope so
             * that we likely see a fitting fraction of those documents.
             */
            try {
                long preferredLimit = Math.min(collectLimit, (long)Math.ceil(options.overflowToDiskThreshold * 0.95));
                deletedOnceCount = vgc.getDeletedOnceCount();
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
        Revision checkpoint = checkpoints.getOldestRevisionToKeep(checkpointCleanup);;

        final GCResult gcResult = getResult(options, checkpoint, clock, RGC, scope);
        scope = gcResult.gcScope;
        ignoreDueToCheckPoint = gcResult.ignoreGC;

        final GCResult fullGCResult = getResult(options, checkpoint, clock, FGC, scopeFullGC);
        scopeFullGC = fullGCResult.gcScope;
        ignoreFullGCDueToCheckPoint = fullGCResult.ignoreGC;

        if (scope.getDurationMs() <= options.precisionMs) {
            // If we have narrowed the collect time interval down as much as we can, no
            // longer enforce a limit. We need to get through this.
            collectLimit = 0;
            log.debug("time interval <= precision ({} ms), disabling collection limits", options.precisionMs);
        }

        this.precisionMs = options.precisionMs;
        this.ignoreDueToCheckPoint = ignoreDueToCheckPoint;
        this.scope = scope;
        this.ignoreFullGCDueToCheckPoint = ignoreFullGCDueToCheckPoint;
        this.scopeFullGC = scopeFullGC;
        this.fullGCId = isFullGCDryRun ? oldestModifiedDryRunDocId : oldestModifiedDocId;
        this.scopeIsComplete = scope.toMs >= keep.fromMs;
        this.fullGCScopeIsComplete = scopeFullGC.toMs >= keep.fromMs;
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
        if (stats.limitExceeded && !isFullGCDryRun) {
            // if the limit was exceeded, slash the recommended interval in half.
            long nextDuration = Math.max(precisionMs, scope.getDurationMs() / 2);
            gcmon.info("Limit {} documents exceeded, reducing next collection interval to {} seconds",
                    this.maxCollect, TimeUnit.MILLISECONDS.toSeconds(nextDuration));
            setVGCSetting(VersionGarbageCollector.SETTINGS_COLLECTION_REC_INTERVAL_PROP, nextDuration);
            stats.needRepeat = true;
        } else if (!stats.canceled && !stats.ignoredGCDueToCheckPoint && !isFullGCDryRun) {
            // success, we would not expect to encounter revisions older than this in the future
            setVGCSetting(SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP, scope.toMs);

            final Map<String, Object> updateVGCMap = new HashMap<>();
            updateVGCMap.put(SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP, stats.oldestModifiedDocTimeStamp);
            updateVGCMap.put(SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP, stats.oldestModifiedDocId);
            updateVGCSetting(updateVGCMap);

            int count = stats.deletedDocGCCount - stats.deletedLeafDocGCCount;
            double usedFraction;
            double allowedFraction = 0.66;

            if (maxCollect <= 0) {
                usedFraction = count / (double) this.originalCollectLimit;
            } else {
                usedFraction = count / (double) maxCollect;
            }

            if (scope.getDurationMs() == suggestedIntervalMs) {
                if (usedFraction < allowedFraction) {
                    long nextDuration = (long) Math.ceil(suggestedIntervalMs * 1.5);
                    log.debug("successful run using {}% of limit, raising recommended interval to {} seconds",
                            Math.round(usedFraction * 1000) / 10.0, TimeUnit.MILLISECONDS.toSeconds(nextDuration));
                    setVGCSetting(VersionGarbageCollector.SETTINGS_COLLECTION_REC_INTERVAL_PROP, nextDuration);
                } else {
                    log.debug("not increasing limit: collected {} documents ({}% >= {}% limit)", count, usedFraction,
                            allowedFraction);
                }
            } else {
                log.debug("successful run not following recommendations, keeping them");
            }
            stats.needRepeat = !scopeIsComplete;
        }

        // save data for full GC
        if (fullGCEnabled && !stats.canceled && !stats.ignoredFullGCDueToCheckPoint) {
            // success, we would not expect to encounter revisions older than this in the future
            if (isFullGCDryRun) {
                setVGCSetting(of(SETTINGS_COLLECTION_FULL_GC_DRY_RUN_TIMESTAMP_PROP, stats.oldestModifiedDocTimeStamp,
                        SETTINGS_COLLECTION_FULL_GC_DRY_RUN_DOCUMENT_ID_PROP, stats.oldestModifiedDocId));
            } else {
                updateVGCSetting(of(SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP, stats.oldestModifiedDocTimeStamp,
                        SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP, stats.oldestModifiedDocId));
            }

            final long scopeEnd = scopeFullGC.toMs;
            final long actualEnd = stats.oldestModifiedDocTimeStamp;
            if (actualEnd < scopeEnd) {
                stats.needRepeat = true;
            } else {
                stats.needRepeat |= !fullGCScopeIsComplete;
            }
        }
    }

    private Map<String, Object> getVGCSettings() {
        Document versionGCDoc = vgc.getDocumentStore().find(Collection.SETTINGS, SETTINGS_COLLECTION_ID, 0);
        Map<String, Object> settings = new HashMap<>();
        // default values
        settings.put(SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP, 0L);
        settings.put(SETTINGS_COLLECTION_REC_INTERVAL_PROP, 0L);
        settings.put(SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP, 0L);
        settings.put(SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP, MIN_ID_VALUE);
        settings.put(SETTINGS_COLLECTION_FULL_GC_DRY_RUN_TIMESTAMP_PROP, 0L);
        settings.put(SETTINGS_COLLECTION_FULL_GC_DRY_RUN_DOCUMENT_ID_PROP, MIN_ID_VALUE);
        if (versionGCDoc != null) {
            for (String k : versionGCDoc.keySet()) {
                Object value = versionGCDoc.get(k);
                if (value instanceof Number) {
                    settings.put(k, ((Number) value).longValue());
                }
                if (value instanceof String) {
                    settings.put(k, value);
                }
            }
        }
        return settings;
    }

    /**
     * Set the VGC settings with the given property and value.
     * If property is not present, it will add the property to versionGC document with given value.
     *
     * @param propName the property name
     * @param val the value
     * @see VersionGCRecommendations#setVGCSetting(Map)
     */
    private void setVGCSetting(final String propName, final Object val) {
        final Map<String, Object> vgcMap = new HashMap<>();
        vgcMap.put(propName, val);
        setVGCSetting(vgcMap);
    }

    /**
     * Set the VGC settings with the given properties and values.
     * If properties are not present, it will add the properties to versionGC document with given values
     * .
     * @param propValMap the properties and values to set
     */
    private void setVGCSetting(final Map<String, Object> propValMap) {
        final UpdateOp updateOp = new UpdateOp(SETTINGS_COLLECTION_ID, true);
        setUpdateOp(propValMap, updateOp);
        vgc.getDocumentStore().createOrUpdate(Collection.SETTINGS, updateOp);
    }

    private void setUpdateOp(final Map<String, Object> propValMap, final UpdateOp updateOp) {
        propValMap.forEach((k, v) -> {
            if (v instanceof Number) updateOp.set(k, ((Number) v).longValue());
            if (v instanceof String) updateOp.set(k, (String) v);
            if (v instanceof Boolean) updateOp.set(k, (Boolean) v);
        });
    }

    /**
     * Update the VGC settings with the given properties and values.
     * Properties are only updated if they already exists in the versionGC document.
     *
     * @param propValMap the properties and values to update
     */
    private void updateVGCSetting(final Map<String, Object> propValMap) {
        final UpdateOp updateOp = new UpdateOp(SETTINGS_COLLECTION_ID, false);
        setUpdateOp(propValMap, updateOp);
        propValMap.forEach((k, v) -> updateOp.contains(k, true));
        vgc.getDocumentStore().findAndUpdate(Collection.SETTINGS, updateOp);
    }

    @NotNull
    private static GCResult getResult(final VersionGCOptions options, final Revision checkpoint, final Clock clock, final GcType gcType,
                                      TimeInterval gcScope) {
        boolean ignoreGC = false;
        if (checkpoint != null && gcScope.endsAfter(checkpoint.getTimestamp())) {
            TimeInterval minimalScope = gcScope.startAndDuration(options.precisionMs);
            if (minimalScope.endsAfter(checkpoint.getTimestamp())) {
                final long now = clock.getTime();
                if (now - lastIgnoreWarning > IGNORED_GC_WARNING_INTERVAL_MS) {
                    log.warn("Ignoring [{}] run because a valid checkpoint [{}] exists inside minimal scope {}.", gcType, checkpoint.toReadableString(), minimalScope);
                    lastIgnoreWarning = now;
                }
                ignoreGC = true;
            } else {
                gcScope = gcScope.notLaterThan(checkpoint.getTimestamp() - 1);
                log.debug("checkpoint at [{}] found, [{}] Scope now {}", timestampToString(checkpoint.getTimestamp()), gcType, gcScope);
            }
        }
        return new GCResult(ignoreGC, gcScope);
    }

    private static class GCResult {
        public final boolean ignoreGC;
        public final TimeInterval gcScope;

        public GCResult(boolean ignoreGC, TimeInterval gcScope) {
            this.ignoreGC = ignoreGC;
            this.gcScope = gcScope;
        }
    }

    enum GcType {
        RGC("Revision GC"), // revision GC
        FGC("Full GC"); // full GC

        private final String name;

        GcType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }
}
