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

import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.TimeInterval;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Long.MAX_VALUE;
import static java.util.Map.of;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MIN_ID_VALUE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.NULL;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_ID;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_REC_INTERVAL_PROP;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.timestampToString;

/**
 * Gives a recommendation about parameters for the next revision garbage collection run.
 */
public class VersionGCRecommendations {

    private static final Logger log = LoggerFactory.getLogger(VersionGCRecommendations.class);

    private final VersionGCSupport vgc;
    private final GCMonitor gcmon;

    final boolean ignoreDueToCheckPoint;
    final TimeInterval scope;
    final TimeInterval scopeDetailedGC;
    final long maxCollect;
    final long deleteCandidateCount;
    final long lastOldestTimestamp;
    final long detailedGCTimestamp;
    final String detailedGCId;
    final long originalCollectLimit;
    private final long precisionMs;
    final long suggestedIntervalMs;
    private final boolean scopeIsComplete;

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
     * @param maxRevisionAgeMs the minimum age for revisions to be collected
     * @param checkpoints checkpoints from {@link DocumentNodeStore}
     * @param clock clock from {@link DocumentNodeStore}
     * @param vgc VersionGC support class
     * @param options options for running the gc
     * @param gcMonitor monitor class for messages
     */
    public VersionGCRecommendations(long maxRevisionAgeMs, Checkpoints checkpoints, Clock clock, VersionGCSupport vgc,
            VersionGCOptions options, GCMonitor gcMonitor) {
        boolean ignoreDueToCheckPoint = false;
        long deletedOnceCount = 0;
        long suggestedIntervalMs;
        long oldestPossible;
        long oldestModifiedDocTimeStamp;
        String oldestModifiedDocId;
        long collectLimit = options.collectLimit;

        this.vgc = vgc;
        this.gcmon = gcMonitor;
        this.originalCollectLimit = options.collectLimit;

        TimeInterval keep = new TimeInterval(clock.getTime() - maxRevisionAgeMs, Long.MAX_VALUE);

        Map<String, Object> settings = getVGCSettings();
        lastOldestTimestamp = (long) settings.get(SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP);
        if (lastOldestTimestamp == 0) {
            log.info("No lastOldestTimestamp found, querying for the oldest deletedOnce candidate");
            oldestPossible = vgc.getOldestDeletedOnceTimestamp(clock, options.precisionMs) - 1;
            log.info("lastOldestTimestamp found: {}", timestampToString(oldestPossible));
        } else {
            oldestPossible = lastOldestTimestamp - 1;
        }

        TimeInterval scope = new TimeInterval(oldestPossible, Long.MAX_VALUE);
        scope = scope.notLaterThan(keep.fromMs);

        detailedGCTimestamp = (long) settings.get(SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP);
        oldestModifiedDocId = (String) settings.get(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP);
        if (detailedGCTimestamp == 0) {
            // it will only happens for the very first time, we run this detailedGC
            log.info("No detailedGCTimestamp found, querying for the oldest modified candidate");
            final NodeDocument doc = vgc.getOldestModifiedDoc(clock);
            if (doc == NULL) {
                oldestModifiedDocTimeStamp = 0L;
            } else {
                oldestModifiedDocTimeStamp = doc.getModified() == null ? 0L : doc.getModified() - 1;
            }
            oldestModifiedDocId = MIN_ID_VALUE;
            log.info("detailedGCTimestamp found: {}", timestampToString(oldestModifiedDocTimeStamp));
        } else {
            oldestModifiedDocTimeStamp = detailedGCTimestamp - 1;
        }

        TimeInterval detailedGCTimeInternal = new TimeInterval(oldestModifiedDocTimeStamp, MAX_VALUE);
        detailedGCTimeInternal = detailedGCTimeInternal.notLaterThan(keep.fromMs);

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
        Revision checkpoint = checkpoints.getOldestRevisionToKeep();
        if (checkpoint != null && scope.endsAfter(checkpoint.getTimestamp())) {
            TimeInterval minimalScope = scope.startAndDuration(options.precisionMs);
            if (minimalScope.endsAfter(checkpoint.getTimestamp())) {
                log.warn("Ignoring RGC run because a valid checkpoint [{}] exists inside minimal scope {}.",
                        checkpoint.toReadableString(), minimalScope);
                ignoreDueToCheckPoint = true;
            } else {
                scope = scope.notLaterThan(checkpoint.getTimestamp() - 1);
                detailedGCTimeInternal = detailedGCTimeInternal.notLaterThan(checkpoint.getTimestamp() - 1);
                log.info("checkpoint at [{}] found, scope now {}, detailedGcScope now {}", timestampToString(checkpoint.getTimestamp()), scope, detailedGCTimeInternal);
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
        this.scopeDetailedGC = detailedGCTimeInternal;
        this.detailedGCId = oldestModifiedDocId;
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
            gcmon.info("Limit {} documents exceeded, reducing next collection interval to {} seconds",
                    this.maxCollect, TimeUnit.MILLISECONDS.toSeconds(nextDuration));
            setLongSetting(VersionGarbageCollector.SETTINGS_COLLECTION_REC_INTERVAL_PROP, nextDuration);
            stats.needRepeat = true;
        } else if (!stats.canceled && !stats.ignoredGCDueToCheckPoint) {
            // success, we would not expect to encounter revisions older than this in the future
            setLongSetting(of(SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP, scope.toMs,
                    SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP, stats.oldestModifiedDocTimeStamp));
            setStringSetting(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP, stats.oldestModifiedDocId);

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
                    setLongSetting(SETTINGS_COLLECTION_REC_INTERVAL_PROP, nextDuration);
                } else {
                    log.debug("not increasing limit: collected {} documents ({}% >= {}% limit)", count, usedFraction,
                            allowedFraction);
                }
            } else {
                log.debug("successful run not following recommendations, keeping them");
            }
            stats.needRepeat = !scopeIsComplete;
        }
    }

    private Map<String, Object> getVGCSettings() {
        Document versionGCDoc = vgc.getDocumentStore().find(Collection.SETTINGS, SETTINGS_COLLECTION_ID, 0);
        Map<String, Object> settings = new HashMap<>();
        // default values
        settings.put(SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP, 0L);
        settings.put(SETTINGS_COLLECTION_REC_INTERVAL_PROP, 0L);
        settings.put(SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP, 0L);
        settings.put(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP, MIN_ID_VALUE);
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

    private void setLongSetting(String propName, long val) {
        setLongSetting(of(propName, val));
    }

    private void setStringSetting(String propName, String val) {
        UpdateOp updateOp = new UpdateOp(SETTINGS_COLLECTION_ID, true);
        updateOp.set(propName, val);
        vgc.getDocumentStore().createOrUpdate(Collection.SETTINGS, updateOp);
    }

    private void setLongSetting(final Map<String, Long> propValMap) {
        UpdateOp updateOp = new UpdateOp(SETTINGS_COLLECTION_ID, true);
        propValMap.forEach(updateOp::set);
        vgc.getDocumentStore().createOrUpdate(Collection.SETTINGS, updateOp);
    }
}