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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.TimeInterval;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;

import com.google.common.collect.Maps;

/**
 * Gives a recommendation about parameters for the next revision garbage collection run.
 */
public class VersionGCRecommendations {

    private final VersionGCSupport vgc;
    private final GCMonitor gcmon;

    final boolean ignoreDueToCheckPoint;
    final TimeInterval scope;
    final long maxCollect;
    final long deleteCandidateCount;
    final long lastOldestTimestamp;

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
     * @param dns DocumentNodeStore to use
     * @param vgc VersionGC support class
     * @param options options for running the gc
     * @param gcMonitor monitor class for messages
     */
    public VersionGCRecommendations(long maxRevisionAgeMs, DocumentNodeStore dns, VersionGCSupport vgc,
            VersionGCOptions options, GCMonitor gcMonitor) {
        this.vgc = vgc;
        this.gcmon = gcMonitor;

        TimeInterval keep = new TimeInterval(dns.getClock().getTime() - maxRevisionAgeMs, Long.MAX_VALUE);
        boolean ignoreDueToCheckPoint = false;
        long deletedOnceCount = 0;
        long suggestedIntervalMs;
        long oldestPossible;
        long collectLimit = options.collectLimit;

        Map<String, Long> settings = getLongSettings();
        lastOldestTimestamp = settings.get(VersionGarbageCollector.SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP);
        if (lastOldestTimestamp == 0) {
            VersionGarbageCollector.log.debug("No lastOldestTimestamp found, querying for the oldest deletedOnce candidate");
            oldestPossible = vgc.getOldestDeletedOnceTimestamp(dns.getClock(), options.precisionMs) - 1;
            VersionGarbageCollector.log.debug("lastOldestTimestamp found: {}", Utils.timestampToString(oldestPossible));
        } else {
            oldestPossible = lastOldestTimestamp - 1;
        }

        TimeInterval scope = new TimeInterval(oldestPossible, Long.MAX_VALUE);
        scope = scope.notLaterThan(keep.fromMs);

        suggestedIntervalMs = settings.get(VersionGarbageCollector.SETTINGS_COLLECTION_REC_INTERVAL_PROP);
        if (suggestedIntervalMs > 0) {
            suggestedIntervalMs = Math.max(suggestedIntervalMs, options.precisionMs);
            if (suggestedIntervalMs < scope.getDurationMs()) {
                scope = scope.startAndDuration(suggestedIntervalMs);
                VersionGarbageCollector.log.debug("previous runs recommend a {} sec duration, scope now {}",
                        TimeUnit.MILLISECONDS.toSeconds(suggestedIntervalMs), scope);
            }
        } else if (scope.getDurationMs() <= options.precisionMs) {
            // the scope is smaller than the minimum precision
            // -> no need to refine the scope
            VersionGarbageCollector.log.debug("scope <= precision ({} ms)", options.precisionMs);
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
                        VersionGarbageCollector.log.debug("deletedOnce candidates: {} found, {} preferred, scope now {}",
                                deletedOnceCount, preferredLimit, scope);
                    }
                }
            } catch (UnsupportedOperationException ex) {
                VersionGarbageCollector.log.debug("check on upper bounds of delete candidates not supported, skipped");
            }
        }

        //Check for any registered checkpoint which prevent the GC from running
        Revision checkpoint = dns.getCheckpoints().getOldestRevisionToKeep();
        if (checkpoint != null && scope.endsAfter(checkpoint.getTimestamp())) {
            TimeInterval minimalScope = scope.startAndDuration(options.precisionMs);
            if (minimalScope.endsAfter(checkpoint.getTimestamp())) {
                VersionGarbageCollector.log.warn("Ignoring RGC run because a valid checkpoint [{}] exists inside minimal scope {}.",
                        checkpoint.toReadableString(), minimalScope);
                ignoreDueToCheckPoint = true;
            } else {
                scope = scope.notLaterThan(checkpoint.getTimestamp() - 1);
                VersionGarbageCollector.log.debug("checkpoint at [{}] found, scope now {}",
                        Utils.timestampToString(checkpoint.getTimestamp()), scope);
            }
        }

        if (scope.getDurationMs() <= options.precisionMs) {
            // If we have narrowed the collect time interval down as much as we can, no
            // longer enforce a limit. We need to get through this.
            collectLimit = 0;
            VersionGarbageCollector.log.debug("time interval <= precision ({} ms), disabling collection limits", options.precisionMs);
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
            gcmon.info("Limit {} documents exceeded, reducing next collection interval to {} seconds",
                    this.maxCollect, TimeUnit.MILLISECONDS.toSeconds(nextDuration));
            setLongSetting(VersionGarbageCollector.SETTINGS_COLLECTION_REC_INTERVAL_PROP, nextDuration);
            stats.needRepeat = true;
        } else if (!stats.canceled && !stats.ignoredGCDueToCheckPoint) {
            // success, we would not expect to encounter revisions older than this in the future
            setLongSetting(VersionGarbageCollector.SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP, scope.toMs);

            if (maxCollect <= 0) {
                VersionGarbageCollector.log.debug("successful run without effective limit, keeping recommendations");
            } else if (scope.getDurationMs() == suggestedIntervalMs) {
                int count = stats.deletedDocGCCount - stats.deletedLeafDocGCCount;
                double used = count / (double) maxCollect;
                if (used < 0.66) {
                    long nextDuration = (long) Math.ceil(suggestedIntervalMs * 1.5);
                    VersionGarbageCollector.log.debug("successful run using {}% of limit, raising recommended interval to {} seconds",
                            Math.round(used*1000)/10.0, TimeUnit.MILLISECONDS.toSeconds(nextDuration));
                    setLongSetting(VersionGarbageCollector.SETTINGS_COLLECTION_REC_INTERVAL_PROP, nextDuration);
                }
            } else {
                VersionGarbageCollector.log.debug("successful run not following recommendations, keeping them");
            }
            stats.needRepeat = !scopeIsComplete;
        }
    }

    private Map<String, Long> getLongSettings() {
        Document versionGCDoc = vgc.getDocumentStore().find(Collection.SETTINGS, VersionGarbageCollector.SETTINGS_COLLECTION_ID, 0);
        Map<String, Long> settings = Maps.newHashMap();
        // default values
        settings.put(VersionGarbageCollector.SETTINGS_COLLECTION_OLDEST_TIMESTAMP_PROP, 0L);
        settings.put(VersionGarbageCollector.SETTINGS_COLLECTION_REC_INTERVAL_PROP, 0L);
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
        UpdateOp updateOp = new UpdateOp(VersionGarbageCollector.SETTINGS_COLLECTION_ID, true);
        updateOp.set(propName, val);
        vgc.getDocumentStore().createOrUpdate(Collection.SETTINGS, updateOp);
    }
}