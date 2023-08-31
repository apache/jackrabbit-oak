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

import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.TimerStats;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.jackrabbit.oak.stats.StatsOptions.DEFAULT;
import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;

/**
 * {@link DocumentNodeStore} Detailed revision garbage collection statistics.
 */
class DetailedRevisionGCStatsCollectorImpl implements DetailedRevisionGCStatsCollector {

    static final String DETAILED_GC = "DetailedGC";
    static final String READ_DOC = "READ_DOC";
    static final String DELETED_PROPERTY = "DELETED_PROPERTY";
    static final String UPDATED_DOC = "UPDATED_DOC";
    static final String SKIPPED_DOC = "SKIPPED_DOC";
    static final String DETAILED_GC_ACTIVE_TIMER = "DETAILED_GC_ACTIVE_TIMER";
    static final String DETAILED_GC_TIMER = "DETAILED_GC_TIMER";
    static final String COLLECT_DETAILED_GARBAGE_TIMER = "COLLECT_DETAILED_GARBAGE_TIMER";
    static final String COLLECT_DELETED_PROPS_TIMER = "COLLECT_DELETED_PROPS_TIMER";
    static final String COLLECT_DELETED_OLD_REVS_TIMER = "COLLECT_DELETED_OLD_REVS_TIMER";
    static final String COLLECT_UNMERGED_BC_TIMER = "COLLECT_UNMERGED_BC_TIMER";
    static final String DELETE_DETAILED_GC_DOCS_TIMER = "DELETE_DETAILED_GC_DOCS_TIMER";

    static final String COUNTER = "COUNTER";
    static final String FAILURE_COUNTER = "FAILURE";

    private final MeterStats readDoc;
    private final MeterStats deletedProperty;
    private final MeterStats updatedDoc;
    private final MeterStats skippedDoc;
    private final TimerStats detailedGCActiveTimer;
    private final TimerStats detailedGCTimer;
    private final TimerStats collectDetailedGarbageTimer;
    private final TimerStats collectDeletedPropsTimer;
    private final TimerStats collectDeletedOldRevsTimer;
    private final TimerStats collectUnmergedBCTimer;
    private final TimerStats deleteDetailedGCDocsTimer;

    private final CounterStats counter;
    private final CounterStats failureCounter;

    DetailedRevisionGCStatsCollectorImpl(StatisticsProvider provider) {

        readDoc = meter(provider, READ_DOC);
        deletedProperty = meter(provider, DELETED_PROPERTY);
        updatedDoc = meter(provider, UPDATED_DOC);
        skippedDoc = meter(provider, SKIPPED_DOC);

        detailedGCActiveTimer = timer(provider, DETAILED_GC_ACTIVE_TIMER);
        detailedGCTimer = timer(provider, DETAILED_GC_TIMER);
        collectDetailedGarbageTimer = timer(provider, COLLECT_DETAILED_GARBAGE_TIMER);
        collectDeletedPropsTimer = timer(provider, COLLECT_DELETED_PROPS_TIMER);
        collectDeletedOldRevsTimer = timer(provider, COLLECT_DELETED_OLD_REVS_TIMER);
        collectUnmergedBCTimer = timer(provider, COLLECT_UNMERGED_BC_TIMER);
        deleteDetailedGCDocsTimer = timer(provider, DELETE_DETAILED_GC_DOCS_TIMER);

        counter = counter(provider, COUNTER);
        failureCounter = counter(provider, FAILURE_COUNTER);
    }

    //---------------------< DetailedRevisionGCStatsCollector >-------------------------

    @Override
    public void documentRead() {
        readDoc.mark();
    }

    @Override
    public void propertiesDeleted(long numProps) {
        deletedProperty.mark(numProps);
    }

    @Override
    public void documentsUpdated(long numDocs) {
        updatedDoc.mark(numDocs);
    }

    @Override
    public void documentsSkippedUpdation(long numDocs) {
        skippedDoc.mark(numDocs);
    }

    @Override
    public void started() {
        counter.inc();
    }

    @Override
    public void finished(VersionGCStats stats) {
        detailedGCActiveTimer.update(stats.detailedGCActiveElapsed, MICROSECONDS);
        detailedGCTimer.update(stats.detailedGCDocsElapsed, MICROSECONDS);
        collectDetailedGarbageTimer.update(stats.collectDetailedGarbageElapsed, MICROSECONDS);
        collectDeletedPropsTimer.update(stats.collectDeletedPropsElapsed, MICROSECONDS);
        collectDeletedOldRevsTimer.update(stats.collectDeletedOldRevsElapsed, MICROSECONDS);
        collectUnmergedBCTimer.update(stats.collectUnmergedBCElapsed, MICROSECONDS);
        deleteDetailedGCDocsTimer.update(stats.deleteDetailedGCDocsElapsed, MICROSECONDS);
        if (!stats.success) {
            failureCounter.inc();
        }
    }


    //----------------------------< internal >----------------------------------

    private static MeterStats meter(StatisticsProvider provider, String name) {
        return provider.getMeter(qualifiedName(name), DEFAULT);
    }

    private static TimerStats timer(StatisticsProvider provider, String name) {
        return provider.getTimer(qualifiedName(name), METRICS_ONLY);
    }

    private static CounterStats counter(StatisticsProvider provider, String name) {
        return provider.getCounterStats(qualifiedName(name), METRICS_ONLY);
    }

    private static String qualifiedName(String metricName) {
        return DETAILED_GC + "." + metricName;
    }

}
