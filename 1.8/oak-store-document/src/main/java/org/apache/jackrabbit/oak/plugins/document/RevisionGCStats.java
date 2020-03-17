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

import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.jackrabbit.oak.stats.StatsOptions.DEFAULT;
import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;

/**
 * DocumentNodeStore revision garbage collection statistics.
 */
class RevisionGCStats implements RevisionGCStatsCollector, RevisionGCStatsMBean {

    static final String RGC = "RevisionGC";
    static final String READ_DOC = "READ_DOC";
    static final String DELETE_DOC = "DELETE_DOC";
    static final String DELETE_LEAF_DOC = "DELETE_LEAF_DOC";
    static final String DELETE_SPLIT_DOC = "DELETE_SPLIT_DOC";
    static final String DELETE_INT_SPLIT_DOC = "DELETE_INT_SPLIT_DOC";
    static final String RESET_DELETED_FLAG = "RESET_DELETED_FLAG";

    static final String ACTIVE_TIMER = "ACTIVE_TIMER";
    static final String READ_DOC_TIMER = "READ_DOC_TIMER";
    static final String CHECK_DELETED_TIMER = "CHECK_DELETED_TIMER";
    static final String SORT_IDS_TIMER = "SORT_IDS_TIMER";
    static final String RESET_DELETED_FLAG_TIMER = "RESET_DELETED_FLAG_TIMER";
    static final String DELETE_DOC_TIMER = "DELETE_DOC_TIMER";
    static final String DELETE_SPLIT_DOC_TIMER = "DELETE_SPLIT_DOC_TIMER";

    private final StatisticsProvider provider;

    private final MeterStats readDoc;
    private final MeterStats deletedDoc;
    private final MeterStats deletedLeafDoc;
    private final MeterStats deletedSplitDoc;
    private final MeterStats deletedIntSplitDoc;
    private final MeterStats resetDeletedFlag;

    private final TimerStats activeTimer;
    private final TimerStats readDocTimer;
    private final TimerStats checkDeletedTimer;
    private final TimerStats sortIdsTimer;
    private final TimerStats resetDeletedFlagTimer;
    private final TimerStats deletedDocTimer;
    private final TimerStats deletedSplitDocTimer;

    RevisionGCStats(StatisticsProvider provider) {
        this.provider = provider;

        readDoc = meter(provider, READ_DOC);
        deletedDoc = meter(provider, DELETE_DOC);
        deletedLeafDoc = meter(provider, DELETE_LEAF_DOC);
        deletedSplitDoc = meter(provider, DELETE_SPLIT_DOC);
        deletedIntSplitDoc = meter(provider, DELETE_INT_SPLIT_DOC);
        resetDeletedFlag = meter(provider, RESET_DELETED_FLAG);

        activeTimer = timer(provider, ACTIVE_TIMER);
        readDocTimer = timer(provider, READ_DOC_TIMER);
        checkDeletedTimer = timer(provider, CHECK_DELETED_TIMER);
        sortIdsTimer = timer(provider, SORT_IDS_TIMER);
        resetDeletedFlagTimer = timer(provider, RESET_DELETED_FLAG_TIMER);
        deletedDocTimer = timer(provider, DELETE_DOC_TIMER);
        deletedSplitDocTimer = timer(provider, DELETE_SPLIT_DOC_TIMER);
    }

    //---------------------< RevisionGCStatsCollector >-------------------------

    @Override
    public void documentRead() {
        readDoc.mark();
    }

    @Override
    public void documentsDeleted(long numDocs) {
        deletedDoc.mark(numDocs);

    }

    @Override
    public void leafDocumentsDeleted(long numDocs) {
        deletedDoc.mark(numDocs);
        deletedLeafDoc.mark(numDocs);
    }

    @Override
    public void splitDocumentsDeleted(long numDocs) {
        deletedDoc.mark(numDocs);
        deletedSplitDoc.mark(numDocs);

    }

    @Override
    public void intermediateSplitDocumentsDeleted(long numDocs) {
        deletedIntSplitDoc.mark(numDocs);
    }

    @Override
    public void deletedOnceFlagReset() {
        resetDeletedFlag.mark();
    }

    @Override
    public void finished(VersionGCStats stats) {
        activeTimer.update(stats.active.elapsed(MICROSECONDS), MICROSECONDS);
        readDocTimer.update(stats.collectDeletedDocsElapsed, MICROSECONDS);
        checkDeletedTimer.update(stats.checkDeletedDocsElapsed, MICROSECONDS);
        deletedDocTimer.update(stats.deleteDeletedDocsElapsed, MICROSECONDS);
        deletedSplitDocTimer.update(stats.collectAndDeleteSplitDocsElapsed, MICROSECONDS);
        sortIdsTimer.update(stats.sortDocIdsElapsed, MICROSECONDS);
        resetDeletedFlagTimer.update(stats.updateResurrectedDocumentsElapsed, MICROSECONDS);
    }


    //------------------------< RevisionGCStatsMBean >--------------------------

    @Override
    public long getReadDocCount() {
        return readDoc.getCount();
    }

    @Override
    public long getDeletedDocCount() {
        return deletedDoc.getCount();
    }

    @Override
    public long getDeletedLeafDocCount() {
        return deletedLeafDoc.getCount();
    }

    @Override
    public long getDeletedSplitDocCount() {
        return deletedSplitDoc.getCount();
    }

    @Override
    public long getDeletedIntSplitDocCount() {
        return deletedIntSplitDoc.getCount();
    }

    @Override
    public long getResetDeletedFlagCount() {
        return resetDeletedFlag.getCount();
    }

    @Override
    public CompositeData getReadDocHistory() {
        return getTimeSeriesData(READ_DOC,
                "Documents read by RevisionGC");
    }

    @Override
    public CompositeData getDeletedDocHistory() {
        return getTimeSeriesData(DELETE_DOC,
                "Documents deleted by RevisionGC");
    }

    @Override
    public CompositeData getDeletedLeafDocHistory() {
        return getTimeSeriesData(DELETE_LEAF_DOC,
                "Leaf documents deleted by RevisionGC");
    }

    @Override
    public CompositeData getDeletedSplitDocHistory() {
        return getTimeSeriesData(DELETE_SPLIT_DOC,
                "Split documents deleted by RevisionGC");
    }

    @Override
    public CompositeData getDeletedIntSplitDocHistory() {
        return getTimeSeriesData(DELETE_INT_SPLIT_DOC,
                "Intermediate split documents deleted by RevisionGC");
    }

    @Override
    public CompositeData getResetDeletedFlagHistory() {
        return getTimeSeriesData(RESET_DELETED_FLAG,
                "Deleted once flags reset by RevisionGC");
    }


    //----------------------------< internal >----------------------------------

    private static MeterStats meter(StatisticsProvider provider,
                                    String name) {
        return provider.getMeter(qualifiedName(name), DEFAULT);
    }

    private static TimerStats timer(StatisticsProvider provider,
                                    String name) {
        return provider.getTimer(qualifiedName(name), METRICS_ONLY);
    }

    private static String qualifiedName(String metricName) {
        return RGC + "." + metricName;
    }

    private CompositeData getTimeSeriesData(String name, String desc) {
        return TimeSeriesStatsUtil.asCompositeData(getTimeSeries(name), desc);
    }

    private TimeSeries getTimeSeries(String name) {
        return provider.getStats().getTimeSeries(qualifiedName(name), true);
    }
}
