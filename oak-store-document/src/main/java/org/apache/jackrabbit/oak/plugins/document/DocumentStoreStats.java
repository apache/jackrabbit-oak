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

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Document Store statistics helper class.
 */
public class DocumentStoreStats implements DocumentStoreStatsCollector, DocumentStoreStatsMBean {
    private final Logger perfLog = LoggerFactory.getLogger(DocumentStoreStats.class.getName() + ".perf");

    public static final int PERF_LOG_THRESHOLD = 1;

    static final String NODES_FIND_CACHED = "DOCUMENT_NODES_FIND_CACHED";
    static final String NODES_FIND_SPLIT = "DOCUMENT_NODES_FIND_SPLIT";
    static final String NODES_FIND_SLAVE = "DOCUMENT_NODES_FIND_SLAVE";
    static final String NODES_FIND_PRIMARY = "DOCUMENT_NODES_FIND_PRIMARY";
    static final String NODES_FIND_MISSING = "DOCUMENT_NODES_FIND_MISSING";
    static final String NODES_FIND_MISSING_TIMER = "DOCUMENT_NODES_FIND_MISSING_TIMER";
    static final String NODES_FIND_TIMER = "DOCUMENT_NODES_FIND";

    static final String NODES_QUERY_FIND_READ_COUNT = "DOCUMENT_NODES_QUERY_FIND";
    static final String NODES_QUERY_FILTER = "DOCUMENT_NODES_QUERY_FILTER";
    static final String NODES_QUERY_TIMER = "DOCUMENT_NODES_QUERY";
    static final String NODES_QUERY_SLAVE = "DOCUMENT_NODES_QUERY_SLAVE";
    static final String NODES_QUERY_PRIMARY = "DOCUMENT_NODES_QUERY_PRIMARY";

    static final String NODES_QUERY_LOCK = "DOCUMENT_NODES_QUERY_LOCK";
    static final String NODES_QUERY_LOCK_TIMER = "DOCUMENT_NODES_QUERY_LOCK_TIMER";

    static final String NODES_CREATE = "DOCUMENT_NODES_CREATE";
    static final String NODES_CREATE_UPSERT = "DOCUMENT_NODES_CREATE_UPSERT";
    static final String NODES_CREATE_SPLIT = "DOCUMENT_NODES_CREATE_SPLIT";
    static final String NODES_CREATE_UPSERT_TIMER = "DOCUMENT_NODES_CREATE_UPSERT_TIMER";
    static final String NODES_CREATE_TIMER = "DOCUMENT_NODES_CREATE_TIMER";
    static final String NODES_UPDATE = "DOCUMENT_NODES_UPDATE";
    static final String NODES_UPDATE_FAILURE = "DOCUMENT_NODES_UPDATE_FAILURE";
    static final String NODES_UPDATE_RETRY_COUNT = "DOCUMENT_NODES_UPDATE_RETRY";
    static final String NODES_UPDATE_TIMER = "DOCUMENT_NODES_UPDATE_TIMER";

    static final String NODES_REMOVE = "DOCUMENT_NODES_REMOVE";
    static final String NODES_REMOVE_TIMER = "DOCUMENT_NODES_REMOVE_TIMER";

    static final String JOURNAL_QUERY = "DOCUMENT_JOURNAL_QUERY";
    static final String JOURNAL_CREATE = "DOCUMENT_JOURNAL_CREATE";
    static final String JOURNAL_QUERY_TIMER = "DOCUMENT_JOURNAL_QUERY_TIMER";
    static final String JOURNAL_CREATE_TIMER = "DOCUMENT_JOURNAL_CREATE_TIMER";


    private final MeterStats findNodesCachedMeter;
    private final MeterStats findNodesMissing;
    private final TimerStats findNodesMissingTimer;
    private final MeterStats findNodesSlave;
    private final TimerStats findNodesTimer;
    private final MeterStats findNodesPrimary;
    private final MeterStats queryNodesSlave;
    private final MeterStats queryNodesPrimary;
    private final MeterStats queryNodesResult;
    private final TimerStats queryNodesWithFilterTimer;
    private final TimerStats queryNodesTimer;
    private final MeterStats queryJournal;
    private final TimerStats queryJournalTimer;
    private final TimerStats createNodeUpsertTimer;
    private final TimerStats createNodeTimer;
    private final TimerStats updateNodeTimer;
    private final MeterStats createNodeUpsertMeter;
    private final MeterStats createNodeMeter;
    private final MeterStats updateNodeMeter;
    private final MeterStats createJournal;
    private final TimerStats createJournalTimer;
    private final MeterStats findSplitNodes;
    private final StatisticsProvider statisticsProvider;
    private final MeterStats queryNodesLock;
    private final TimerStats queryNodesLockTimer;
    private final MeterStats createSplitNodeMeter;
    private final MeterStats updateNodeFailureMeter;
    private final MeterStats updateNodeRetryCountMeter;
    private final MeterStats removeNodes;
    private final TimerStats removeNodesTimer;

    public DocumentStoreStats(StatisticsProvider provider) {
        statisticsProvider = checkNotNull(provider);
        findNodesCachedMeter = provider.getMeter(NODES_FIND_CACHED, StatsOptions.DEFAULT);
        findNodesMissing = provider.getMeter(NODES_FIND_MISSING, StatsOptions.DEFAULT);
        findNodesMissingTimer = provider.getTimer(NODES_FIND_MISSING_TIMER, StatsOptions.METRICS_ONLY);
        findNodesTimer = provider.getTimer(NODES_FIND_TIMER, StatsOptions.METRICS_ONLY);
        findSplitNodes = provider.getMeter(NODES_FIND_SPLIT, StatsOptions.DEFAULT);

        findNodesSlave = provider.getMeter(NODES_FIND_SLAVE, StatsOptions.DEFAULT);
        findNodesPrimary = provider.getMeter(NODES_FIND_PRIMARY, StatsOptions.DEFAULT);

        queryNodesSlave = provider.getMeter(NODES_QUERY_SLAVE, StatsOptions.DEFAULT);
        queryNodesPrimary = provider.getMeter(NODES_QUERY_PRIMARY, StatsOptions.DEFAULT);
        queryNodesResult = provider.getMeter(NODES_QUERY_FIND_READ_COUNT, StatsOptions.DEFAULT);

        queryNodesWithFilterTimer = provider.getTimer(NODES_QUERY_FILTER, StatsOptions.METRICS_ONLY);
        queryNodesTimer = provider.getTimer(NODES_QUERY_TIMER, StatsOptions.METRICS_ONLY);

        queryJournal = provider.getMeter(JOURNAL_QUERY, StatsOptions.DEFAULT);
        queryJournalTimer = provider.getTimer(JOURNAL_QUERY_TIMER, StatsOptions.METRICS_ONLY);
        createJournal = provider.getMeter(JOURNAL_CREATE, StatsOptions.DEFAULT);
        createJournalTimer = provider.getTimer(JOURNAL_CREATE_TIMER, StatsOptions.METRICS_ONLY);

        createNodeUpsertTimer = provider.getTimer(NODES_CREATE_UPSERT_TIMER, StatsOptions.METRICS_ONLY);
        createNodeTimer = provider.getTimer(NODES_CREATE_TIMER, StatsOptions.METRICS_ONLY);
        updateNodeTimer = provider.getTimer(NODES_UPDATE_TIMER, StatsOptions.METRICS_ONLY);
        createNodeMeter = provider.getMeter(NODES_CREATE, StatsOptions.DEFAULT);
        createNodeUpsertMeter = provider.getMeter(NODES_CREATE_UPSERT, StatsOptions.DEFAULT);
        createSplitNodeMeter = provider.getMeter(NODES_CREATE_SPLIT, StatsOptions.DEFAULT);
        updateNodeMeter = provider.getMeter(NODES_UPDATE, StatsOptions.DEFAULT);
        updateNodeFailureMeter = provider.getMeter(NODES_UPDATE_FAILURE, StatsOptions.DEFAULT);
        updateNodeRetryCountMeter = provider.getMeter(NODES_UPDATE_RETRY_COUNT, StatsOptions.DEFAULT);

        queryNodesLock = provider.getMeter(NODES_QUERY_LOCK, StatsOptions.DEFAULT);
        queryNodesLockTimer = provider.getTimer(NODES_QUERY_LOCK_TIMER, StatsOptions.METRICS_ONLY);

        removeNodes = provider.getMeter(NODES_REMOVE, StatsOptions.DEFAULT);
        removeNodesTimer = provider.getTimer(NODES_REMOVE_TIMER, StatsOptions.METRICS_ONLY);
    }

    //~------------------------------------------< DocumentStoreStatsCollector >

    @Override
    public void doneFindCached(Collection<? extends Document> collection, String key) {
        //findCached call is almost done for NODES collection only
        if (collection == Collection.NODES){
            findNodesCachedMeter.mark();
        }
    }

    @Override
    public void doneFindUncached(long timeTakenNanos, Collection<? extends Document> collection, String key,
                                 boolean docFound, boolean isSlaveOk) {
        if (collection == Collection.NODES){
            //For now collect time for reads from primary/secondary in same timer
            TimerStats timer;
            if (docFound) {
                timer = findNodesTimer;
            } else {
                timer = findNodesMissingTimer;
                findNodesMissing.mark();
            }
            timer.update(timeTakenNanos, TimeUnit.NANOSECONDS);

            //For now only nodes can be looked up from slave
            if (isSlaveOk){
                findNodesSlave.mark();
            } else {
                findNodesPrimary.mark();
            }

            if (Utils.isPreviousDocId(key)){
                findSplitNodes.mark();
            }
        }

        perfLog(timeTakenNanos, "findUncached on key={}, isSlaveOk={}", key, isSlaveOk);
    }

    @Override
    public void doneQuery(long timeTakenNanos, Collection<? extends Document> collection, String fromKey, String toKey,
                          boolean indexedProperty, int resultSize, long lockTime, boolean isSlaveOk) {
        if (collection == Collection.NODES){
            //Distinguish between query done with filter and without filter
            TimerStats timer = indexedProperty ? queryNodesWithFilterTimer : queryNodesTimer;
            timer.update(timeTakenNanos, TimeUnit.NANOSECONDS);

            //Number of nodes read
            queryNodesResult.mark(resultSize);

            //Stats for queries to slaves
            if (isSlaveOk){
                queryNodesSlave.mark();
            } else {
                queryNodesPrimary.mark();
            }

            if (lockTime > 0){
                queryNodesLock.mark();
                queryNodesLockTimer.update(lockTime, TimeUnit.NANOSECONDS);
            }

            //TODO What more to gather
            // - Histogram of result - How the number of children vary
        } else if (collection == Collection.JOURNAL){
            //Journals are read from primary and without any extra condition on indexedProperty
            queryJournal.mark(resultSize);
            queryJournalTimer.update(timeTakenNanos, TimeUnit.NANOSECONDS);
        }
        perfLog(timeTakenNanos, "query for children from [{}] to [{}], lock:{}", fromKey, toKey, lockTime);
    }

    @Override
    public void doneCreate(long timeTakenNanos, Collection<? extends Document> collection, List<String> ids, boolean insertSuccess) {
        if (collection == Collection.NODES && insertSuccess){
            for (String id : ids){
                createNodeMeter.mark();
                if (Utils.isPreviousDocId(id)){
                    createSplitNodeMeter.mark();
                }
            }
            createNodeTimer.update(timeTakenNanos / ids.size(), TimeUnit.NANOSECONDS);
        } else if (collection == Collection.JOURNAL){
            createJournal.mark(ids.size());
            createJournalTimer.update(timeTakenNanos, TimeUnit.NANOSECONDS);
        }
        perfLog(timeTakenNanos, "create");
    }

    @Override
    public void doneCreateOrUpdate(long timeTakenNanos,
                                   Collection<? extends Document> collection,
                                   List<String> ids) {
        if (collection == Collection.NODES) {
            for (String id : ids){
                createNodeUpsertMeter.mark();
                if (Utils.isPreviousDocId(id)){
                    createSplitNodeMeter.mark();
                }
            }
            createNodeUpsertTimer.update(timeTakenNanos / ids.size(), TimeUnit.NANOSECONDS);
        }
        perfLog(timeTakenNanos, "createOrUpdate {}", ids);
    }

    @Override
    public void doneFindAndModify(long timeTakenNanos, Collection<? extends Document> collection, String key, boolean newEntry,
                                  boolean success, int retryCount) {
        if (collection == Collection.NODES){
            if (success) {
                if (newEntry) {
                    createNodeUpsertMeter.mark();
                    createNodeUpsertTimer.update(timeTakenNanos, TimeUnit.NANOSECONDS);
                } else {
                    updateNodeMeter.mark();
                    updateNodeTimer.update(timeTakenNanos, TimeUnit.NANOSECONDS);
                }

                if (retryCount > 0){
                    updateNodeRetryCountMeter.mark(retryCount);
                }
            } else {
                updateNodeRetryCountMeter.mark(retryCount);
                updateNodeFailureMeter.mark();
            }
        }
        perfLog(timeTakenNanos, "findAndModify [{}]", key);
    }

    @Override
    public void doneRemove(long timeTakenNanos,
                           Collection<? extends Document> collection,
                           int removeCount) {
        if (collection == Collection.NODES) {
            if (removeCount > 0) {
                removeNodes.mark(removeCount);
                removeNodesTimer.update(timeTakenNanos / removeCount, TimeUnit.NANOSECONDS);
            }
        }
        perfLog(timeTakenNanos, "remove [{}]", removeCount);
    }

    private void perfLog(long timeTakenNanos, String logMessagePrefix, Object... arguments){
        if (!perfLog.isDebugEnabled()){
            return;
        }

        final long diff = TimeUnit.NANOSECONDS.toMillis(timeTakenNanos);
        if (perfLog.isTraceEnabled()) {
            // if log level is TRACE, then always log - and do that on TRACE
            // then:
            perfLog.trace(logMessagePrefix + " [took " + diff + "ms]",
                    (Object[]) arguments);
        } else if (diff > PERF_LOG_THRESHOLD) {
            perfLog.debug(logMessagePrefix + " [took " + diff + "ms]",
                    (Object[]) arguments);
        }
    }

    //~--------------------------------------------< DocumentStoreStatsMBean >

    @Override
    public long getNodesFindCount() {
        return findNodesSlave.getCount() + queryNodesPrimary.getCount();
    }

    @Override
    public long getNodesFindQueryCount() {
        return queryNodesSlave.getCount() + queryNodesPrimary.getCount();
    }

    @Override
    public long getNodesFindMissingCount() {
        return findNodesMissing.getCount();
    }

    @Override
    public long getNodesReadByQueryCount() {
        return queryNodesResult.getCount();
    }

    @Override
    public long getNodesCreateCount() {
        return createNodeMeter.getCount() + createNodeUpsertMeter.getCount();
    }

    @Override
    public long getNodesUpdateCount() {
        return updateNodeMeter.getCount();
    }

    @Override
    public long getNodesRemoveCount() {
        return removeNodes.getCount();
    }

    @Override
    public long getJournalCreateCount() {
        return createJournal.getCount();
    }

    @Override
    public long getJournalReadCount() {
        return queryJournal.getCount();
    }

    @Override
    public CompositeData getFindCachedNodesHistory() {
        return getTimeSeriesData(NODES_FIND_CACHED,
                "Number of find node document calls served from the cache.");
    }

    @Override
    public CompositeData getFindSplitNodesHistory() {
        return getTimeSeriesData(NODES_FIND_SPLIT,
                "Number of un-cached find calls for split document.");
    }

    @Override
    public CompositeData getFindNodesFromPrimaryHistory() {
        return getTimeSeriesData(NODES_FIND_PRIMARY,
                "Number of un-cached find node document calls targeting the primary.");
    }

    @Override
    public CompositeData getFindNodesFromSlaveHistory() {
        return getTimeSeriesData(NODES_FIND_SLAVE,
                "Number of un-cached find node document calls targeting a slave/secondary.");
    }

    @Override
    public CompositeData getFindNodesMissingHistory() {
        return getTimeSeriesData(NODES_FIND_MISSING,
                "Number of un-cached find node document calls that returned no document.");
    }

    @Override
    public CompositeData getQueryNodesFromSlaveHistory() {
        return getTimeSeriesData(NODES_QUERY_SLAVE,
                "Number of queries for node documents targeting a slave/secondary.");
    }

    @Override
    public CompositeData getQueryNodesFromPrimaryHistory() {
        return getTimeSeriesData(NODES_QUERY_PRIMARY,
                "Number of queries for node documents targeting the primary.");
    }

    @Override
    public CompositeData getQueryNodesLockHistory() {
        return getTimeSeriesData(NODES_QUERY_LOCK,
                "Number of queries for node documents done while holding a lock.");
    }

    @Override
    public CompositeData getQueryJournalHistory() {
        return getTimeSeriesData(JOURNAL_QUERY,
                "Number of queries for journal documents.");
    }

    @Override
    public CompositeData getCreateJournalHistory() {
        return getTimeSeriesData(JOURNAL_CREATE,
                "Number of journal documents created.");
    }

    @Override
    public CompositeData getCreateNodesHistory() {
        return getTimeSeriesData(NODES_CREATE,
                "Number of node documents created.");
    }

    @Override
    public CompositeData getUpdateNodesHistory() {
        return getTimeSeriesData(NODES_UPDATE,
                "Number of node documents updated.");
    }

    @Override
    public CompositeData getUpdateNodesRetryHistory() {
        return getTimeSeriesData(NODES_UPDATE_RETRY_COUNT,
                "Number of times a node document update had to be retried.");
    }

    @Override
    public CompositeData getUpdateNodesFailureHistory() {
        return getTimeSeriesData(NODES_UPDATE_FAILURE,
                "Number of times a node document update failed.");
    }

    @Override
    public CompositeData getRemoveNodesHistory() {
        return getTimeSeriesData(NODES_REMOVE,
                "Number of removed node documents.");
    }

    private CompositeData getTimeSeriesData(String name, String desc){
        return TimeSeriesStatsUtil.asCompositeData(getTimeSeries(name), desc);
    }

    private TimeSeries getTimeSeries(String name) {
        return statisticsProvider.getStats().getTimeSeries(name, true);
    }
}
