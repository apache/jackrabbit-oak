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

import org.apache.jackrabbit.oak.plugins.document.util.CreateMetricUpdater;
import org.apache.jackrabbit.oak.plugins.document.util.ModifyMetricUpdater;
import org.apache.jackrabbit.oak.plugins.document.util.RemoveMetricUpdater;
import org.apache.jackrabbit.oak.plugins.document.util.UpsertMetricUpdater;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.slf4j.Logger;

import java.util.List;

import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.getCreateStatsConsumer;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.getJournalStatsConsumer;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.getStatsConsumer;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.isNodesCollectionUpdated;
import static org.apache.jackrabbit.oak.plugins.document.util.StatsCollectorUtil.perfLog;
import static org.apache.jackrabbit.oak.stats.StatsOptions.DEFAULT;
import static org.apache.jackrabbit.oak.stats.StatsOptions.METRICS_ONLY;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Throttling statistics helper class.
 */
public class ThrottlingStatsCollectorImpl implements ThrottlingStatsCollector {
    private final Logger perfLog = getLogger(ThrottlingStatsCollectorImpl.class.getName() + ".perf");

    public static final int PERF_LOG_THRESHOLD = 1;

    static final String NODES_CREATE_SPLIT_THROTTLING = "DOCUMENT_NODES_CREATE_SPLIT_THROTTLING";
    static final String NODES_UPDATE_FAILURE_THROTTLING = "DOCUMENT_NODES_UPDATE_FAILURE_THROTTLING";
    static final String NODES_UPDATE_RETRY_COUNT_THROTTLING = "DOCUMENT_NODES_UPDATE_RETRY_THROTTLING";
    static final String NODES_CREATE_UPSERT_THROTTLING = "DOCUMENT_NODES_CREATE_UPSERT_THROTTLING";
    static final String NODES_CREATE_UPSERT_THROTTLING_TIMER = "DOCUMENT_NODES_CREATE_UPSERT_THROTTLING_TIMER";
    static final String NODES_CREATE_THROTTLING = "DOCUMENT_NODES_CREATE_THROTTLING";
    static final String NODES_CREATE_THROTTLING_TIMER = "DOCUMENT_NODES_CREATE_THROTTLING_TIMER";
    static final String NODES_UPDATE_THROTTLING = "DOCUMENT_NODES_UPDATE_THROTTLING";
    static final String NODES_UPDATE_THROTTLING_TIMER = "DOCUMENT_NODES_UPDATE_THROTTLING_TIMER";
    static final String NODES_REMOVE_THROTTLING = "DOCUMENT_NODES_REMOVE_THROTTLING";
    static final String NODES_REMOVE_THROTTLING_TIMER = "DOCUMENT_NODES_REMOVE_THROTTLING_TIMER";
    static final String JOURNAL_CREATE_THROTTLING = "DOCUMENT_JOURNAL_CREATE_THROTTLING";
    static final String JOURNAL_CREATE_THROTTLING_TIMER = "DOCUMENT_JOURNAL_CREATE_THROTTLING_TIMER";


    private final RemoveMetricUpdater removeMetricUpdater;
    private final CreateMetricUpdater createMetricUpdater;
    private final UpsertMetricUpdater upsertMetricUpdater;
    private final ModifyMetricUpdater modifyMetricUpdater;

    public ThrottlingStatsCollectorImpl(StatisticsProvider provider) {

        // metrics for throttling
        MeterStats createSplitNodeThrottlingMeter = provider.getMeter(NODES_CREATE_SPLIT_THROTTLING, DEFAULT);

        MeterStats updateNodeFailureThrottlingMeter = provider.getMeter(NODES_UPDATE_FAILURE_THROTTLING, DEFAULT);
        MeterStats updateNodeRetryCountThrottlingMeter = provider.getMeter(NODES_UPDATE_RETRY_COUNT_THROTTLING, DEFAULT);

        MeterStats createNodeThrottlingMeter = provider.getMeter(NODES_CREATE_THROTTLING, DEFAULT);
        TimerStats createNodeThrottlingTimer = provider.getTimer(NODES_CREATE_THROTTLING_TIMER, METRICS_ONLY);

        MeterStats createNodeUpsertThrottlingMeter = provider.getMeter(NODES_CREATE_UPSERT_THROTTLING, DEFAULT);
        TimerStats createNodeUpsertThrottlingTimer = provider.getTimer(NODES_CREATE_UPSERT_THROTTLING_TIMER, METRICS_ONLY);

        MeterStats updateNodeThrottlingMeter = provider.getMeter(NODES_UPDATE_THROTTLING, DEFAULT);
        TimerStats updateNodeThrottlingTimer = provider.getTimer(NODES_UPDATE_THROTTLING_TIMER, METRICS_ONLY);

        MeterStats createJournalThrottling = provider.getMeter(JOURNAL_CREATE_THROTTLING, DEFAULT);
        TimerStats createJournalThrottlingTimer = provider.getTimer(JOURNAL_CREATE_THROTTLING_TIMER, METRICS_ONLY);

        MeterStats removeNodesThrottling = provider.getMeter(NODES_REMOVE_THROTTLING, DEFAULT);
        TimerStats removeNodesThrottlingTimer = provider.getTimer(NODES_REMOVE_THROTTLING_TIMER, METRICS_ONLY);

        removeMetricUpdater = new RemoveMetricUpdater(removeNodesThrottling, removeNodesThrottlingTimer);

        createMetricUpdater = new CreateMetricUpdater(createNodeThrottlingMeter, createSplitNodeThrottlingMeter, createNodeThrottlingTimer, createJournalThrottling,
                createJournalThrottlingTimer);

        upsertMetricUpdater = new UpsertMetricUpdater(createNodeUpsertThrottlingMeter, createSplitNodeThrottlingMeter, createNodeUpsertThrottlingTimer);

        modifyMetricUpdater = new ModifyMetricUpdater(createNodeUpsertThrottlingMeter, createNodeUpsertThrottlingTimer, updateNodeThrottlingMeter, updateNodeThrottlingTimer,
                updateNodeRetryCountThrottlingMeter, updateNodeFailureThrottlingMeter);
    }

    //~------------------------------------------< ThrottlingStatsCollector >

    @Override
    public void doneCreate(long throttlingTimeNanos, Collection<? extends Document> collection, List<String> ids, boolean insertSuccess) {

        createMetricUpdater.update(collection, throttlingTimeNanos, ids, insertSuccess, isNodesCollectionUpdated(),
                getCreateStatsConsumer(), c -> c == JOURNAL, getJournalStatsConsumer());

        perfLog(perfLog, PERF_LOG_THRESHOLD, throttlingTimeNanos, "create");
    }

    @Override
    public void doneCreateOrUpdate(long throttlingTimeNanos, Collection<? extends Document> collection, List<String> ids) {

        upsertMetricUpdater.update(collection, throttlingTimeNanos, ids, isNodesCollectionUpdated(),
                getCreateStatsConsumer());

        perfLog(perfLog, PERF_LOG_THRESHOLD, throttlingTimeNanos, "createOrUpdate {}", ids);
    }

    @Override
    public void doneFindAndModify(long throttlingTimeNanos, Collection<? extends Document> collection, String key, boolean newEntry,
                                  boolean success, int retryCount) {

        modifyMetricUpdater.update(collection, retryCount, throttlingTimeNanos, success, newEntry, c -> c == NODES,
                getStatsConsumer(), getStatsConsumer(), MeterStats::mark, MeterStats::mark);

        perfLog(perfLog, PERF_LOG_THRESHOLD, throttlingTimeNanos, "findAndModify [{}]", key);
    }

    @Override
    public void doneRemove(long throttlingTimeNanos, Collection<? extends Document> collection) {

        removeMetricUpdater.update(collection, 1, throttlingTimeNanos, isNodesCollectionUpdated(),
                getStatsConsumer());

        perfLog(perfLog, PERF_LOG_THRESHOLD, throttlingTimeNanos, "remove [{}]", 1);
    }
}
