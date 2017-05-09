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

package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.stats.TimeSeriesStatsUtil.asCompositeData;

import java.util.concurrent.TimeUnit;

import javax.management.openmbean.CompositeData;

import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;

public class SegmentNodeStoreStats implements SegmentNodeStoreStatsMBean, SegmentNodeStoreMonitor {
    public static final String COMMITS_COUNT = "COMMITS_COUNT";
    public static final String COMMIT_QUEUE_SIZE = "COMMIT_QUEUE_SIZE";
    public static final String COMMIT_TIME = "COMMIT_TIME";
    public static final String QUEUEING_TIME = "QUEUEING_TIME";
    
    private final StatisticsProvider statisticsProvider;
    private final MeterStats commitsCount;
    private final CounterStats commitQueueSize;
    private final TimerStats commitTime;
    private final TimerStats queueingTime;
    
    public SegmentNodeStoreStats(StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
        this.commitsCount = statisticsProvider.getMeter(COMMITS_COUNT, StatsOptions.DEFAULT);
        this.commitQueueSize = statisticsProvider.getCounterStats(COMMIT_QUEUE_SIZE, StatsOptions.DEFAULT);
        this.commitTime = statisticsProvider.getTimer(COMMIT_TIME, StatsOptions.DEFAULT);
        this.queueingTime = statisticsProvider.getTimer(QUEUEING_TIME, StatsOptions.DEFAULT);
    }

    //~--------------------------------< SegmentStoreMonitor >
    
    @Override
    public void onCommit() {
        commitsCount.mark();
    }
    
    @Override
    public void onCommitQueued() {
        commitQueueSize.inc();
    }
    
    @Override
    public void onCommitDequeued() {
        commitQueueSize.dec();
    }
    

    @Override
    public void committedAfter(long time) {
        commitTime.update(time, TimeUnit.NANOSECONDS);
    }
    
    @Override
    public void dequeuedAfter(long time) {
        queueingTime.update(time, TimeUnit.NANOSECONDS);
    }
    
    //~--------------------------------< SegmentStoreStatsMBean >
    
    @Override
    public CompositeData getCommitsCount() {
        return asCompositeData(getTimeSeries(COMMITS_COUNT), COMMITS_COUNT);
    }

    @Override
    public CompositeData getQueuingCommitsCount() {
        return asCompositeData(getTimeSeries(COMMIT_QUEUE_SIZE), COMMIT_QUEUE_SIZE);
    }
    
    @Override
    public CompositeData getCommitTimes() {
        return asCompositeData(getTimeSeries(COMMIT_TIME), COMMIT_TIME);
    }
    
    @Override
    public CompositeData getQueuingTimes() {
        return asCompositeData(getTimeSeries(QUEUEING_TIME), QUEUEING_TIME);
    }
    
    private TimeSeries getTimeSeries(String name) {
        return statisticsProvider.getStats().getTimeSeries(name, true);
    }

}
