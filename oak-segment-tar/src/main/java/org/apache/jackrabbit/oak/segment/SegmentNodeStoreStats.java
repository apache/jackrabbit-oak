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

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;

public class SegmentNodeStoreStats implements SegmentNodeStoreStatsMBean, SegmentNodeStoreMonitor {
    private static final boolean COLLECT_STACK_TRACES = Boolean
            .parseBoolean(System.getProperty("oak.commitsTracker.collectStackTraces", "true"));
    private static final int OTHER_WRITERS_LIMIT = Integer.getInteger("oak.commitsTracker.otherWritersLimit", 20);

    public static final String COMMITS_COUNT = "COMMITS_COUNT";
    public static final String COMMIT_QUEUE_SIZE = "COMMIT_QUEUE_SIZE";
    public static final String COMMIT_TIME = "COMMIT_TIME";
    public static final String QUEUEING_TIME = "QUEUEING_TIME";

    private final StatisticsProvider statisticsProvider;
    private final MeterStats commitsCount;
    private final CounterStats commitQueueSize;
    private final TimerStats commitTime;
    private final TimerStats queueingTime;
    
    private volatile CommitsTracker commitsTracker;
    private boolean collectStackTraces = COLLECT_STACK_TRACES;
    private int otherWritersLimit = OTHER_WRITERS_LIMIT;
    private String[] writerGroups;
    
    public SegmentNodeStoreStats(StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
        
        this.commitsTracker = new CommitsTracker(writerGroups, otherWritersLimit, collectStackTraces);
        this.commitsCount = statisticsProvider.getMeter(COMMITS_COUNT, StatsOptions.DEFAULT);
        this.commitQueueSize = statisticsProvider.getCounterStats(COMMIT_QUEUE_SIZE, StatsOptions.DEFAULT);
        this.commitTime = statisticsProvider.getTimer(COMMIT_TIME, StatsOptions.DEFAULT);
        this.queueingTime = statisticsProvider.getTimer(QUEUEING_TIME, StatsOptions.DEFAULT);
    }

    // ~--------------------------------< SegmentStoreMonitor >

    @Override
    public void onCommit(Thread t, long time) {
        commitsCount.mark();
        commitTime.update(time, TimeUnit.NANOSECONDS);
        commitsTracker.trackExecutedCommitOf(t);
    }

    @Override
    public void onCommitQueued(Thread t) {
        commitQueueSize.inc();
        commitsTracker.trackQueuedCommitOf(t);
    }

    @Override
    public void onCommitDequeued(Thread t, long time) {
        commitQueueSize.dec();
        queueingTime.update(time, TimeUnit.NANOSECONDS);
        commitsTracker.trackDequedCommitOf(t);
    }

    // ~--------------------------------< SegmentStoreStatsMBean >

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

    @Override
    public TabularData getCommitsCountPerWriterGroupLastMinute() throws OpenDataException {
        return createTabularDataFromCountMap(commitsTracker.getCommitsCountPerGroupLastMinute(), "commitsPerWriterGroup",
                "writerGroup");
    }
    
    @Override
    public TabularData getCommitsCountForOtherWriters() throws OpenDataException {
        return createTabularDataFromCountMap(commitsTracker.getCommitsCountOthers(), "commitsPerWriter",
                "writerName");
    }
    
    private TabularData createTabularDataFromCountMap(Map<String, Long> commitsCountMap, String typeName,
            String writerDescription) throws OpenDataException {
        CompositeType commitsPerWriterRowType = new CompositeType(typeName, typeName,
                new String[] { "count", writerDescription }, new String[] { "count", writerDescription },
                new OpenType[] { SimpleType.LONG, SimpleType.STRING });

        TabularDataSupport tabularData = new TabularDataSupport(new TabularType(typeName, "Most active writers",
                commitsPerWriterRowType, new String[] { writerDescription }));

        if (commitsCountMap.isEmpty()) {
            commitsCountMap.put("N/A", 0L);
        }

        commitsCountMap.entrySet().stream()
                .sorted(Comparator.<Entry<String, Long>> comparingLong(Entry::getValue).reversed()).map(e -> {
                    Map<String, Object> m = new HashMap<>();
                    m.put("count", e.getValue());
                    m.put(writerDescription, e.getKey());
                    return m;
                }).map(d -> mapToCompositeData(commitsPerWriterRowType, d)).forEach(tabularData::put);

        return tabularData;
    }

    @Override
    public TabularData getQueuedWriters() throws OpenDataException {
        CompositeType queuedWritersDetailsRowType = new CompositeType("queuedWritersDetails", "queuedWritersDetails",
                new String[] { "writerName", "writerDetails" }, new String[] { "writerName", "writerDetails" },
                new OpenType[] { SimpleType.STRING, SimpleType.STRING });

        TabularDataSupport tabularData = new TabularDataSupport(new TabularType("queuedWritersDetails",
                "Queued writers details", queuedWritersDetailsRowType, new String[] { "writerName" }));

        Map<String, String> queuedWritersMap = commitsTracker.getQueuedWritersMap();
        if (queuedWritersMap.isEmpty()) {
            queuedWritersMap.put("N/A", "N/A");
        }
        
        queuedWritersMap.entrySet().stream().map(e -> {
            Map<String, Object> m = new HashMap<>();
            m.put("writerName", e.getKey());
            m.put("writerDetails", e.getValue());
            return m;
        }).map(d -> mapToCompositeData(queuedWritersDetailsRowType, d)).forEach(tabularData::put);

        return tabularData; 
    }

    @Override
    public void setCollectStackTraces(boolean flag) {
        this.collectStackTraces = flag;
        commitsTracker.close();
        commitsTracker = new CommitsTracker(writerGroups, otherWritersLimit, collectStackTraces);
    }
    
    @Override
    public boolean isCollectStackTraces() {
        return collectStackTraces;
    }
    
    @Override
    public int getNumberOfOtherWritersToDetail() {
        return otherWritersLimit;
    }

    @Override
    public void setNumberOfOtherWritersToDetail(int otherWritersLimit) {
        this.otherWritersLimit = otherWritersLimit;
        commitsTracker.close();
        commitsTracker = new CommitsTracker(writerGroups, otherWritersLimit, collectStackTraces);
    }
    
    @Override
    public String[] getWriterGroupsForLastMinuteCounts() {
        return writerGroups;
    }

    @Override
    public void setWriterGroupsForLastMinuteCounts(String[] writerGroups) {
        this.writerGroups = writerGroups;
        commitsTracker.close();
        commitsTracker = new CommitsTracker(writerGroups, otherWritersLimit, collectStackTraces);
    }

    private TimeSeries getTimeSeries(String name) {
        return statisticsProvider.getStats().getTimeSeries(name, true);
    }

    private static CompositeData mapToCompositeData(CompositeType compositeType, Map<String, Object> data) {
        try {
            return new CompositeDataSupport(compositeType, data);
        } catch (OpenDataException | ArrayStoreException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
