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

package org.apache.jackrabbit.oak.stats;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.scheduleWithFixedDelay;

import java.util.EnumSet;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.jackrabbit.api.jmx.QueryStatManagerMBean;
import org.apache.jackrabbit.api.stats.RepositoryStatistics.Type;
import org.apache.jackrabbit.oak.api.jmx.RepositoryStatsMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.stats.QueryStatImpl;
import org.apache.jackrabbit.stats.TimeSeriesMax;
import org.apache.jackrabbit.stats.jmx.QueryStatManager;

/**
 * Manager for all repository wide statistics.
 * @see org.apache.jackrabbit.api.stats.RepositoryStatistics
 * @see org.apache.jackrabbit.api.stats.QueryStat
 */
public class StatisticManager {
    private final QueryStatImpl queryStat = new QueryStatImpl();
    private final StatisticsProvider repoStats;
    private final TimeSeriesMax maxQueueLength;
    private final CounterStats maxQueueLengthCounter;
    private final CompositeRegistration registration;

    /**
     * Types for which Metrics based stats would not be collected
     * and only default stats would be collected
     */
    private static final EnumSet<Type> NOOP_METRIC_TYPES = EnumSet.of(
            Type.SESSION_READ_COUNTER,
            Type.SESSION_READ_DURATION,
            Type.SESSION_WRITE_DURATION,
            Type.QUERY_COUNT
    );

    /**
     * Create a new instance of this class registering all repository wide
     * statistics with the passed {@code whiteboard}.
     * @param whiteboard   whiteboard for registering the individual statistics with
     */
    public StatisticManager(Whiteboard whiteboard, ScheduledExecutorService executor) {
        queryStat.setEnabled(true);
        repoStats = getStatsProvider(whiteboard, executor);
        maxQueueLengthCounter = repoStats.getCounterStats(
                RepositoryStats.OBSERVATION_QUEUE_MAX_LENGTH, StatsOptions.METRICS_ONLY);
        maxQueueLength = new TimeSeriesMax(-1) {
            @Override
            public void recordValue(long value) {
                super.recordValue(value);
                long currentValue = maxQueueLengthCounter.getCount();
                maxQueueLengthCounter.inc(Math.max(value, currentValue) - currentValue);
            }
        };
        registration = new CompositeRegistration(
            registerMBean(whiteboard, QueryStatManagerMBean.class, new QueryStatManager(queryStat),
                    "QueryStat", "Oak Query Statistics"),
            registerMBean(whiteboard, RepositoryStatsMBean.class, new RepositoryStats(repoStats.getStats(), maxQueueLength),
                    RepositoryStats.TYPE, "Oak Repository Statistics"),
            scheduleWithFixedDelay(whiteboard, new Runnable() {
                    @Override
                    public void run() {
                        maxQueueLength.recordOneSecond();
                        // reset counter to missing value (-1)
                        maxQueueLengthCounter.dec(maxQueueLengthCounter.getCount() + 1);
                    }
                }, 1));
    }

    /**
     * Logs the call of each query ran on the repository.
     * @param language   the query language
     * @param statement  the query
     * @param millis     time it took to evaluate the query in milli seconds.
     * @see org.apache.jackrabbit.stats.QueryStatCore#logQuery(java.lang.String, java.lang.String, long)
     */
    public void logQueryEvaluationTime(String language, String statement, long millis) {
        queryStat.logQuery(language, statement, millis);
    }

    public MeterStats getMeter(Type type){
        return repoStats.getMeter(type.name(), getOption(type));
    }

    public CounterStats getStatsCounter(Type type){
        return repoStats.getCounterStats(type.name(), getOption(type));
    }

    public TimerStats getTimer(Type type){
        return repoStats.getTimer(type.name(), getOption(type));
    }


    public TimeSeriesMax maxQueLengthRecorder() {
        return maxQueueLength;
    }

    /**
     * Unregister all statistics previously registered with the whiteboard passed
     * to the constructor.
     */
    public void dispose() {
        registration.unregister();
    }

    private StatisticsProvider getStatsProvider(Whiteboard wb,
                                                ScheduledExecutorService executor) {
        StatisticsProvider provider = WhiteboardUtils.getService(wb, StatisticsProvider.class);
        if (provider == null){
            provider = new DefaultStatisticsProvider(executor);
        }
        return provider;
    }

    private static StatsOptions getOption(Type type){
        if (NOOP_METRIC_TYPES.contains(type)){
            return StatsOptions.TIME_SERIES_ONLY;
        }
        return StatsOptions.DEFAULT;
    }

}
