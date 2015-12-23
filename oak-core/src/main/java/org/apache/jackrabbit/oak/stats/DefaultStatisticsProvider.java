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

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.api.stats.RepositoryStatistics.Type;
import org.apache.jackrabbit.stats.RepositoryStatisticsImpl;

public final class DefaultStatisticsProvider implements StatisticsProvider {
    private final RepositoryStatisticsImpl repoStats;
    private final Map<String, SimpleStats> statsMeters = Maps.newHashMap();

    public DefaultStatisticsProvider(ScheduledExecutorService executor){
        this.repoStats = new RepositoryStatisticsImpl(executor);
    }

    @Override
    public RepositoryStatistics getStats() {
        return repoStats;
    }

    @Override
    public MeterStats getMeter(String name, StatsOptions options) {
        return getStats(name, true, SimpleStats.Type.METER, options);
    }

    @Override
    public CounterStats getCounterStats(String name, StatsOptions options) {
        return getStats(name, false, SimpleStats.Type.COUNTER, options);
    }

    @Override
    public TimerStats getTimer(String name, StatsOptions options) {
        return getStats(name, true, SimpleStats.Type.TIMER, options);
    }

    @Override
    public HistogramStats getHistogram(String name, StatsOptions options) {
        return getStats(name, true, SimpleStats.Type.HISTOGRAM, options);
    }

    private synchronized SimpleStats getStats(String type, boolean resetValueEachSecond, SimpleStats.Type statsType,
                                              StatsOptions options){
        Type enumType = Type.getType(type);
        SimpleStats stats = statsMeters.get(type);
        if (stats == null){
            if (enumType != null) {
                stats = new SimpleStats(repoStats.getCounter(enumType), statsType);
            } else if (options.isTimeSeriesEnabled()) {
                stats = new SimpleStats(repoStats.getCounter(type, resetValueEachSecond), statsType);
            } else {
                stats = new SimpleStats(new AtomicLong(), statsType);
            }
            statsMeters.put(type, stats);
        }
        return stats;
    }
}
