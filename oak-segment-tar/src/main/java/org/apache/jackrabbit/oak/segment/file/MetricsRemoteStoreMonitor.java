/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.NotNull;
import java.util.concurrent.TimeUnit;

public class MetricsRemoteStoreMonitor extends RemoteStoreMonitorAdapter {

    public static final String REQUEST_COUNT = "REQUEST_COUNT";
    public static final String REQUEST_ERROR = "REQUEST_ERROR";
    public static final String REQUEST_DURATION = "REQUEST_DURATION";

    private final CounterStats requestCountStats;
    private final CounterStats requestErrorStats;
    private final TimerStats requestDurationStats;

    public MetricsRemoteStoreMonitor(@NotNull StatisticsProvider statisticsProvider) {
        requestCountStats = statisticsProvider.getCounterStats(REQUEST_COUNT, StatsOptions.DEFAULT);
        requestErrorStats = statisticsProvider.getCounterStats(REQUEST_ERROR, StatsOptions.DEFAULT);
        requestDurationStats = statisticsProvider.getTimer(REQUEST_DURATION, StatsOptions.METRICS_ONLY);
    }

    @Override
    public void requestCount() {
        requestCountStats.inc(); }

    @Override
    public void requestError() {
        requestErrorStats.inc();
    }

    @Override
    public void requestDuration(long duration, TimeUnit timeUnit) {
        requestDurationStats.update(duration, timeUnit);
    }
}
