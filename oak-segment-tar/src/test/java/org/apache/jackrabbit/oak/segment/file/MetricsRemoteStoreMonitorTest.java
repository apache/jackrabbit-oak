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

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.After;

import static org.apache.jackrabbit.oak.segment.file.MetricsRemoteStoreMonitor.*;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricsRemoteStoreMonitorTest {
    private ScheduledExecutorService executor;

    private CounterStats requestCount;
    private CounterStats requestErrorCount;
    private TimerStats requestDuration;

    private int requestCountExpected = 3;
    private int requestErrorCountExpected = 2;

    @Before
    public void setup(){
        executor = Executors.newSingleThreadScheduledExecutor();
        DefaultStatisticsProvider statisticsProvider = new DefaultStatisticsProvider(executor);
        MetricsRemoteStoreMonitor remoteStoreMonitor = new MetricsRemoteStoreMonitor(statisticsProvider);
        requestCount = statisticsProvider.getCounterStats(REQUEST_COUNT, StatsOptions.DEFAULT);
        requestErrorCount = statisticsProvider.getCounterStats(REQUEST_ERROR, StatsOptions.DEFAULT);
        requestDuration =  statisticsProvider.getTimer(REQUEST_DURATION, StatsOptions.METRICS_ONLY);

        for(int i = 0; i < requestCountExpected; i++){
            remoteStoreMonitor.requestCount();
        }
        for(int i = 0; i < requestErrorCountExpected; i++){
            requestErrorCount.inc();
        }
        requestDuration.update(100, TimeUnit.MILLISECONDS);

    }

    @After
    public void tearDown(){
        new ExecutorCloser(executor).close();
    }

    @Test
    public void testStats(){
        assertEquals(requestCountExpected, requestCount.getCount());
        assertEquals(requestErrorCountExpected, requestErrorCount.getCount());
        assertEquals(1, requestDuration.getCount());
    }
}
