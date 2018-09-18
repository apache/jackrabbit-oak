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

import static org.apache.jackrabbit.oak.segment.file.MetricsIOMonitor.OAK_SEGMENT_SEGMENT_READ_BYTES;
import static org.apache.jackrabbit.oak.segment.file.MetricsIOMonitor.OAK_SEGMENT_SEGMENT_READ_TIME;
import static org.apache.jackrabbit.oak.segment.file.MetricsIOMonitor.OAK_SEGMENT_SEGMENT_WRITE_BYTES;
import static org.apache.jackrabbit.oak.segment.file.MetricsIOMonitor.OAK_SEGMENT_SEGMENT_WRITE_TIME;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetricsIOMonitorTest {
    private ScheduledExecutorService executor;

    private MeterStats segmentReadBytes;
    private MeterStats segmentWriteBytes;
    private TimerStats segmentReadTime;
    private TimerStats segmentWriteTime;

    @Before
    public void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
        DefaultStatisticsProvider statisticsProvider = new DefaultStatisticsProvider(executor);
        MetricsIOMonitor ioMonitor = new MetricsIOMonitor(statisticsProvider);
        segmentReadBytes = statisticsProvider.getMeter(
                OAK_SEGMENT_SEGMENT_READ_BYTES, StatsOptions.METRICS_ONLY);
        segmentWriteBytes = statisticsProvider.getMeter(
                OAK_SEGMENT_SEGMENT_WRITE_BYTES, StatsOptions.METRICS_ONLY);
        segmentReadTime = statisticsProvider.getTimer(
                OAK_SEGMENT_SEGMENT_READ_TIME, StatsOptions.METRICS_ONLY);
        segmentWriteTime = statisticsProvider.getTimer(
                OAK_SEGMENT_SEGMENT_WRITE_TIME, StatsOptions.METRICS_ONLY);

        File file = new File("");
        ioMonitor.afterSegmentRead(file, 0, 0, 4, 0);
        ioMonitor.afterSegmentRead(file, 0, 0, 5, 0);
        ioMonitor.afterSegmentWrite(file, 0, 0, 3, 0);
        ioMonitor.afterSegmentWrite(file, 0, 0, 4, 0);
    }

    @After
    public void tearDown() {
        new ExecutorCloser(executor).close();
    }

    @Test
    public void testStats() {
        assertEquals(9, segmentReadBytes.getCount());
        assertEquals(2, segmentReadTime.getCount());
        assertEquals(7, segmentWriteBytes.getCount());
        assertEquals(2, segmentWriteTime.getCount());
    }
}
