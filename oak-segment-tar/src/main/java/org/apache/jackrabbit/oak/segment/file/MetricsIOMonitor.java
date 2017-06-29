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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.io.File;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.segment.file.tar.IOMonitorAdapter;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;

/**
 * This {@code IOMonitor} implementations registers the following monitoring endpoints
 * with the Metrics library if available:
 * <ul>
 *     <li>{@link #OAK_SEGMENT_SEGMENT_READ_BYTES}:
 *          a meter metrics for the number of bytes read from tar files</li>
 *     <li>{@link #OAK_SEGMENT_SEGMENT_WRITE_BYTES}:
 *          a meter metrics for the number of bytes written to tar files</li>
 *     <li>{@link #OAK_SEGMENT_SEGMENT_READ_TIME}:
 *          a timer metrics for the time spent reading from tar files</li>
 *     <li>{@link #OAK_SEGMENT_SEGMENT_WRITE_TIME}:
 *          a timer metrics for the time spent writing to tar files</li>
 * </ul>
 */
public class MetricsIOMonitor extends IOMonitorAdapter {
    public static final String OAK_SEGMENT_SEGMENT_READ_BYTES = "oak.segment.segment-read-bytes";
    public static final String OAK_SEGMENT_SEGMENT_WRITE_BYTES = "oak.segment.segment-write-bytes";
    public static final String OAK_SEGMENT_SEGMENT_READ_TIME = "oak.segment.segment-read-time";
    public static final String OAK_SEGMENT_SEGMENT_WRITE_TIME = "oak.segment.segment-write-time";

    private final MeterStats segmentReadBytes;
    private final MeterStats segmentWriteBytes;
    private final TimerStats segmentReadTime;
    private final TimerStats segmentWriteTime;

    public MetricsIOMonitor(@Nonnull StatisticsProvider statisticsProvider) {
        segmentReadBytes = statisticsProvider.getMeter(
                OAK_SEGMENT_SEGMENT_READ_BYTES, StatsOptions.METRICS_ONLY);
        segmentWriteBytes = statisticsProvider.getMeter(
                OAK_SEGMENT_SEGMENT_WRITE_BYTES, StatsOptions.METRICS_ONLY);
        segmentReadTime = statisticsProvider.getTimer(
                OAK_SEGMENT_SEGMENT_READ_TIME, StatsOptions.METRICS_ONLY);
        segmentWriteTime = statisticsProvider.getTimer(
                OAK_SEGMENT_SEGMENT_WRITE_TIME, StatsOptions.METRICS_ONLY);
    }

    @Override
    public void afterSegmentRead(File file, long msb, long lsb, int length, long elapsed) {
        segmentReadBytes.mark(length);
        segmentReadTime.update(elapsed, NANOSECONDS);
    }

    @Override
    public void afterSegmentWrite(File file, long msb, long lsb, int length, long elapsed) {
        segmentWriteBytes.mark(length);
        segmentWriteTime.update(elapsed, NANOSECONDS);
    }
}
