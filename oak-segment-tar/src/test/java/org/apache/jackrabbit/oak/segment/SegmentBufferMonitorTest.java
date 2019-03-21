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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.segment.SegmentBufferMonitor.DIRECT_BUFFER_CAPACITY;
import static org.apache.jackrabbit.oak.segment.SegmentBufferMonitor.DIRECT_BUFFER_COUNT;
import static org.apache.jackrabbit.oak.segment.SegmentBufferMonitor.HEAP_BUFFER_CAPACITY;
import static org.apache.jackrabbit.oak.segment.SegmentBufferMonitor.HEAP_BUFFER_COUNT;
import static org.apache.jackrabbit.oak.stats.SimpleStats.Type.COUNTER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.SimpleStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.Test;

public class SegmentBufferMonitorTest {

    private final Map<String, CounterStats> stats = newHashMap();

    private final SegmentBufferMonitor segmentBufferMonitor = new SegmentBufferMonitor(new StatisticsProvider() {
        @Override
        public RepositoryStatistics getStats() {
            throw new IllegalStateException();
        }

        @Override
        public MeterStats getMeter(String name, StatsOptions options) {
            throw new IllegalStateException();
        }

        @Override
        public CounterStats getCounterStats(String name, StatsOptions options) {
            SimpleStats simpleStats = new SimpleStats(new AtomicLong(), COUNTER);
            stats.put(name, simpleStats);
            return simpleStats;
        }

        @Override
        public TimerStats getTimer(String name, StatsOptions options) {
            throw new IllegalStateException();
        }

        @Override
        public HistogramStats getHistogram(String name, StatsOptions options) {
            throw new IllegalStateException();
        }
    });

    @Test
    public void emptyStats() {
        assertEquals(0, stats.get(DIRECT_BUFFER_COUNT).getCount());
        assertEquals(0, stats.get(DIRECT_BUFFER_CAPACITY).getCount());
        assertEquals(0, stats.get(HEAP_BUFFER_COUNT).getCount());
        assertEquals(0, stats.get(HEAP_BUFFER_CAPACITY).getCount());
    }

    @Test
    public void heapBuffer() {
        Buffer buffer = Buffer.allocate(42);
        segmentBufferMonitor.trackAllocation(buffer);

        assertEquals(0, stats.get(DIRECT_BUFFER_COUNT).getCount());
        assertEquals(0, stats.get(DIRECT_BUFFER_CAPACITY).getCount());
        assertEquals(1, stats.get(HEAP_BUFFER_COUNT).getCount());
        assertEquals(42, stats.get(HEAP_BUFFER_CAPACITY).getCount());

        buffer = null;
        System.gc();

        assertEquals(0, stats.get(DIRECT_BUFFER_COUNT).getCount());
        assertEquals(0, stats.get(DIRECT_BUFFER_CAPACITY).getCount());
        assertTrue(stats.get(HEAP_BUFFER_COUNT).getCount() <= 1);
        assertTrue(stats.get(HEAP_BUFFER_CAPACITY).getCount() <= 42);
    }

    @Test
    public void directBuffer() {
        Buffer buffer = Buffer.allocateDirect(42);
        segmentBufferMonitor.trackAllocation(buffer);

        assertEquals(1, stats.get(DIRECT_BUFFER_COUNT).getCount());
        assertEquals(42, stats.get(DIRECT_BUFFER_CAPACITY).getCount());
        assertEquals(0, stats.get(HEAP_BUFFER_COUNT).getCount());
        assertEquals(0, stats.get(HEAP_BUFFER_CAPACITY).getCount());

        buffer = null;
        System.gc();

        assertTrue(stats.get(DIRECT_BUFFER_COUNT).getCount() <= 1);
        assertTrue(stats.get(DIRECT_BUFFER_CAPACITY).getCount() <= 42);
        assertEquals(0, stats.get(HEAP_BUFFER_COUNT).getCount());
        assertEquals(0, stats.get(HEAP_BUFFER_CAPACITY).getCount());
    }
}
