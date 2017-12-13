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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.Maps;
import org.apache.jackrabbit.api.jmx.QueryStatManagerMBean;
import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.api.stats.RepositoryStatistics.Type;
import org.apache.jackrabbit.oak.api.jmx.RepositoryStatsMBean;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.apache.jackrabbit.stats.TimeSeriesMax;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StatisticManagerTest {
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @Test
    public void defaultSetup() throws Exception {
        Whiteboard wb = new DefaultWhiteboard();
        StatisticManager mgr = new StatisticManager(wb, executorService);

        MeterStats meterStats = mgr.getMeter(Type.QUERY_COUNT);
        meterStats.mark(5);

        assertNotNull(WhiteboardUtils.getServices(wb, RepositoryStatsMBean.class));
        assertNotNull(WhiteboardUtils.getServices(wb, QueryStatManagerMBean.class));
    }

    @Test
    public void setupWithCustom() throws Exception {
        Whiteboard wb = new DefaultWhiteboard();
        wb.register(StatisticsProvider.class, StatisticsProvider.NOOP, null);
        StatisticManager mgr = new StatisticManager(wb, executorService);

        MeterStats meterStats = mgr.getMeter(Type.QUERY_COUNT);
        meterStats.mark(5);

        //TODO Not easy to do any asserts on call. Need to figure out a way
    }

    @Test
    public void timeSeriesOnly() throws Exception{
        Whiteboard wb = new DefaultWhiteboard();
        final Map<String, StatsOptions> optionsPassed = Maps.newHashMap();
        wb.register(StatisticsProvider.class, new DummyStatsProvider(){
            @Override
            public MeterStats getMeter(String name, StatsOptions options) {
                optionsPassed.put(name, options);
                return super.getMeter(name, options);
            }
        }, null);
        StatisticManager mgr = new StatisticManager(wb, executorService);

        mgr.getMeter(Type.SESSION_READ_COUNTER);
        assertEquals(StatsOptions.TIME_SERIES_ONLY, optionsPassed.get(Type.SESSION_READ_COUNTER.name()));

        mgr.getMeter(Type.SESSION_WRITE_COUNTER);
        assertEquals(StatsOptions.DEFAULT, optionsPassed.get(Type.SESSION_WRITE_COUNTER.name()));
    }

    @Test
    public void observationQueueMaxLength() {
        AtomicLong maxQueueLength = new AtomicLong();
        SimpleStats stats = new SimpleStats(maxQueueLength, SimpleStats.Type.COUNTER);
        Whiteboard wb = new DefaultWhiteboard();
        wb.register(StatisticsProvider.class, new DummyStatsProvider() {
            @Override
            public CounterStats getCounterStats(String name,
                                                StatsOptions options) {
                if (name.equals(RepositoryStats.OBSERVATION_QUEUE_MAX_LENGTH)) {
                    return stats;
                }
                return super.getCounterStats(name, options);
            }
        }, null);
        StatisticManager mgr = new StatisticManager(wb, executorService);

        TimeSeriesMax rec = mgr.maxQueLengthRecorder();
        List<Runnable> services = wb.track(Runnable.class).getServices();
        assertEquals(1, services.size());
        // this is the scheduled task registered by the StatisticsManager
        // updating the TimeSeries every second
        Runnable pulse = services.get(0);

        // must reset the stats to 'unknown' (-1)
        pulse.run();
        assertEquals(-1L, maxQueueLength.get());

        rec.recordValue(7);
        rec.recordValue(5);
        rec.recordValue(-1);

        // must return the max value
        assertEquals(7L, maxQueueLength.get());

        // must reset the stats to 'unknown' (-1)
        pulse.run();
        assertEquals(-1L, maxQueueLength.get());
    }

    @After
    public void cleanup() {
        executorService.shutdownNow();
    }

    private class DummyStatsProvider implements StatisticsProvider {

        @Override
        public RepositoryStatistics getStats() {
            return NoopRepositoryStatistics.INSTANCE;
        }

        @Override
        public MeterStats getMeter(String name, StatsOptions options) {
            return NoopStats.INSTANCE;
        }

        @Override
        public CounterStats getCounterStats(String name, StatsOptions options) {
            return NoopStats.INSTANCE;
        }

        @Override
        public TimerStats getTimer(String name, StatsOptions options) {
            return NoopStats.INSTANCE;
        }

        @Override
        public HistogramStats getHistogram(String name, StatsOptions options) {
            return NoopStats.INSTANCE;
        }
    }
}
