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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.stats.RepositoryStatisticsImpl;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DefaultStatisticsProviderTest {
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private DefaultStatisticsProvider statsProvider = new DefaultStatisticsProvider(executorService);

    @Test
    public void basicSetup() throws Exception {
        assertNotNull(statsProvider.getStats());

        CounterStats stats = statsProvider.getCounterStats(RepositoryStatistics.Type.SESSION_COUNT.name(), StatsOptions.DEFAULT);
        stats.inc();
        assertEquals(1, statsImpl(statsProvider).getCounter(RepositoryStatistics.Type.SESSION_COUNT).get());
    }

    @Test
    public void meter() throws Exception {
        MeterStats meterStats = statsProvider.getMeter("test", StatsOptions.DEFAULT);

        assertNotNull(meterStats);
        meterStats.mark();
        assertEquals(1, statsImpl(statsProvider).getCounter("test", true).get());
        assertTrue(getRegisteredTimeSeries(statsProvider).contains("test"));
    }

    @Test
    public void counter() throws Exception {
        CounterStats counterStats = statsProvider.getCounterStats("test", StatsOptions.DEFAULT);

        assertNotNull(counterStats);
        counterStats.inc();
        assertEquals(1, statsImpl(statsProvider).getCounter("test", false).get());
        assertTrue(getRegisteredTimeSeries(statsProvider).contains("test"));
    }

    @Test
    public void timer() throws Exception {
        TimerStats timerStats = statsProvider.getTimer("test", StatsOptions.DEFAULT);

        assertNotNull(timerStats);
        timerStats.update(100, TimeUnit.SECONDS);
        assertEquals(TimeUnit.SECONDS.toMillis(100), statsImpl(statsProvider).getCounter("test", false).get());
        assertTrue(getRegisteredTimeSeries(statsProvider).contains("test"));
    }
    
    @Test
    public void metricOnly() throws Exception{
        MeterStats meterStats = statsProvider.getMeter("test", StatsOptions.METRICS_ONLY);
        assertFalse(getRegisteredTimeSeries(statsProvider).contains("test"));
    }

    @After
    public void cleanup() {
        executorService.shutdownNow();
    }

    private RepositoryStatisticsImpl statsImpl(DefaultStatisticsProvider statsProvider) {
        return (RepositoryStatisticsImpl) statsProvider.getStats();
    }

    private Set<String> getRegisteredTimeSeries(DefaultStatisticsProvider statsProvider){
        RepositoryStatisticsImpl stats = (RepositoryStatisticsImpl) statsProvider.getStats();
        Set<String> names = new HashSet<String>();
        for (Map.Entry<String, TimeSeries> e : stats){
            names.add(e.getKey());
        }
        return names;
    }
}
