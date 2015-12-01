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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.stats.RepositoryStatisticsImpl;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DefaultStatisticsProviderTest {
    private ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @Test
    public void basicSetup() throws Exception {
        DefaultStatisticsProvider statsProvider = new DefaultStatisticsProvider(executorService);
        assertNotNull(statsProvider.getStats());

        CounterStats stats = statsProvider.getCounterStats(RepositoryStatistics.Type.SESSION_COUNT.name());
        stats.inc();
        assertEquals(1, statsImpl(statsProvider).getCounter(RepositoryStatistics.Type.SESSION_COUNT).get());
    }

    @Test
    public void meter() throws Exception {
        DefaultStatisticsProvider statsProvider = new DefaultStatisticsProvider(executorService);
        MeterStats meterStats = statsProvider.getMeter("test");

        assertNotNull(meterStats);
        meterStats.mark();
        assertEquals(1, statsImpl(statsProvider).getCounter("test", true).get());
    }

    @Test
    public void counter() throws Exception {
        DefaultStatisticsProvider statsProvider = new DefaultStatisticsProvider(executorService);
        CounterStats counterStats = statsProvider.getCounterStats("test");

        assertNotNull(counterStats);
        counterStats.inc();
        assertEquals(1, statsImpl(statsProvider).getCounter("test", false).get());
    }

    @Test
    public void timer() throws Exception {
        DefaultStatisticsProvider statsProvider = new DefaultStatisticsProvider(executorService);
        TimerStats timerStats = statsProvider.getTimer("test");

        assertNotNull(timerStats);
        timerStats.update(100, TimeUnit.SECONDS);
        assertEquals(TimeUnit.SECONDS.toMillis(100), statsImpl(statsProvider).getCounter("test", false).get());
    }

    @After
    public void cleanup() {
        executorService.shutdownNow();
    }

    private RepositoryStatisticsImpl statsImpl(DefaultStatisticsProvider statsProvider) {
        return (RepositoryStatisticsImpl) statsProvider.getStats();
    }
}
