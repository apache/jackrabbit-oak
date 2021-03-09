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
 */
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.monitor;

import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncException;
import org.apache.jackrabbit.oak.spi.security.authentication.external.SyncResult;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ExternalIdentityMonitorImplTest {

    private final MeterStats meter = mock(MeterStats.class);
    private final TimerStats timer = mock(TimerStats.class);
    private final CounterStats counter = mock(CounterStats.class);

    private StatisticsProvider statisticsProvider;
    private ExternalIdentityMonitorImpl monitor;

    @Before
    public void before() {
        statisticsProvider = mockStatisticsProvider();
        monitor = new ExternalIdentityMonitorImpl(statisticsProvider);
    }

    @After
    public void after() {
        clearInvocations(meter, timer, statisticsProvider);
    }

    private StatisticsProvider mockStatisticsProvider() {
        StatisticsProvider statisticsProvider = mock(StatisticsProvider.class);
        when(statisticsProvider.getMeter(anyString(), any(StatsOptions.class))).thenReturn(meter);
        when(statisticsProvider.getTimer(anyString(), any(StatsOptions.class))).thenReturn(timer);
        when(statisticsProvider.getCounterStats(anyString(), any(StatsOptions.class))).thenReturn(counter);
        return statisticsProvider;
    }

    @Test
    public void testConstructor() {
        verify(statisticsProvider, times(2)).getMeter(anyString(), any(StatsOptions.class));
        verify(statisticsProvider, times(2)).getTimer(anyString(), any(StatsOptions.class));
        verifyNoMoreInteractions(statisticsProvider);
        verifyNoInteractions(meter);
        verifyNoInteractions(timer);
    }

    @Test
    public void testDoneSyncExternalIdentity() {
        monitor.doneSyncExternalIdentity(20, mock(SyncResult.class), 34);
        verify(timer).update(20, NANOSECONDS);
        verify(meter).mark(34);
        verifyNoMoreInteractions(meter, timer);
    }

    @Test
    public void testDoneSyncId() {
        monitor.doneSyncId(5,  mock(SyncResult.class));
        verify(timer).update(5, NANOSECONDS);
        verifyNoMoreInteractions(timer);
        verifyNoInteractions(meter);
    }

    @Test
    public void testSyncFailed() {
        monitor.syncFailed(mock(SyncException.class));
        verify(meter).mark();
        verifyNoMoreInteractions(meter);
        verifyNoInteractions(timer);
    }

    @Test
    public void testGetMonitorClass() {
        assertSame(ExternalIdentityMonitor.class, monitor.getMonitorClass());
    }

    @Test
    public void testGetMonitorProperties() {
        assertTrue(monitor.getMonitorProperties().isEmpty());
    }
}