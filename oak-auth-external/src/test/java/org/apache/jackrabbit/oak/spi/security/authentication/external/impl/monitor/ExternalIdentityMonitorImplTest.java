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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ExternalIdentityMonitorImplTest {

    private final MeterStats failed = mock(MeterStats.class);
    private final TimerStats timer = mock(TimerStats.class);
    private final MeterStats retries = mock(MeterStats.class);

    private StatisticsProvider statisticsProvider;
    private ExternalIdentityMonitorImpl monitor;

    @Before
    public void before() {
        statisticsProvider = mockStatisticsProvider();
        monitor = new ExternalIdentityMonitorImpl(statisticsProvider);
    }

    @After
    public void after() {
        clearInvocations(retries, failed, timer, statisticsProvider);
    }

    private StatisticsProvider mockStatisticsProvider() {
        StatisticsProvider statisticsProvider = mock(StatisticsProvider.class);
        when(statisticsProvider.getMeter(eq("security.authentication.external.sync_external_identity.retries"), any(StatsOptions.class))).thenReturn(retries);
        when(statisticsProvider.getMeter(eq("security.authentication.external.sync.failed"), any(StatsOptions.class))).thenReturn(failed);
        when(statisticsProvider.getTimer(anyString(), any(StatsOptions.class))).thenReturn(timer);
        return statisticsProvider;
    }

    @Test
    public void testConstructor() {
        verify(statisticsProvider, times(2)).getMeter(anyString(), any(StatsOptions.class));
        verify(statisticsProvider, times(2)).getTimer(anyString(), any(StatsOptions.class));
        verifyNoMoreInteractions(statisticsProvider);
        verifyNoInteractions(failed, retries, timer);
    }

    @Test
    public void testDoneSyncExternalIdentity() {
        monitor.doneSyncExternalIdentity(20, mock(SyncResult.class), 34);
        verify(timer).update(20, NANOSECONDS);
        verify(retries).mark(34);
        verifyNoMoreInteractions(retries, timer);
        verifyNoInteractions(failed);
    }


    @Test
    public void testDoneSyncExternalIdentityNoRetry() {
        monitor.doneSyncExternalIdentity(20, mock(SyncResult.class), 0);
        verify(timer).update(20, NANOSECONDS);
        verifyNoMoreInteractions(timer);
        verifyNoInteractions(retries, failed);
    }

    @Test
    public void testDoneSyncId() {
        monitor.doneSyncId(5,  mock(SyncResult.class));
        verify(timer).update(5, NANOSECONDS);
        verifyNoMoreInteractions(timer);
        verifyNoInteractions(retries, failed);
    }

    @Test
    public void testSyncFailed() {
        monitor.syncFailed(mock(SyncException.class));
        verify(failed).mark();
        verifyNoMoreInteractions(failed);
        verifyNoInteractions(retries, timer);
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