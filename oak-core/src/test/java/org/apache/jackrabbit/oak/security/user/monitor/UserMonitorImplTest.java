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
package org.apache.jackrabbit.oak.security.user.monitor;

import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class UserMonitorImplTest {

    private final MeterStats meter = mock(MeterStats.class);
    private final TimerStats timer = mock(TimerStats.class);

    private StatisticsProvider statisticsProvider;
    private UserMonitorImpl monitor;

    @Before
    public void before() {
        statisticsProvider = mockStatisticsProvider();
        monitor = new UserMonitorImpl(statisticsProvider);
    }

    @After
    public void after() {
        clearInvocations(meter, timer, statisticsProvider);
    }

    private StatisticsProvider mockStatisticsProvider() {
        StatisticsProvider statisticsProvider = mock(StatisticsProvider.class);
        when(statisticsProvider.getMeter(anyString(), any(StatsOptions.class))).thenReturn(meter);
        when(statisticsProvider.getTimer(anyString(), any(StatsOptions.class))).thenReturn(timer);
        return statisticsProvider;
    }

    @Test
    public void testConstructor() {
        verify(statisticsProvider, times(12)).getMeter(anyString(), any(StatsOptions.class));
        verify(statisticsProvider, times(6)).getTimer(anyString(), any(StatsOptions.class));
        verifyNoMoreInteractions(statisticsProvider);
        verifyNoInteractions(meter);
        verifyNoInteractions(timer);
    }

    @Test
    public void testDoneGetMembers() {
        monitor.doneGetMembers(20, true);
        monitor.doneGetMembers(40, false);
        assertTimerOnly(20, 40, timer, meter);
    }

    @Test
    public void testDoneMemberOf() {
        monitor.doneMemberOf(35, true);
        monitor.doneMemberOf(64, false);
        assertTimerOnly(35, 64, timer, meter);
    }

    private static void assertTimerOnly(long timeDeclaredOnly, long timeInherited, @NotNull TimerStats timer, @NotNull MeterStats meter) {
        verify(timer, times(1)).update(timeDeclaredOnly, NANOSECONDS);
        verify(timer, times(1)).update(timeInherited, NANOSECONDS);
        verify(timer, times(2)).update(anyLong(), any(TimeUnit.class));

        verifyNoMoreInteractions(timer);
        verifyNoInteractions(meter);
    }

    @Test
    public void testDoneUpdateMembers() {
        monitor.doneUpdateMembers(13, 1, 0, true);
        assertUpdateStats(13, 1, 0, timer, meter);

        monitor.doneUpdateMembers(Long.MAX_VALUE, Integer.MAX_VALUE, 5000, false);
        assertUpdateStats(Long.MAX_VALUE, Integer.MAX_VALUE, 5000, timer, meter);

        verify(meter, times(4)).mark(anyLong());
        verify(timer, times(2)).update(anyLong(), any(TimeUnit.class));
        verifyNoMoreInteractions(meter, timer);
    }

    private static void assertUpdateStats(long time, long total, long failed, @NotNull TimerStats timer, @NotNull MeterStats meter) {
        long succeeded = total-failed;
        verify(meter, times(1)).mark(succeeded);
        verify(meter, times(1)).mark(failed);
        verify(timer, times(1)).update(time, NANOSECONDS);
    }

    @Test
    public void testAdminLogin() {
        MeterStats succeeded = mockMeter("security.user.admin.login.succeeded");
        MeterStats failed = mockMeter("security.user.admin.login.failed");
        UserMonitorImpl m = new UserMonitorImpl(statisticsProvider);

        m.adminLogin(true);
        m.adminLogin(true);
        m.adminLogin(false);

        verify(succeeded, times(2)).mark();
        verify(failed, times(1)).mark();
        verifyNoMoreInteractions(succeeded, failed);
        verifyNoInteractions(meter, timer);
    }

    @Test
    public void testAdminImpersonated() {
        MeterStats succeeded = mockMeter("security.user.admin.impersonation.succeeded");
        MeterStats failed = mockMeter("security.user.admin.impersonation.failed");
        UserMonitorImpl m = new UserMonitorImpl(statisticsProvider);

        m.adminImpersonated(false);
        m.adminImpersonated(false);
        m.adminImpersonated(true);

        verify(succeeded, times(1)).mark();
        verify(failed, times(2)).mark();
        verifyNoMoreInteractions(succeeded, failed);
        verifyNoInteractions(meter, timer);
    }

    @Test
    public void testAdminPasswordChanged() {
        MeterStats added = mockMeter("security.user.admin.password.added");
        MeterStats changed = mockMeter("security.user.admin.password.changed");
        UserMonitorImpl m = new UserMonitorImpl(statisticsProvider);

        m.adminPasswordChanged(false);
        m.adminPasswordChanged(false);
        m.adminPasswordChanged(true);

        verify(changed, times(2)).mark();
        verify(added, times(1)).mark();
        verifyNoMoreInteractions(added, changed);
        verifyNoInteractions(meter, timer);
    }

    @Test
    public void testAdminImpersonatorsChanged() {
        MeterStats granted = mockMeter("security.user.admin.impersonators.granted");
        MeterStats revoked = mockMeter("security.user.admin.impersonators.revoked");
        UserMonitorImpl m = new UserMonitorImpl(statisticsProvider);

        m.adminImpersonatorsChanged(3, 1);

        verify(granted, times(1)).mark(3);
        verify(revoked, times(1)).mark(1);
        verifyNoMoreInteractions(granted, revoked);
        verifyNoInteractions(meter, timer);
    }

    @Test
    public void testNoopAdminMethods() {
        UserMonitor noop = UserMonitor.NOOP;
        noop.adminLogin(true);
        noop.adminImpersonated(true);
        noop.adminPasswordChanged(true);
        noop.adminImpersonatorsChanged(1, 1);
        verifyNoInteractions(meter, timer);
    }

    @NotNull
    private MeterStats mockMeter(@NotNull String name) {
        MeterStats m = mock(MeterStats.class);
        when(statisticsProvider.getMeter(eq(name), any(StatsOptions.class))).thenReturn(m);
        return m;
    }

    @Test
    public void testGetMonitorClass() {
        assertSame(UserMonitor.class, monitor.getMonitorClass());
    }

    @Test
    public void testGetMonitorProperties() {
        assertTrue(monitor.getMonitorProperties().isEmpty());
    }
}