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
package org.apache.jackrabbit.oak.security.authentication.monitor;

import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoginModuleMonitorImplTest {

    private final MeterStats meter = mock(MeterStats.class);
    private final TimerStats timer = mock(TimerStats.class);
    private StatisticsProvider sp;

    @Before
    public void before() {
        sp = mock(StatisticsProvider.class);
        when(sp.getMeter(anyString(), any(StatsOptions.class))).thenReturn(meter);
        when(sp.getTimer(anyString(), any(StatsOptions.class))).thenReturn(timer);
    }

    @After
    public void after() {
        clearInvocations(meter, timer, sp);
    }

    @Test
    public void testLoginError() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        StatisticsProvider sp = new DefaultStatisticsProvider(executor);

        LoginModuleMonitorImpl s = new LoginModuleMonitorImpl(sp);
        s.loginError();
        assertEquals(1, s.getLoginErrors());
        assertNotNull(s.getLoginErrorsHistory());
    }

    @Test
    public void testConstructor() {
        LoginModuleMonitorImpl s = new LoginModuleMonitorImpl(sp);
        verify(sp, times(1)).getMeter(anyString(), any(StatsOptions.class));
        verify(sp, times(0)).getTimer(anyString(), any(StatsOptions.class));
    }
}