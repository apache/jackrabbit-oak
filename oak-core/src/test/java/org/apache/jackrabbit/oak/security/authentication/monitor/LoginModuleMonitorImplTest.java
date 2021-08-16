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

import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
        verify(sp, times(5)).getMeter(anyString(), any(StatsOptions.class));
        verify(sp, times(1)).getTimer(anyString(), any(StatsOptions.class));
    }

    @Test
    public void testLoginFailed() {
        LoginException e = new LoginException();
        LoginModuleMonitorImpl s = new LoginModuleMonitorImpl(sp);
        s.loginFailed(e, null);
        SimpleCredentials sc = new SimpleCredentials("id", new char[0]);
        s.loginFailed(e, sc);
        s.loginFailed(e, new TokenCredentials("token"));
        s.loginFailed(e, new ImpersonationCredentials(sc, AuthInfo.EMPTY));
        verify(meter, times(4)).mark();
        verify(meter, never()).mark(any(Long.class));
    }

    @Test
    public void testPrincipalsCollected() {
        LoginModuleMonitorImpl s = new LoginModuleMonitorImpl(sp);
        s.principalsCollected(25, 4);
        verify(meter, times(1)).mark(4);
        verify(meter, never()).mark();
        verify(timer, times(1)).update(25, TimeUnit.NANOSECONDS);
    }
}