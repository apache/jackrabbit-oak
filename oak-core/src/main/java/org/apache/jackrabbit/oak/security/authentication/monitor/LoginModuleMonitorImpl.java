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
import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.spi.security.authentication.ImpersonationCredentials;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMBean;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMonitor;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.Credentials;
import javax.management.openmbean.CompositeData;
import javax.security.auth.login.LoginException;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class LoginModuleMonitorImpl implements LoginModuleMBean, LoginModuleMonitor {

    private final StatisticsProvider statisticsProvider;

    static final String LOGIN_ERRORS = "LOGIN_ERRORS";
    private static final String LOGIN_FAILED = "security.authentication.login.failed";
    private static final String LOGIN_TOKEN_FAILED = "security.authentication.login_token.failed";
    private static final String LOGIN_IMPERSONATION_FAILED = "security.authentication.login_impersonation.failed";
    private static final String PRINCIPALS_SIZE = "security.authentication.principals.size";
    private static final String PRINCIPALS_TIMER = "security.authentication.principals.timer";

    private final MeterStats loginErrors;
    private final MeterStats loginFailed;
    private final MeterStats loginTokenFailed;
    private final MeterStats loginImpersonationFailed;
    private final MeterStats principalsSize;
    private final TimerStats principalsTime;

    public LoginModuleMonitorImpl(@NotNull StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
        loginErrors = statisticsProvider.getMeter(LOGIN_ERRORS, StatsOptions.DEFAULT);
        loginFailed = statisticsProvider.getMeter(LOGIN_FAILED, StatsOptions.DEFAULT);
        loginTokenFailed = statisticsProvider.getMeter(LOGIN_TOKEN_FAILED, StatsOptions.DEFAULT);
        loginImpersonationFailed = statisticsProvider.getMeter(LOGIN_IMPERSONATION_FAILED, StatsOptions.DEFAULT);
        principalsSize = statisticsProvider.getMeter(PRINCIPALS_SIZE, StatsOptions.DEFAULT);
        principalsTime = statisticsProvider.getTimer(PRINCIPALS_TIMER, StatsOptions.METRICS_ONLY);
    }

    //------------------------------------------------------- < LoginModuleMonitor >---

    @Override
    public void loginError() {
        loginErrors.mark();
    }

    @Override
    public void loginFailed(@NotNull LoginException loginException, @Nullable Credentials credentials) {
        if (credentials instanceof ImpersonationCredentials) {
            loginImpersonationFailed.mark();
        } else if (credentials instanceof TokenCredentials) {
            loginTokenFailed.mark();
        } else {
            loginFailed.mark();
        }
    }

    @Override
    public void principalsCollected(long timeTakenNanos, int numberOfPrincipals) {
        principalsSize.mark(numberOfPrincipals);
        principalsTime.update(timeTakenNanos, NANOSECONDS);
    }

    //----------------------------------------------------------< LoginModuleMBean >---

    @Override
    public long getLoginErrors() {
        return loginErrors.getCount();
    }

    @Override
    public CompositeData getLoginErrorsHistory() {
        return getTimeSeriesData(LOGIN_ERRORS, "Number of login errors.");
    }

    //-----------------------------------------------------------------< internal >---

    @NotNull
    private CompositeData getTimeSeriesData(@NotNull String name, @NotNull String desc) {
        return TimeSeriesStatsUtil.asCompositeData(getTimeSeries(name), desc);
    }

    @NotNull
    private TimeSeries getTimeSeries(@NotNull String name) {
        return statisticsProvider.getStats().getTimeSeries(name, true);
    }
}
