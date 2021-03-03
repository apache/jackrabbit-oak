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

import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMBean;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginModuleMonitor;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;
import org.jetbrains.annotations.NotNull;

import javax.management.openmbean.CompositeData;

public class LoginModuleMonitorImpl implements LoginModuleMBean, LoginModuleMonitor {

    private final StatisticsProvider statisticsProvider;

    static final String LOGIN_ERRORS = "LOGIN_ERRORS";

    private final MeterStats loginErrors;

    public LoginModuleMonitorImpl(StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
        loginErrors = statisticsProvider.getMeter(LOGIN_ERRORS, StatsOptions.DEFAULT);
    }

    //------------------------------------------------------- < LoginModuleMonitor >---

    @Override
    public void loginError() {
        loginErrors.mark();
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
