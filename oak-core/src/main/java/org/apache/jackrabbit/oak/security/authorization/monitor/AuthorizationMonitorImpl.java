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
package org.apache.jackrabbit.oak.security.authorization.monitor;

import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class AuthorizationMonitorImpl implements AuthorizationMonitor {

    private final MeterStats accessViolations;
    private final MeterStats permissionError;
    private final MeterStats permissionRefresh;
    private final TimerStats permissionAllLoaded;

    public AuthorizationMonitorImpl(@NotNull StatisticsProvider statisticsProvider) {
        accessViolations = statisticsProvider.getMeter("security.authorization.default.access.violation", StatsOptions.DEFAULT);
        permissionError = statisticsProvider.getMeter("security.authorization.default.permission.error", StatsOptions.DEFAULT);
        permissionRefresh = statisticsProvider.getMeter("security.authorization.default.permission.refresh", StatsOptions.DEFAULT);
        permissionAllLoaded = statisticsProvider.getTimer("security.authorization.default.permission.all_loaded", StatsOptions.METRICS_ONLY);
    }

    //-------------------------------------------------------------------------------------< AccessViolationMonitor >---

    @Override
    public @NotNull Class<AuthorizationMonitor> getMonitorClass() {
        return AuthorizationMonitor.class;
    }

    @Override
    public @NotNull Map<Object, Object> getMonitorProperties() {
        return Collections.emptyMap();
    }

    @Override
    public void accessViolation() {
        accessViolations.mark();
    }

    @Override
    public void permissionError() {
        permissionError.mark();
    }

    @Override
    public void permissionRefresh() {
        permissionRefresh.mark();

    }

    @Override
    public void permissionAllLoaded(long timeTakenNanos) {
        permissionAllLoaded.update(timeTakenNanos, NANOSECONDS);
    }
}