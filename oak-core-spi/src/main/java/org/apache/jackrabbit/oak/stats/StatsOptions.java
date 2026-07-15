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

public final class StatsOptions {
    /**
     * Default mode where both TimeSeries data and other statistical data
     * would be collected
     */
    public static final StatsOptions DEFAULT = new StatsOptions(true, true);
    /**
     * In this mode only TimeSeries data would be collected.
     */
    public static final StatsOptions TIME_SERIES_ONLY = new StatsOptions(true, false);
    /**
     * In this mode only statistical data would be collected.
     */
    public static final StatsOptions METRICS_ONLY = new StatsOptions(false, true);

    private final boolean timeSeriesEnabled;
    private final boolean metricsEnabled;

    private StatsOptions(boolean timeSeriesEnabled, boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;
        this.timeSeriesEnabled = timeSeriesEnabled;
    }

    public boolean isTimeSeriesEnabled() {
        return timeSeriesEnabled;
    }

    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    public boolean isOnlyMetricEnabled() {
        return !timeSeriesEnabled && metricsEnabled;
    }

    public boolean isOnlyTimeSeriesEnabled() {
        return timeSeriesEnabled && !metricsEnabled;
    }

}
