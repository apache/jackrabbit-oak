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
package org.apache.jackrabbit.oak.plugins.metric.util;

import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Util class to generate a name for Stats implementations that can be used for creating labels in prometheus.
 * Usage - StatsProviderUtil(<StatisticsProvider Object>).getHistoStats().apply(metricName, labels)
 * where metricName is a String to denote the metric name and labels is map of label values.
 * Resultant metric will be created with a name as follows -
 * metricName;labelName1=labelValue1;labelName2=labelValue2
 * This can then be translated by a consuming alerting system like prometheus into metric name and labels separately.
 */
public class StatsProviderUtil {

    private final StatisticsProvider statisticsProvider;
    private final BiFunction<String, Map<String, String>, String> METRIC = (name, labels) -> labels.entrySet().stream().reduce(name,
            (n, e) -> n + ";" + e.getKey() + "=" + e.getValue(),
            (n1, n2) -> n1 + n2);

    public StatsProviderUtil(@NotNull StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
    }

    public BiFunction<String, Map<String, String>, HistogramStats> getHistoStats() {
        return (name, labels) -> statisticsProvider.getHistogram(METRIC.apply(name, labels), StatsOptions.METRICS_ONLY);
    }

    public BiFunction<String, Map<String, String>, CounterStats> getCounterStats() {
        return (name, labels) -> statisticsProvider.getCounterStats(METRIC.apply(name, labels), StatsOptions.METRICS_ONLY);
    }

    public BiFunction<String, Map<String, String>, TimerStats> getTimerStats() {
        return (name, labels) -> statisticsProvider.getTimer(METRIC.apply(name, labels), StatsOptions.METRICS_ONLY);
    }

    public BiFunction<String, Map<String, String>, MeterStats> getMeterStats() {
        return (name, labels) -> statisticsProvider.getMeter(METRIC.apply(name, labels), StatsOptions.METRICS_ONLY);
    }

}
