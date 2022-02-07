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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Provides high level functions to track and measure activities against Elastic.
 */
public class ElasticMetricHandler {

    private static final String QUERY_RATE = "ELASTIC_QUERY_RATE";
    private static final String QUERY_INTERNAL_RATE = "ELASTIC_QUERY_INTERNAL_RATE";

    private static final String QUERY_HITS = "ELASTIC_QUERY_HITS";
    private static final String QUERY_SERVER_TIME = "ELASTIC_QUERY_SERVER_TIME";
    private static final String QUERY_TOTAL_TIME = "ELASTIC_QUERY_TOTAL_TIME";

    private static final String QUERY_TIMED_OUT_RATE = "ELASTIC_QUERY_TIMED_OUT_RATE";
    private static final String QUERY_FAILED_RATE = "ELASTIC_QUERY_FAILED_RATE";

    private final Function<Map<String, String>, MeterStats> queryRate;
    private final Function<Map<String, String>, MeterStats> queryInternalRate;

    private final Function<Map<String, String>, HistogramStats> queryHitsHistogram;
    private final Function<Map<String, String>, TimerStats> queryServerTimer;
    private final Function<Map<String, String>, TimerStats> queryTotalTimer;

    private final Function<Map<String, String>, MeterStats> queryTimedOutRate;
    private final Function<Map<String, String>, MeterStats> queryFailedRate;

    public ElasticMetricHandler(StatisticsProvider sp) {
        BiFunction<String, Map<String, String>, String> metricName = (name, labels) -> labels.entrySet().stream().reduce(name,
                (n, e) -> n + ";" + e.getKey() + "=" + e.getValue(),
                (n1, n2) -> n1 + n2);

        queryRate = labels -> sp.getMeter(metricName.apply(QUERY_RATE, labels), StatsOptions.METRICS_ONLY);
        queryInternalRate = labels -> sp.getMeter(metricName.apply(QUERY_INTERNAL_RATE, labels), StatsOptions.METRICS_ONLY);

        queryHitsHistogram = labels -> sp.getHistogram(metricName.apply(QUERY_HITS, labels), StatsOptions.METRICS_ONLY);
        queryServerTimer = labels -> sp.getTimer(metricName.apply(QUERY_SERVER_TIME, labels), StatsOptions.METRICS_ONLY);
        queryTotalTimer = labels -> sp.getTimer(metricName.apply(QUERY_TOTAL_TIME, labels), StatsOptions.METRICS_ONLY);

        queryTimedOutRate = labels -> sp.getMeter(metricName.apply(QUERY_TIMED_OUT_RATE, labels), StatsOptions.METRICS_ONLY);
        queryFailedRate = labels -> sp.getMeter(metricName.apply(QUERY_FAILED_RATE, labels), StatsOptions.METRICS_ONLY);
    }

    /**
     * Tracks a new query using two metrics:
     * <ul>
     *     <li>{@code QUERY_RATE}</li>
     *     <li>{@code QUERY_INTERNAL_RATE}</li>
     * </ul>
     *
     * @param indexPath the index path passed as metric label
     * @param isRootQuery if {@code false} only {@code QUERY_INTERNAL_RATE} gets incremented
     */
    public void markQuery(String indexPath, boolean isRootQuery) {
        Map<String, String> labels = Collections.singletonMap("index", indexPath);
        if (isRootQuery) {
            queryRate.apply(labels).mark();
        }
        queryInternalRate.apply(labels).mark();
    }

    /**
     * Measures a single query execution
     *
     * @param indexPath the index path passed as metric label
     * @param hits the number of hits in the result set
     * @param serverTimeMs the Elastic server time in milliseconds
     * @param totalTimeMs the complete query execution time
     * @param timedOut Elastic could time out while returning partial results. When {@code true} these
     *                 occurrences get tracked
     */
    public void measureQuery(String indexPath, int hits, long serverTimeMs, long totalTimeMs, boolean timedOut) {
        Map<String, String> labels = Collections.singletonMap("index", indexPath);
        queryHitsHistogram.apply(labels).update(hits);
        queryServerTimer.apply(labels).update(serverTimeMs, TimeUnit.MILLISECONDS);
        queryTotalTimer.apply(labels).update(totalTimeMs, TimeUnit.MILLISECONDS);
        if (timedOut) {
            queryTimedOutRate.apply(labels).mark();
        }
    }

    /**
     * Measures a failed query execution
     *
     * @param indexPath the index path passed as metric label
     * @param totalTimeMs the total execution time
     */
    public void measureFailedQuery(String indexPath, long totalTimeMs) {
        Map<String, String> labels = Collections.singletonMap("index", indexPath);
        queryFailedRate.apply(labels).mark();
        queryTotalTimer.apply(labels).update(totalTimeMs, TimeUnit.MILLISECONDS);
    }
}
