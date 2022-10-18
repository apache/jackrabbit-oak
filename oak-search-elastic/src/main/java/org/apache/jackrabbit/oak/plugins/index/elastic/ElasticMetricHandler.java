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

import org.apache.jackrabbit.oak.plugins.metric.util.StatsProviderUtil;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.TimerStats;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

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

    private static final String INDEX_DOCUMENTS = "ELASTIC_INDEX_DOCUMENTS";
    private static final String INDEX_SIZE = "ELASTIC_INDEX_SIZE";
    private static final String INDEX_WITH_REPLICAS_SIZE = "ELASTIC_INDEX_WITH_REPLICAS_SIZE";

    private final BiFunction<String, Map<String, String>, MeterStats> meter;
    private final BiFunction<String, Map<String, String>, HistogramStats> histogram;
    private final BiFunction<String, Map<String, String>, TimerStats> timer;

    public ElasticMetricHandler(StatisticsProvider sp) {
        StatsProviderUtil statsProviderUtil = new StatsProviderUtil(sp);
        meter = statsProviderUtil.getMeterStats();
        histogram = statsProviderUtil.getHistoStats();
        timer = statsProviderUtil.getTimerStats();
    }

    /**
     * Tracks a new query using two metrics:
     * <ul>
     *     <li>{@code QUERY_RATE}</li>
     *     <li>{@code QUERY_INTERNAL_RATE}</li>
     * </ul>
     *
     * @param index the index passed as metric label
     * @param isRootQuery if {@code false} only {@code QUERY_INTERNAL_RATE} gets incremented
     */
    public void markQuery(String index, boolean isRootQuery) {
        Map<String, String> labels = Collections.singletonMap("index", index);
        if (isRootQuery) {
            meter.apply(QUERY_RATE, labels).mark();
        }
        meter.apply(QUERY_INTERNAL_RATE, labels).mark();
    }

    /**
     * Measures a single query execution
     *
     * @param index the index passed as metric label
     * @param hits the number of hits in the result set
     * @param serverTimeMs the Elastic server time in milliseconds
     * @param totalTimeMs the complete query execution time
     * @param timedOut Elastic could time out while returning partial results. When {@code true} these
     *                 occurrences get tracked
     */
    public void measureQuery(String index, int hits, long serverTimeMs, long totalTimeMs, boolean timedOut) {
        Map<String, String> labels = Collections.singletonMap("index", index);
        histogram.apply(QUERY_HITS, labels).update(hits);
        timer.apply(QUERY_SERVER_TIME, labels).update(serverTimeMs, TimeUnit.MILLISECONDS);
        timer.apply(QUERY_TOTAL_TIME, labels).update(totalTimeMs, TimeUnit.MILLISECONDS);
        if (timedOut) {
            meter.apply(QUERY_TIMED_OUT_RATE, labels).mark();
        }
    }

    /**
     * Measures a failed query execution
     *
     * @param index the index passed as metric label
     * @param totalTimeMs the total execution time
     */
    public void measureFailedQuery(String index, long totalTimeMs) {
        Map<String, String> labels = Collections.singletonMap("index", index);
        meter.apply(QUERY_FAILED_RATE, labels).mark();
        timer.apply(QUERY_TOTAL_TIME, labels).update(totalTimeMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Tracks the number of document in an index
     *
     * @param index the index passed as metric label
     * @param numDocs the current number of documents. Only top level documents are tracked
     */
    public void markDocuments(String index, long numDocs) {
        Map<String, String> labels = Collections.singletonMap("index", index);
        histogram.apply(INDEX_DOCUMENTS, labels).update(numDocs);
    }

    /**
     * Tracks the size of an index
     *
     * @param index         the index passed as metric label
     * @param primarySize   the primary shards size
     * @param storeSize     the total size in bytes. The value includes potential replicas
     */
    public void markSize(String index, long primarySize, long storeSize) {
        Map<String, String> labels = Collections.singletonMap("index", index);
        histogram.apply(INDEX_SIZE, labels).update(primarySize);
        histogram.apply(INDEX_WITH_REPLICAS_SIZE, labels).update(storeSize);
    }
}
