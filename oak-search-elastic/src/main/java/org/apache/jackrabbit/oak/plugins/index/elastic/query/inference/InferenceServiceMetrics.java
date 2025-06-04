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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Collects and reports metrics for the inference service.
 */
public class InferenceServiceMetrics {
    final static Logger LOG = LoggerFactory.getLogger(InferenceServiceMetrics.class);

    // Tracks the last time metrics were logged
    private long lastLogTimeMillis;
    private String metricsServiceKey;
    private int cacheSize;
    private StatisticsProvider statisticsProvider;

    // Metric constants for both output property names and registry base names
    public static final String TOTAL_REQUESTS = "INFERENCE_TOTAL_REQUESTS";
    public static final String CACHE_HITS = "INFERENCE_CACHE_HITS";
    public static final String CACHE_MISSES = "INFERENCE_CACHE_MISSES";
    public static final String INFERENCE_CACHE_HIT_RATE = "INFERENCE_CACHE_HIT_RATE";
    public static final String INFERENCE_CACHE_SIZE = "INFERENCE_CACHE_SIZE";
    public static final String INFERENCE_REQUEST_ERRORS = "INFERENCE_REQUEST_ERRORS";
    public static final String INFERENCE_ERROR_RATE = "INFERENCE_ERROR_RATE";
    public static final String INFERENCE_REQUEST_TIMES = "INFERENCE_REQUEST_TIMES";
    public static final String INFERENCE_ERROR_TIMES = "INFERENCE_ERROR_TIMES";

    /**
     * Creates a new InferenceServiceMetrics instance
     *
     * @param statisticsProvider The statistics provider to use
     * @param metricsServiceKey  The service key for logging
     * @param cacheSize          The configured cache size
     */
    public InferenceServiceMetrics(StatisticsProvider statisticsProvider, String metricsServiceKey, int cacheSize) {
        this.lastLogTimeMillis = System.currentTimeMillis();
        this.metricsServiceKey = metricsServiceKey;
        this.cacheSize = cacheSize;
        this.statisticsProvider = statisticsProvider;
    }

    /**
     * Records a request start and returns a TimerStats.Context
     *
     * @return a TimerStats.Context that should be stopped when the request completes
     */
    public TimerStats.Context requestStarted() {
        getMeter(TOTAL_REQUESTS).mark();
        // Start timing
        return getTimer(INFERENCE_REQUEST_TIMES).time();
    }

    /**
     * Records a cache hit
     */
    public void cacheHit() {
        getMeter(CACHE_HITS).mark();
    }

    /**
     * Records a cache miss
     */
    public void cacheMiss() {
        getMeter(CACHE_MISSES).mark();
    }

    /**
     * Records a request error
     *
     * @param timeMillis   Time taken before the error occurred in milliseconds
     * @param timerContext Timer context to stop, if available (can be null)
     */
    public void requestError(long timeMillis, TimerStats.Context timerContext) {
        getMeter(INFERENCE_REQUEST_ERRORS).mark();
        // Stop the timer context if provided
        if (timerContext != null) {
            timerContext.stop();
        }
        LOG.debug("Request error occurred after {} ms for {}", timeMillis, metricsServiceKey);
    }

    /**
     * Records a request error without timing information
     */
    public void requestError() {
        getMeter(INFERENCE_REQUEST_ERRORS).mark();
        LOG.debug("Request error occurred (timing unknown) for {}", metricsServiceKey);
    }

    /**
     * Records a request completion
     *
     * @param timeMillis   Time taken to complete the request in milliseconds
     * @param timerContext Timer context to stop, if available (can be null)
     */
    public void requestCompleted(long timeMillis, TimerStats.Context timerContext) {
        // Stop timer context if provided
        if (timerContext != null) {
            timerContext.stop();
        } else {
            // If no context was provided, update the timer directly
            getTimer(INFERENCE_REQUEST_TIMES).update(timeMillis, TimeUnit.MILLISECONDS);
        }
        LOG.debug("Request completed in {} ms for {}", timeMillis, metricsServiceKey);
    }

    /**
     * Returns the cache hit rate percentage (0-100)
     *
     * @return The cache hit rate as a percentage
     */
    public double getCacheHitRate() {
        long hits = getMeter(CACHE_HITS).getCount();
        long misses = getMeter(CACHE_MISSES).getCount();
        long total = hits + misses;
        return total > 0 ? (hits * 100.0 / total) : 0.0;
    }

    /**
     * Returns metrics as a map for monitoring
     *
     * @return A map of metric names to values
     */
    public Map<String, Object> getMetrics() {
        Map<String, Object> metricsMap = new LinkedHashMap<>();

        // Get base metrics
        long total = getMeter(TOTAL_REQUESTS).getCount();
        long hits = getMeter(CACHE_HITS).getCount();
        long misses = getMeter(CACHE_MISSES).getCount();
        long errors = getMeter(INFERENCE_REQUEST_ERRORS).getCount();
        // Add to map
        metricsMap.put("totalRequests", total);
        metricsMap.put("cacheHits", hits);
        metricsMap.put("cacheMisses", misses);
        metricsMap.put("requestErrors", errors);
        double hitRate = total > 0 ? (hits * 100.0 / total) : 0.0;
        metricsMap.put(INFERENCE_CACHE_HIT_RATE, hitRate);
        metricsMap.put(INFERENCE_CACHE_SIZE, cacheSize);
        metricsMap.put(INFERENCE_ERROR_RATE, total > 0 ? (errors * 100.0 / total) : 0.0);
        return metricsMap;
    }

    /**
     * Logs a summary of the current metrics
     */
    public void logMetricsSummary() {
        logMetricsSummary(0);
    }

    /**
     * Logs a summary of the current metrics if the interval has passed
     *
     * @param intervalMillis Minimum interval between logs in milliseconds
     */
    public void logMetricsSummary(long intervalMillis) {
        // Skip if interval has not passed
        if (lastLogTimeMillis + intervalMillis > System.currentTimeMillis()) {
            return;
        }
        // Get metrics
        Map<String, Object> metrics = getMetrics();
        // Build log message
        StringBuilder logMessage = new StringBuilder();
        logMessage.append("Inference service metrics");
        if (metricsServiceKey != null) {
            logMessage.append(" for ServiceKey '").append(metricsServiceKey).append("'");
        }
        logMessage.append(": requests=").append(metrics.get("totalRequests"))
            .append(", hitRate=").append(metrics.get(INFERENCE_CACHE_HIT_RATE)).append("%")
            .append(", errorRate=").append(metrics.get(INFERENCE_ERROR_RATE)).append("%");
        // Log the message
        lastLogTimeMillis = System.currentTimeMillis();
        LOG.info(logMessage.toString());
    }

    private MeterStats getMeter(String name) {
        return statisticsProvider.getMeter(getMetricName(this.metricsServiceKey + "-" + name), StatsOptions.DEFAULT);
    }

    private TimerStats getTimer(String name) {
        return statisticsProvider.getTimer(this.metricsServiceKey + "-" + getMetricName(name), StatsOptions.DEFAULT);
    }

    /**
     * Returns the metric name to use with the StatisticsProvider.
     * This method can be overridden by tests to provide unique metric names.
     *
     * @param baseName The base metric name
     * @return The actual metric name to use
     */
    protected String getMetricName(String baseName) {
        return baseName;
    }
} 