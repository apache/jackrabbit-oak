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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Collects and reports metrics for the inference service.
 */
public class InferenceServiceMetrics {
    final static Logger LOG = LoggerFactory.getLogger(InferenceServiceMetrics.class);

    // Tracks the last time metrics were logged
    private long lastLogTimeMillis;
    private String metricsServiceKey;
    private int cacheSize;

    // Metric constants for both output property names and registry base names
    public static final String TOTAL_REQUESTS = "totalRequests";
    public static final String CACHE_HITS = "cacheHits";
    public static final String CACHE_MISSES = "cacheMisses";
    public static final String CACHE_HIT_RATE = "cacheHitRate";
    public static final String CACHE_SIZE = "cacheSize";
    public static final String REQUEST_ERRORS = "requestErrors";
    public static final String ERROR_RATE = "errorRate";
    public static final String AVG_REQUEST_TIME = "avgRequestTime";
    public static final String MAX_REQUEST_TIME = "maxRequestTime";
    public static final String TIME_HISTOGRAM = "timeHistogram";
    public static final String REQUESTS = "requests";
    public static final String ERRORS = "errors";
    public static final String TIMEOUTS = "timeouts";

    // Registry specific names (using camelCase where possible, hyphens only when needed for convention)
    public static final String REQUEST_TIMER = "requestTimer";
    public static final String REQUEST_TIMES = "requestTimes";
    public static final String CACHE_HITS_METRIC = CACHE_HITS;
    public static final String CACHE_MISSES_METRIC = CACHE_MISSES;
    public static final String TOTAL_REQUESTS_METRIC = TOTAL_REQUESTS;
    public static final String TOTAL_CACHE_HITS = "totalCacheHits";
    public static final String TOTAL_CACHE_MISSES = "totalCacheMisses";
    public static final String TOTAL_ERRORS = "totalErrors";

    // Metric constants for histogram and percentile keys
    private static final String KEY_COUNT = "count";
    private static final String KEY_MIN = "min";
    private static final String KEY_MAX = "max";
    private static final String KEY_MEAN = "mean";
    private static final String KEY_STD_DEV = "stdDev";
    private static final String KEY_PERCENTILES = "percentiles";
    private static final String KEY_REQUEST_PERCENTILES = "requestPercentiles";
    public static final String KEY_ERROR_TIME_DATA = "errorTimeData";
    private static final String ERROR_TIMES = "errorTimes";

    // Metric constants for percentile keys
    private static final String KEY_50TH = "50th";
    private static final String KEY_75TH = "75th";
    private static final String KEY_95TH = "95th";
    private static final String KEY_98TH = "98th";
    private static final String KEY_99TH = "99th";
    private static final String KEY_999TH = "999th";

    // Rate keys
    private static final String KEY_REQUEST_RATE_1M = "requestRate1m";
    private static final String KEY_REQUEST_RATE_5M = "requestRate5m";
    private static final String KEY_ERROR_RATE_1M = "errorRate1m";
    private static final String KEY_ERROR_RATE_5M = "errorRate5m";
    private static final String KEY_TIMEOUT_RATE_1M = "timeoutRate1m";
    private static final String KEY_TIMEOUT_RATE_5M = "timeoutRate5m";

    private final MetricRegistry metrics = new MetricRegistry();

    // Meters for rate measurements
    private final Meter requests;
    private final Meter hits;
    private final Meter misses;
    private final Meter errors;

    // Counters for absolute counts
    private final Counter totalRequestCounter;
    private final Counter cacheHitCounter;
    private final Counter cacheMissCounter;
    private final Counter errorCounter;

    // Timers and histograms for timing statistics
    private final Timer requestTimer;
    private final Histogram requestTimes;
    private final Histogram errorTimes;

    public InferenceServiceMetrics(String metricsServiceKey, int cacheSize) {
        this.lastLogTimeMillis = System.currentTimeMillis();
        this.metricsServiceKey = metricsServiceKey;
        this.cacheSize = cacheSize;

        // Initialize meters
        this.requests = metrics.meter(MetricRegistry.name(InferenceServiceMetrics.class, REQUESTS));
        this.hits = metrics.meter(MetricRegistry.name(InferenceServiceMetrics.class, CACHE_HITS_METRIC));
        this.misses = metrics.meter(MetricRegistry.name(InferenceServiceMetrics.class, CACHE_MISSES_METRIC));
        this.errors = metrics.meter(MetricRegistry.name(InferenceServiceMetrics.class, ERRORS));

        // Initialize counters
        this.totalRequestCounter = metrics.counter(MetricRegistry.name(InferenceServiceMetrics.class, TOTAL_REQUESTS_METRIC));
        this.cacheHitCounter = metrics.counter(MetricRegistry.name(InferenceServiceMetrics.class, TOTAL_CACHE_HITS));
        this.cacheMissCounter = metrics.counter(MetricRegistry.name(InferenceServiceMetrics.class, TOTAL_CACHE_MISSES));
        this.errorCounter = metrics.counter(MetricRegistry.name(InferenceServiceMetrics.class, TOTAL_ERRORS));

        // Initialize timers and histograms
        this.requestTimer = metrics.timer(MetricRegistry.name(InferenceServiceMetrics.class, REQUEST_TIMER));
        this.requestTimes = metrics.histogram(MetricRegistry.name(InferenceServiceMetrics.class, REQUEST_TIMES));
        this.errorTimes = metrics.histogram(MetricRegistry.name(InferenceServiceMetrics.class, ERROR_TIMES));
    }

    /**
     * Records a request start
     *
     * @return Timer.Context that should be stopped when the request completes
     */
    public Timer.Context requestStarted() {
        totalRequestCounter.inc();
        requests.mark();
        return requestTimer.time();
    }

    /**
     * Records a cache hit
     */
    public void cacheHit() {
        cacheHitCounter.inc();
        hits.mark();
    }

    /**
     * Records a cache miss
     */
    public void cacheMiss() {
        cacheMissCounter.inc();
        misses.mark();
    }

    /**
     * Records a request error
     *
     * @param timeMillis   Time taken before the error occurred in milliseconds
     * @param timerContext Timer context to stop, if available (can be null)
     */
    public void requestError(long timeMillis, Timer.Context timerContext) {
        errorCounter.inc();
        errors.mark();

        // Record time in the error timer
        errorTimes.update(timeMillis);

        // Stop the request timer context if provided (this marks the end of the entire operation, even if it's an error)
        if (timerContext != null) {
            timerContext.stop();
        }

        LOG.debug("Request error occurred after {} ms", timeMillis);
    }

    /**
     * Records a request error
     */
    public void requestError() {
        errorCounter.inc();
        errors.mark();

        // Without timing information, we'll use -1 as placeholder
        // This won't affect percentiles but will be counted in the error histogram
        errorTimes.update(-1);

        LOG.debug("Request error occurred (timing unknown)");
    }

    /**
     * Records the request completion time
     *
     * @param timeMillis   Time taken to complete the request in milliseconds
     * @param timerContext Timer context to stop, if available (can be null)
     */
    public void requestCompleted(long timeMillis, Timer.Context timerContext) {
        // Update histogram
        requestTimes.update(timeMillis);

        // Stop timer context if provided
        if (timerContext != null) {
            timerContext.stop();
        }

        LOG.debug("Request completed in {} ms", timeMillis);
    }

    /**
     * Records the request completion time
     *
     * @param timeMillis Time taken to complete the request in milliseconds
     */
    public void requestCompleted(long timeMillis) {
        requestCompleted(timeMillis, null);
    }

    /**
     * Returns the cache hit rate percentage (0-100).
     */
    public double getCacheHitRate() {
        long hits = cacheHitCounter.getCount();
        long misses = cacheMissCounter.getCount();
        long total = hits + misses;
        return total > 0 ? (hits * 100.0 / total) : 0.0;
    }

    /**
     * Returns metrics as a map for monitoring.
     */
    public Map<String, Object> getMetrics() {
        Map<String, Object> metricsMap = new LinkedHashMap<>();
        long total = totalRequestCounter.getCount();
        long hits = cacheHitCounter.getCount();
        long missesCount = cacheMissCounter.getCount();
        long errorsCount = errorCounter.getCount();

        metricsMap.put(TOTAL_REQUESTS, total);
        metricsMap.put(CACHE_HITS, hits);
        metricsMap.put(CACHE_MISSES, missesCount);
        metricsMap.put(CACHE_HIT_RATE, getCacheHitRate());
        metricsMap.put(CACHE_SIZE, cacheSize);
        metricsMap.put(REQUEST_ERRORS, errorsCount);
        metricsMap.put(ERROR_RATE, total > 0 ? (errorsCount * 100.0 / total) : 0.0);

        // Timer statistics
        Snapshot histSnapshot = requestTimes.getSnapshot();
        metricsMap.put(AVG_REQUEST_TIME, histSnapshot.getMean());
        metricsMap.put(MAX_REQUEST_TIME, histSnapshot.getMax());

        // Add histogram data
        Map<String, Object> histogramData = new LinkedHashMap<>();
        histogramData.put(KEY_COUNT, requestTimes.getCount());
        histogramData.put(KEY_MIN, histSnapshot.getMin());
        histogramData.put(KEY_MAX, histSnapshot.getMax());
        histogramData.put(KEY_MEAN, histSnapshot.getMean());
        histogramData.put(KEY_STD_DEV, histSnapshot.getStdDev());

        // Add percentiles
        Map<String, Object> percentiles = new LinkedHashMap<>();
        percentiles.put(KEY_50TH, histSnapshot.getMedian());
        percentiles.put(KEY_75TH, histSnapshot.get75thPercentile());
        percentiles.put(KEY_95TH, histSnapshot.get95thPercentile());
        percentiles.put(KEY_98TH, histSnapshot.get98thPercentile());
        percentiles.put(KEY_99TH, histSnapshot.get99thPercentile());
        percentiles.put(KEY_999TH, histSnapshot.get999thPercentile());

        histogramData.put(KEY_REQUEST_PERCENTILES, percentiles);
        metricsMap.put(TIME_HISTOGRAM, histogramData);

        // Add error histogram data
        if (errorCounter.getCount() > 0) {
            Snapshot errorHistSnapshot = errorTimes.getSnapshot();

            Map<String, Object> errorHistogramData = new LinkedHashMap<>();
            errorHistogramData.put(KEY_COUNT, errorTimes.getCount());
            errorHistogramData.put(KEY_MIN, errorHistSnapshot.getMin());
            errorHistogramData.put(KEY_MAX, errorHistSnapshot.getMax());
            errorHistogramData.put(KEY_MEAN, errorHistSnapshot.getMean());
            errorHistogramData.put(KEY_STD_DEV, errorHistSnapshot.getStdDev());

            // Add percentiles
            Map<String, Object> errorPercentiles = new LinkedHashMap<>();
            errorPercentiles.put(KEY_50TH, errorHistSnapshot.getMedian());
            errorPercentiles.put(KEY_75TH, errorHistSnapshot.get75thPercentile());
            errorPercentiles.put(KEY_95TH, errorHistSnapshot.get95thPercentile());
            errorPercentiles.put(KEY_98TH, errorHistSnapshot.get98thPercentile());
            errorPercentiles.put(KEY_99TH, errorHistSnapshot.get99thPercentile());
            errorPercentiles.put(KEY_999TH, errorHistSnapshot.get999thPercentile());

            errorHistogramData.put(KEY_PERCENTILES, errorPercentiles);
            metricsMap.put(KEY_ERROR_TIME_DATA, errorHistogramData);
        }

        // Add rates
        metricsMap.put(KEY_REQUEST_RATE_1M, requests.getOneMinuteRate());
        metricsMap.put(KEY_REQUEST_RATE_5M, requests.getFiveMinuteRate());
        metricsMap.put(KEY_ERROR_RATE_1M, errors.getOneMinuteRate());
        metricsMap.put(KEY_ERROR_RATE_5M, errors.getFiveMinuteRate());
        return metricsMap;
    }

    /**
     * Returns the total number of requests processed
     */
    public long getTotalRequests() {
        return totalRequestCounter.getCount();
    }

    /**
     * Returns the Dropwizard Metrics registry used by this class.
     * This can be used to add additional metrics or to register reporters.
     */
    public MetricRegistry getMetricRegistry() {
        return metrics;
    }

    public void logMetricsSummary() {
        logMetricsSummary(0, 0);
    }

    public void logMetricsSummary(int intervalMillis, int requestCountThreshold) {
        if (lastLogTimeMillis + intervalMillis > System.currentTimeMillis() ||
            totalRequestCounter.getCount() > requestCountThreshold) {
            return; // Skip logging if the interval has not passed
        }

        // Avoid format specifier issues by converting everything to strings
        Map<String, Object> metricsMap = getMetrics();
        double hitRate = (Double) metricsMap.get(CACHE_HIT_RATE);
        Object avgTime = metricsMap.get(AVG_REQUEST_TIME);
        double avgTimeValue = avgTime instanceof Long ? (double) (Long) avgTime : (Double) avgTime;

        // Convert timer values to doubles for safe formatting
        Snapshot timerSnapshot = requestTimes.getSnapshot();
        double median = timerSnapshot.getMedian();
        double p95 = timerSnapshot.get95thPercentile();
        double p99 = timerSnapshot.get99thPercentile();
        double maxTimer = timerSnapshot.getMax();
        double oneMinRate = requests.getOneMinuteRate();
        double fiveMinRate = requests.getFiveMinuteRate();
        double fifteenMinRate = requests.getFifteenMinuteRate();

        // Error timer values
        Snapshot errorTimerSnapshot = errorTimes.getSnapshot();
        double errorRate = (Double) metricsMap.get(ERROR_RATE);
        double errorMedian = errorTimerSnapshot.getMedian();
        double errorP95 = errorTimerSnapshot.get95thPercentile();
        double errorP99 = errorTimerSnapshot.get99thPercentile();
        double errorMaxTimer = errorTimerSnapshot.getMax();
        double errorRate1m = errors.getOneMinuteRate();
        double errorRate5m = errors.getFiveMinuteRate();
        double errorRate15m = errors.getFifteenMinuteRate();

        StringBuilder logMessage = new StringBuilder();
        logMessage.append("Inference service metrics for ").append(metricsServiceKey)
            .append(": requests=").append(metricsMap.get(TOTAL_REQUESTS))
            .append(", hitRate=").append(Double.toString(hitRate)).append("%")
            .append(", errorRate=").append(Double.toString(errorRate)).append("%")
            .append(", avgTime=").append(Double.toString(avgTimeValue)).append("ms")
            .append(", maxTime=").append(metricsMap.get(MAX_REQUEST_TIME)).append("ms")
            .append(", lastLogTime=").append(lastLogTimeMillis);

        // Add percentiles
        logMessage.append(", successPercentiles [50th=").append(Double.toString(median)).append("ms")
            .append(", 95th=").append(Double.toString(p95)).append("ms")
            .append(", 99th=").append(Double.toString(p99)).append("ms")
            .append(", max=").append(Double.toString(maxTimer)).append("ms]");

        // Add success rates
        logMessage.append(", successRates [1m=").append(Double.toString(oneMinRate)).append("req/s")
            .append(", 5m=").append(Double.toString(fiveMinRate)).append("req/s")
            .append(", 15m=").append(Double.toString(fifteenMinRate)).append("req/s]");

        // Add error rates
        logMessage.append(", errorRates [1m=").append(Double.toString(errorRate1m)).append("err/s")
            .append(", 5m=").append(Double.toString(errorRate5m)).append("err/s")
            .append(", 15m=").append(Double.toString(errorRate15m)).append("err/s]");

        // Add error percentiles
        logMessage.append(", errorPercentiles=[50th=").append(Double.toString(errorMedian)).append("ms")
            .append(", 95th=").append(Double.toString(errorP95)).append("ms")
            .append(", 99th=").append(Double.toString(errorP99)).append("ms")
            .append(", max=").append(Double.toString(errorMaxTimer)).append("ms]");

        LOG.info(logMessage.toString());
    }
} 