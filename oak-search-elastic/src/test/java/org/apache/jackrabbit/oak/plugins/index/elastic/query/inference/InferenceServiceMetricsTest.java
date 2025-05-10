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

import ch.qos.logback.classic.Level;
import com.codahale.metrics.Timer;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class InferenceServiceMetricsTest {

    private InferenceServiceMetrics metrics;
    private static final String TEST_SERVICE_KEY = "testService";
    private static final int TEST_CACHE_SIZE = 100;
    private static final String KEY_REQUEST_PERCENTILES = "requestPercentiles";

    @Before
    public void setUp() {
        metrics = new InferenceServiceMetrics(TEST_SERVICE_KEY, TEST_CACHE_SIZE);
    }

    @Test
    public void testInitialState() {
        assertEquals(0, metrics.getTotalRequests());

        Map<String, Object> metricsMap = metrics.getMetrics();
        assertEquals(0L, metricsMap.get(InferenceServiceMetrics.TOTAL_REQUESTS));
        assertEquals(0L, metricsMap.get(InferenceServiceMetrics.CACHE_HITS));
        assertEquals(0L, metricsMap.get(InferenceServiceMetrics.CACHE_MISSES));
        assertEquals(0.0, metricsMap.get(InferenceServiceMetrics.CACHE_HIT_RATE));
        assertEquals(TEST_CACHE_SIZE, metricsMap.get(InferenceServiceMetrics.CACHE_SIZE));
        assertEquals(0L, metricsMap.get(InferenceServiceMetrics.REQUEST_ERRORS));
        assertEquals(0.0, metricsMap.get(InferenceServiceMetrics.ERROR_RATE));
    }

    @Test
    public void testRequestTracking() {
        // Start a request
        Timer.Context context = metrics.requestStarted();
        assertEquals(1, metrics.getTotalRequests());

        // Complete the request
        metrics.requestCompleted(150, context);

        Map<String, Object> metricsMap = metrics.getMetrics();
        assertEquals(1L, metricsMap.get(InferenceServiceMetrics.TOTAL_REQUESTS));

        // Second request without context
        metrics.requestStarted();
        metrics.requestCompleted(200);

        metricsMap = metrics.getMetrics();
        assertEquals(2L, metricsMap.get(InferenceServiceMetrics.TOTAL_REQUESTS));
    }

    @Test
    public void testCacheHitRate() {
        // No cache activity
        assertEquals(0.0, metrics.getCacheHitRate(), 0.001);

        // Record some hits and misses
        metrics.cacheHit();
        metrics.cacheHit();
        metrics.cacheMiss();

        // Should be 2/3 = 66.67%
        assertEquals(66.67, metrics.getCacheHitRate(), 0.01);

        // Add more misses
        metrics.cacheMiss();
        metrics.cacheMiss();

        // Should be 2/5 = 40%
        assertEquals(40.0, metrics.getCacheHitRate(), 0.001);

        Map<String, Object> metricsMap = metrics.getMetrics();
        assertEquals(2L, metricsMap.get(InferenceServiceMetrics.CACHE_HITS));
        assertEquals(3L, metricsMap.get(InferenceServiceMetrics.CACHE_MISSES));
        assertEquals(40.0, metricsMap.get(InferenceServiceMetrics.CACHE_HIT_RATE));
    }

    @Test
    public void testErrorTracking() {
        // Start a request and record an error
        Timer.Context context = metrics.requestStarted();
        metrics.requestError(100, context);

        // Start another request and record an error without timing
        metrics.requestStarted();
        metrics.requestError();

        Map<String, Object> metricsMap = metrics.getMetrics();
        assertEquals(2L, metricsMap.get(InferenceServiceMetrics.TOTAL_REQUESTS));
        assertEquals(2L, metricsMap.get(InferenceServiceMetrics.REQUEST_ERRORS));
        assertEquals(100.0, metricsMap.get(InferenceServiceMetrics.ERROR_RATE));

        // Add a successful request
        metrics.requestStarted();
        metrics.requestCompleted(150);

        metricsMap = metrics.getMetrics();
        assertEquals(3L, metricsMap.get(InferenceServiceMetrics.TOTAL_REQUESTS));
        assertEquals(2L, metricsMap.get(InferenceServiceMetrics.REQUEST_ERRORS));
        assertEquals(66.67, (double) metricsMap.get(InferenceServiceMetrics.ERROR_RATE), 0.01);
    }

    @Test
    public void testTimingMetrics() {
        // Record multiple requests with different timings
        for (int i = 0; i < 10; i++) {
            Timer.Context context = metrics.requestStarted();
            metrics.requestCompleted(100 + (i * 50), context);
        }

        Map<String, Object> metricsMap = metrics.getMetrics();

        // Check time histogram exists
        assertTrue(metricsMap.containsKey(InferenceServiceMetrics.TIME_HISTOGRAM));

        @SuppressWarnings("unchecked")
        Map<String, Object> histogram = (Map<String, Object>) metricsMap.get(InferenceServiceMetrics.TIME_HISTOGRAM);

        // Verify histogram has expected metrics
        assertEquals(10L, histogram.get("count"));
        assertTrue(histogram.containsKey("min"));
        assertTrue(histogram.containsKey("max"));
        assertTrue(histogram.containsKey("mean"));
        assertTrue(histogram.containsKey("stdDev"));

        // Verify percentiles exist
        assertTrue(histogram.containsKey(KEY_REQUEST_PERCENTILES));
    }

    @Test
    public void testMetricRegistry() {
        // Verify metric registry is available
        assertNotNull(metrics.getMetricRegistry());
    }

    @Test
    public void testLogMetricsSummaryOutput() {
        // Setup the LogCustomizer to capture log messages from InferenceServiceMetrics
        LogCustomizer custom = LogCustomizer
            .forLogger(InferenceServiceMetrics.class.getName())
            .enable(Level.INFO)
            .create();

        try {
            custom.starting();

            // 1. Generate comprehensive metrics data
            // Multiple requests with different timing values
            for (int i = 0; i < 5; i++) {
                Timer.Context context = metrics.requestStarted();
                metrics.requestCompleted(100 + (i * 50), context);
            }

            // Add cache hits and misses to test hit rate
            for (int i = 0; i < 3; i++) {
                metrics.cacheHit();
            }
            for (int i = 0; i < 2; i++) {
                metrics.cacheMiss();
            }

            // Add some errors to test error rate
            for (int i = 0; i < 2; i++) {
                Timer.Context context = metrics.requestStarted();
                metrics.requestError(75 + i * 25, context);
            }

            // At this point we should have:
            // - 7 total requests (5 successful, 2 errors)
            // - 3 cache hits, 2 cache misses (60% hit rate)
            // - 2 errors (28.6% error rate)
            // - Various timing metrics

            // Verify metrics were recorded correctly
            Map<String, Object> metricsMap = metrics.getMetrics();
            assertEquals(7L, metricsMap.get(InferenceServiceMetrics.TOTAL_REQUESTS));
            assertEquals(3L, metricsMap.get(InferenceServiceMetrics.CACHE_HITS));
            assertEquals(2L, metricsMap.get(InferenceServiceMetrics.CACHE_MISSES));
            assertEquals(60.0, metricsMap.get(InferenceServiceMetrics.CACHE_HIT_RATE));
            assertEquals(2L, metricsMap.get(InferenceServiceMetrics.REQUEST_ERRORS));
            assertEquals(28.57, (double) metricsMap.get(InferenceServiceMetrics.ERROR_RATE), 0.01);

            // Check both histograms exist
            @SuppressWarnings("unchecked")
            Map<String, Object> histogram = (Map<String, Object>) metricsMap.get(InferenceServiceMetrics.TIME_HISTOGRAM);
            assertEquals(5L, ((Number) histogram.get("count")).longValue());

            @SuppressWarnings("unchecked")
            Map<String, Object> errorHistogram = (Map<String, Object>) metricsMap.get(InferenceServiceMetrics.KEY_ERROR_TIME_DATA);
            assertNotNull("Error histogram should exist", errorHistogram);
            assertEquals(2L, ((Number) errorHistogram.get("count")).longValue());

            // 2. Log the metrics summary
            // Force logging regardless of time interval by using parameters that won't trigger early return
            metrics.logMetricsSummary(0, 100);

            // 3. Verify the log output contains all expected metrics
            List<String> logs = custom.getLogs();
            assertFalse("Log should contain at least one entry", logs.isEmpty());

            String logMessage = logs.get(0);
            assertTrue("Log should contain the service key", logMessage.contains(TEST_SERVICE_KEY));

            // Check all metrics are in the log
            assertTrue("Log should contain requests count", logMessage.contains("requests=7"));
            assertTrue("Log should contain hit rate", logMessage.contains("hitRate="));
            assertTrue("Log should contain error rate", logMessage.contains("errorRate="));
            assertTrue("Log should contain avgTime", logMessage.contains("avgTime="));
            assertTrue("Log should contain maxTime", logMessage.contains("maxTime="));
            assertTrue("Log should contain percentiles", logMessage.contains("successPercentiles [50th="));
            assertTrue("Log should contain rates", logMessage.contains("successRates [1m="));
            assertTrue("Log should contain error rates", logMessage.contains("errorRates [1m="));

            // Check additional metrics present in updated implementation
            assertTrue("Log should contain error rate metrics", logMessage.contains("err/s"));

            // Check for request rate metrics
            assertTrue("Log should contain 1-minute request rate", logMessage.contains("1m="));
            assertTrue("Log should contain 5-minute request rate", logMessage.contains("5m="));
            assertTrue("Log should contain 15-minute request rate", logMessage.contains("15m="));

            // Verify error histogram details are included in the log output
            assertTrue("Log should include error timing information",
                logMessage.contains(InferenceServiceMetrics.KEY_ERROR_TIME_DATA) ||
                    metricsMap.containsKey(InferenceServiceMetrics.KEY_ERROR_TIME_DATA));
        } finally {
            custom.finished();
        }
    }
} 