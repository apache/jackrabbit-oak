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
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for InferenceServiceMetrics.
 */
public class InferenceServiceMetricsTest {

    private static final String TEST_SERVICE_KEY = "testService";
    private static final int TEST_CACHE_SIZE = 100;
    private LogCustomizer logCustomizer;
    private ScheduledExecutorService executorService;
    private StatisticsProvider statisticsProvider;

    @Before
    public void setUp() {
        logCustomizer = LogCustomizer
            .forLogger(InferenceServiceMetrics.class.getName())
            .enable(Level.INFO)
            .enable(Level.DEBUG)
            .create();
        logCustomizer.starting();

        executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Statistics-Test-Thread-" + UUID.randomUUID());
            t.setDaemon(true);
            return t;
        });
        statisticsProvider = new DefaultStatisticsProvider(executorService);
    }

    @After
    public void tearDown() throws Exception {
        if (logCustomizer != null) {
            logCustomizer.finished();
        }

        if (executorService != null) {
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
            if (!executorService.isTerminated()) {
                executorService.shutdownNow();
            }
        }
    }

    @Test
    public void testRequestMeters() {
        TestInferenceServiceMetrics metrics = new TestInferenceServiceMetrics("request");

        // Verify initial state
        assertEquals(0L, metrics.getDirectMeter(InferenceServiceMetrics.TOTAL_REQUESTS).getCount());
        assertEquals(0L, metrics.getDirectMeter(InferenceServiceMetrics.INFERENCE_REQUEST_ERRORS).getCount());

        // Start a request and verify the total requests meter is marked
        TimerStats.Context context = metrics.requestStarted();
        assertEquals(1L, metrics.getDirectMeter(InferenceServiceMetrics.TOTAL_REQUESTS).getCount());

        // Complete the request and verify the meter count remains the same
        // No additional meters should be marked for completion
        metrics.requestCompleted(100, context);
        assertEquals(1L, metrics.getDirectMeter(InferenceServiceMetrics.TOTAL_REQUESTS).getCount());
        assertEquals(0L, metrics.getDirectMeter(InferenceServiceMetrics.INFERENCE_REQUEST_ERRORS).getCount());

        // Start another request, but this time record an error
        context = metrics.requestStarted();
        metrics.requestError(50, context);

        // Verify both total requests and error meters are incremented
        assertEquals(2L, metrics.getDirectMeter(InferenceServiceMetrics.TOTAL_REQUESTS).getCount());
        assertEquals(1L, metrics.getDirectMeter(InferenceServiceMetrics.INFERENCE_REQUEST_ERRORS).getCount());

        // Test error without context
        metrics.requestError();
        assertEquals(2L, metrics.getDirectMeter(InferenceServiceMetrics.INFERENCE_REQUEST_ERRORS).getCount());
    }

    @Test
    public void testCacheMeters() {
        TestInferenceServiceMetrics metrics = new TestInferenceServiceMetrics("cache");

        // Verify initial state
        assertEquals(0L, metrics.getDirectMeter(InferenceServiceMetrics.CACHE_HITS).getCount());
        assertEquals(0L, metrics.getDirectMeter(InferenceServiceMetrics.CACHE_MISSES).getCount());
        assertEquals(0.0, metrics.getCacheHitRate(), 0.01);

        // Record cache hits and misses
        for (int i = 0; i < 7; i++) metrics.cacheHit();
        for (int i = 0; i < 3; i++) metrics.cacheMiss();

        // Verify meters
        assertEquals(7L, metrics.getDirectMeter(InferenceServiceMetrics.CACHE_HITS).getCount());
        assertEquals(3L, metrics.getDirectMeter(InferenceServiceMetrics.CACHE_MISSES).getCount());
        assertEquals(70.0, metrics.getCacheHitRate(), 0.01);

        // Add more hits and verify meters are incremented correctly
        for (int i = 0; i < 3; i++) metrics.cacheHit();
        assertEquals(10L, metrics.getDirectMeter(InferenceServiceMetrics.CACHE_HITS).getCount());
        assertEquals(3L, metrics.getDirectMeter(InferenceServiceMetrics.CACHE_MISSES).getCount());
        assertEquals(10L / 13.0 * 100.0, metrics.getCacheHitRate(), 0.01);
    }

    @Test
    public void testMultipleRequestsAndErrors() {
        TestInferenceServiceMetrics metrics = new TestInferenceServiceMetrics("multi");

        // Record successful requests
        for (int i = 0; i < 5; i++) {
            TimerStats.Context context = metrics.requestStarted();
            metrics.requestCompleted(100, context);
        }

        // Record error requests
        for (int i = 0; i < 3; i++) {
            TimerStats.Context context = metrics.requestStarted();
            metrics.requestError(200, context);
        }

        // Verify meters
        assertEquals(8L, metrics.getDirectMeter(InferenceServiceMetrics.TOTAL_REQUESTS).getCount());
        assertEquals(3L, metrics.getDirectMeter(InferenceServiceMetrics.INFERENCE_REQUEST_ERRORS).getCount());

        // Verify error rate through getMetrics() method
        double errorRate = (Double) metrics.getMetrics().get(InferenceServiceMetrics.INFERENCE_ERROR_RATE);
        assertEquals(3.0 / 8.0 * 100.0, errorRate, 0.01);
    }

    @Test
    public void testMetricsMap() {
        TestInferenceServiceMetrics metrics = new TestInferenceServiceMetrics("map");

        // Record some data
        for (int i = 0; i < 8; i++) metrics.cacheHit();
        for (int i = 0; i < 2; i++) metrics.cacheMiss();

        TimerStats.Context context = metrics.requestStarted();
        metrics.requestCompleted(100, context);

        context = metrics.requestStarted();
        metrics.requestError(50, context);

        // Get metrics map and verify its content
        var metricsMap = metrics.getMetrics();

        assertEquals(2L, metricsMap.get("totalRequests"));
        assertEquals(8L, metricsMap.get("cacheHits"));
        assertEquals(2L, metricsMap.get("cacheMisses"));
        assertEquals(1L, metricsMap.get("requestErrors"));

        // There's a difference in how hit rate is calculated in getMetrics()
        // In the implementation, it uses total hits/(total requests) if total > 0,
        // not hits/(hits+misses) as in getCacheHitRate()
        double hitRateFromMap = (Double) metricsMap.get(InferenceServiceMetrics.INFERENCE_CACHE_HIT_RATE);
        // Verify that we're using the correct calculation method
        long hits = metrics.getDirectMeter(InferenceServiceMetrics.CACHE_HITS).getCount();
        long totalReq = metrics.getDirectMeter(InferenceServiceMetrics.TOTAL_REQUESTS).getCount();
        double calculatedRate = totalReq > 0 ? (hits * 100.0 / 2) : 0.0;
        assertEquals(calculatedRate, hitRateFromMap, 0.01);

        assertEquals(TEST_CACHE_SIZE, metricsMap.get(InferenceServiceMetrics.INFERENCE_CACHE_SIZE));
        assertEquals(50.0, (Double) metricsMap.get(InferenceServiceMetrics.INFERENCE_ERROR_RATE), 0.01);
    }

    @Test
    public void testMetricsLogging() {
        TestInferenceServiceMetrics metrics = new TestInferenceServiceMetrics("logging");

        // Generate metrics
        for (int i = 0; i < 5; i++) {
            TimerStats.Context context = metrics.requestStarted();
            metrics.requestCompleted(100, context);
        }
        for (int i = 0; i < 7; i++) metrics.cacheHit();
        for (int i = 0; i < 3; i++) metrics.cacheMiss();

        // Log metrics
        metrics.logMetricsSummary();

        // Verify logging
        List<String> logs = logCustomizer.getLogs();
        assertFalse("Should have logged metrics", logs.isEmpty());

        String logMessage = logs.get(logs.size() - 1);
        assertTrue("Log should contain service metrics", logMessage.contains("Inference service metrics"));
        // Validation against the actual output format of InferenceServiceMetrics
        assertTrue("Log should contain request count", logMessage.contains("requests="));
        assertTrue("Log should contain hit rate", logMessage.contains("hitRate="));
        assertTrue("Log should contain error rate", logMessage.contains("errorRate="));
    }

    private class TestInferenceServiceMetrics extends InferenceServiceMetrics {
        private final String testPrefix;

        public TestInferenceServiceMetrics(String testPrefix) {
            super(statisticsProvider, TEST_SERVICE_KEY, TEST_CACHE_SIZE);
            this.testPrefix = testPrefix;
        }

        @Override
        protected String getMetricName(String baseName) {
            // This method is called with metricsServiceKey + ";" + name, so we need to preserve that format
            // but still add our test prefix for uniqueness
            return testPrefix + "_" + baseName;
        }

        // Methods to directly access the stats for verification
        public CounterStats getDirectCounter(String name) {
            // Format the name the same way it's done in the parent class
            return statisticsProvider.getCounterStats(getMetricName(TEST_SERVICE_KEY + ";" + name), StatsOptions.DEFAULT);
        }

        public MeterStats getDirectMeter(String name) {
            // Format the name the same way it's done in the parent getMeter() method
            return statisticsProvider.getMeter(getMetricName(TEST_SERVICE_KEY + ";" + name), StatsOptions.DEFAULT);
        }
    }
} 