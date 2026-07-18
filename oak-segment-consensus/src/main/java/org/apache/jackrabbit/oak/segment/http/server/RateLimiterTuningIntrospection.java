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
package org.apache.jackrabbit.oak.segment.http.server;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Read-only introspection helpers for effective rate-limiter tuning.
 */
public final class RateLimiterTuningIntrospection {

    private static final int DEFAULT_REQUESTS_PER_SECOND = 100;
    private static final int DEFAULT_BURST_SIZE = 200;
    private static final int DEFAULT_GLOBAL_RPS = 1000;
    private static final int DEFAULT_WRITE_RPS = 10;
    private static final boolean DEFAULT_WARN_LOGGING_ENABLED = true;
    private static final long DEFAULT_WARN_LOG_INTERVAL_MS = 30_000L;
    private static final int DEFAULT_WARN_LOG_SAMPLE_SIZE = 250;

    private RateLimiterTuningIntrospection() {
    }

    public static Map<String, Object> effectiveValues() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("enabled", readBoolean(RateLimiter.PROP_ENABLED, true));
        values.put("requests_per_second", readInt(RateLimiter.PROP_REQUESTS_PER_SECOND, DEFAULT_REQUESTS_PER_SECOND));
        values.put("burst_size", readInt(RateLimiter.PROP_BURST_SIZE, DEFAULT_BURST_SIZE));
        values.put("global_rps", readInt(RateLimiter.PROP_GLOBAL_RPS, DEFAULT_GLOBAL_RPS));
        values.put("write_rps", readInt(RateLimiter.PROP_WRITE_RPS, DEFAULT_WRITE_RPS));
        values.put("warn_logging_enabled", readBoolean(RateLimiter.PROP_WARN_LOGGING_ENABLED, DEFAULT_WARN_LOGGING_ENABLED));
        values.put("warn_log_interval_ms", readLong(RateLimiter.PROP_WARN_LOG_INTERVAL_MS, DEFAULT_WARN_LOG_INTERVAL_MS));
        values.put("warn_log_sample_size", readInt(RateLimiter.PROP_WARN_LOG_SAMPLE_SIZE, DEFAULT_WARN_LOG_SAMPLE_SIZE));
        return values;
    }

    public static String source() {
        String registeredSource = RateLimiterTuningSourceRegistry.getSource();
        if (!"system-properties".equals(registeredSource)) {
            return registeredSource;
        }
        return hasSystemPropertyOverride() ? "system-properties" : "defaults";
    }

    private static boolean hasSystemPropertyOverride() {
        return System.getProperty(RateLimiter.PROP_ENABLED) != null
            || System.getProperty(RateLimiter.PROP_REQUESTS_PER_SECOND) != null
            || System.getProperty(RateLimiter.PROP_BURST_SIZE) != null
            || System.getProperty(RateLimiter.PROP_GLOBAL_RPS) != null
            || System.getProperty(RateLimiter.PROP_WRITE_RPS) != null
            || System.getProperty(RateLimiter.PROP_WARN_LOGGING_ENABLED) != null
            || System.getProperty(RateLimiter.PROP_WARN_LOG_INTERVAL_MS) != null
            || System.getProperty(RateLimiter.PROP_WARN_LOG_SAMPLE_SIZE) != null;
    }

    private static int readInt(String key, int defaultValue) {
        String raw = System.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static long readLong(String key, long defaultValue) {
        String raw = System.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException ignored) {
            return defaultValue;
        }
    }

    private static boolean readBoolean(String key, boolean defaultValue) {
        String raw = System.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(raw.trim());
    }
}
