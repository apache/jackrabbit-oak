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

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RateLimiterTuningIntrospectionTest {

    @Before
    public void setUp() throws Exception {
        clearProperties();
        resetSourceRegistry();
    }

    @After
    public void tearDown() throws Exception {
        clearProperties();
        resetSourceRegistry();
    }

    @Test
    public void reportsDefaultsWhenNoOverrideIsConfigured() {
        assertEquals("defaults", RateLimiterTuningIntrospection.source());
    }

    @Test
    public void reportsSystemPropertiesWhenAnyOverrideIsConfigured() {
        System.setProperty(RateLimiter.PROP_ENABLED, "false");

        assertEquals("system-properties", RateLimiterTuningIntrospection.source());
    }

    private static void clearProperties() {
        System.clearProperty(RateLimiter.PROP_ENABLED);
        System.clearProperty(RateLimiter.PROP_REQUESTS_PER_SECOND);
        System.clearProperty(RateLimiter.PROP_BURST_SIZE);
        System.clearProperty(RateLimiter.PROP_GLOBAL_RPS);
        System.clearProperty(RateLimiter.PROP_WRITE_RPS);
        System.clearProperty(RateLimiter.PROP_WARN_LOGGING_ENABLED);
        System.clearProperty(RateLimiter.PROP_WARN_LOG_INTERVAL_MS);
        System.clearProperty(RateLimiter.PROP_WARN_LOG_SAMPLE_SIZE);
    }

    private static void resetSourceRegistry() throws Exception {
        Field field = RateLimiterTuningSourceRegistry.class.getDeclaredField("SOURCE");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<String> source = (AtomicReference<String>) field.get(null);
        source.set("system-properties");
    }
}
