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
package org.apache.jackrabbit.oak.plugins.document.util;

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.ThrottlingMetrics;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoThrottlingMetrics.of;
import static org.junit.Assert.assertEquals;

/**
 * Junit for {@link ThrottlingDocumentStoreWrapper}
 */
public class ThrottlingDocumentStoreWrapperTest {

    private final ThrottlingDocumentStoreWrapper docStore = new ThrottlingDocumentStoreWrapper(new MemoryDocumentStore());

    @Test
    public void testDefaultThrottlingMetrics() {
        DocumentStore store = new ThrottlingDocumentStoreWrapper(new MemoryDocumentStore());
        ThrottlingMetrics metrics = store.throttlingMetrics();
        assertEquals(0.0, metrics.currValue(), 0.00001);
        assertEquals(0, metrics.throttlingTime());
        assertEquals(Integer.MAX_VALUE, metrics.threshold());
    }

    @Test
    public void testNoThrottling() {
        ThrottlingMetrics throttlingMetrics = of(new AtomicDouble(11), 10, 100);
        long throttleTime = docStore.getThrottleTime(throttlingMetrics);
        assertEquals(0L, throttleTime);
    }

    @Test
    public void testNoThrottling_2() {
        ThrottlingMetrics throttlingMetrics = of(new AtomicDouble(10.002), 10, 100);
        long throttleTime = docStore.getThrottleTime(throttlingMetrics);
        assertEquals(0L, throttleTime);
    }

    @Test
    public void testThrottlingNormalPace() {
        ThrottlingMetrics throttlingMetrics = of(new AtomicDouble(10.001), 10, 10);
        long throttleTime = docStore.getThrottleTime(throttlingMetrics);
        assertEquals(10L, throttleTime);
    }

    @Test
    public void testThrottlingNormalPace_2() {
        ThrottlingMetrics throttlingMetrics = of(new AtomicDouble(5.001), 10, 10);
        long throttleTime = docStore.getThrottleTime(throttlingMetrics);
        assertEquals(10L, throttleTime);
    }

    @Test
    public void testThrottlingDoublePace() {
        ThrottlingMetrics throttlingMetrics = of(new AtomicDouble(5.0), 10, 10);
        long throttleTime = docStore.getThrottleTime(throttlingMetrics);
        assertEquals(20L, throttleTime);
    }

    @Test
    public void testThrottlingDoublePace_2() {
        ThrottlingMetrics throttlingMetrics = of(new AtomicDouble(5.0001), 10, 10);
        long throttleTime = docStore.getThrottleTime(throttlingMetrics);
        assertEquals(20L, throttleTime);
    }

    @Test
    public void testThrottlingDoublePace_3() {
        ThrottlingMetrics throttlingMetrics = of(new AtomicDouble(5.001), 20, 10);
        long throttleTime = docStore.getThrottleTime(throttlingMetrics);
        assertEquals(20L, throttleTime);
    }

    @Test
    public void testThrottlingQuadruplePace() {
        ThrottlingMetrics throttlingMetrics = of(new AtomicDouble(5.0001), 20, 10);
        long throttleTime = docStore.getThrottleTime(throttlingMetrics);
        assertEquals(40L, throttleTime);
    }

    @Test
    public void testThrottlingQuadruplePace_2() {
        ThrottlingMetrics throttlingMetrics = of(new AtomicDouble(5.001), 40, 10);
        long throttleTime = docStore.getThrottleTime(throttlingMetrics);
        assertEquals(40L, throttleTime);
    }

    @Test
    public void testThrottlingOctagonalPace() {
        ThrottlingMetrics throttlingMetrics = of(new AtomicDouble(5.0001), 80, 10);
        long throttleTime = docStore.getThrottleTime(throttlingMetrics);
        assertEquals(80L, throttleTime);
    }

    @Test
    public void testThrottlingOctagonalPace_2() {
        ThrottlingMetrics throttlingMetrics = of(new AtomicDouble(5.0001), 160, 10);
        long throttleTime = docStore.getThrottleTime(throttlingMetrics);
        assertEquals(80L, throttleTime);
    }
}