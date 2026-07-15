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
package org.apache.jackrabbit.oak.plugins.document.mongo;

import org.apache.jackrabbit.oak.plugins.document.Throttler;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoThrottlerFactory.exponentialThrottler;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoThrottlerFactory.extFactorThrottler;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoThrottlerFactory.noThrottler;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Junit for {@link MongoThrottlerFactory}
 */
public class MongoThrottlerFactoryTest {

    @Test
    public void testNoThrottler() {
        Throttler throttler = noThrottler();
        assertEquals(0.0, throttler.throttlingTime(), 0.001);
    }

    @Test(expected = NullPointerException.class)
    public void testExpThrottler_NPE() {
        Throttler throttler = exponentialThrottler(10, null, 10);
        fail("Shouldn't reach here");
    }

    @Test
    public void testExpThrottler() {
        Throttler throttler = exponentialThrottler(10, new AtomicReference<>(11.0), 10);
        assertEquals(0L, throttler.throttlingTime());
    }

    @Test
    public void testExpThrottler_2() {
        Throttler throttler = exponentialThrottler(10, new AtomicReference<>(10.002), 10);
        assertEquals(0L, throttler.throttlingTime());
    }

    @Test
    public void testExpThrottlerNormalPace() {
        Throttler throttler = exponentialThrottler(10, new AtomicReference<>(10.001), 10);
        assertEquals(10L, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingNormalPace_2() {
        Throttler throttler = exponentialThrottler(10, new AtomicReference<>(5.001), 10);
        assertEquals(10L, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingDoublePace() {
        Throttler throttler = exponentialThrottler(10, new AtomicReference<>(5.0), 10);
        assertEquals(20L, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingDoublePace_2() {
        Throttler throttler = exponentialThrottler(10, new AtomicReference<>(5.0001), 10);
        assertEquals(20L, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingDoublePace_3() {
        Throttler throttler = exponentialThrottler(20, new AtomicReference<>(5.001), 10);
        assertEquals(20L, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingQuadruplePace() {
        Throttler throttler = exponentialThrottler(20, new AtomicReference<>(5.0001), 10);
        assertEquals(40L, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingQuadruplePace_2() {
        Throttler throttler = exponentialThrottler(40, new AtomicReference<>(5.001), 10);
        assertEquals(40L, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingOctagonalPace() {
        Throttler throttler = exponentialThrottler(80, new AtomicReference<>(5.0001), 10);
        assertEquals(80L, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingOctagonalPace_2() {
        Throttler throttler = exponentialThrottler(160, new AtomicReference<>(5.0001), 10);
        assertEquals(80L, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingTimeWithFactorZero() {
        Throttler throttler = extFactorThrottler(new AtomicReference<>(0), 100L);
        assertEquals(0L, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingTimeWithFactorOne() {
        Throttler throttler = extFactorThrottler(new AtomicReference<>(1), 100L);
        assertEquals(100L, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingTimeWithFactorFive() {
        Throttler throttler = extFactorThrottler(new AtomicReference<>(5), 200L);
        assertEquals(1000L, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingTimeWithNegativeFactor() {
        Throttler throttler = extFactorThrottler(new AtomicReference<>(-2), 100L);
        assertEquals(0, throttler.throttlingTime());
    }

    @Test
    public void testThrottlingTimeWithFactorChange() {
        AtomicReference<Integer> factor = new AtomicReference<>(2);
        Throttler throttler = extFactorThrottler(factor, 50L);
        assertEquals(100L, throttler.throttlingTime());
        factor.set(4);
        assertEquals(200L, throttler.throttlingTime());
    }

}
