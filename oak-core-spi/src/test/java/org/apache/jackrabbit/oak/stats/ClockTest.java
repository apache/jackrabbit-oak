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
package org.apache.jackrabbit.oak.stats;

import static junit.framework.Assert.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.jackrabbit.oak.stats.Clock.Fast;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class ClockTest {
    private static long SYSTEM_CLOCK_GRANULARITY;
    private static Long FAST_CLOCK_GRANULARITY;

    /**
     * Helper for checking how accurate the system clock is.
     */
    public static void main(String[] args) {
        System.out.println(
                "average clock granularity: " + getAverageClockGranularity());
    }

    @BeforeClass
    public static void setup() {
        SYSTEM_CLOCK_GRANULARITY = getAverageClockGranularity();
        FAST_CLOCK_GRANULARITY = 1000 * Clock.FAST_CLOCK_INTERVAL;
    }

    @Test
    public void testClockDriftSimple() throws InterruptedException {
        testClockDrift(Clock.SIMPLE);
    }

    @Test
    public void testClockDriftAccurate() throws InterruptedException {
        testClockDrift(Clock.ACCURATE);
    }

    @Test
    @Ignore("OAK-3220")
    public void testClockDriftFast() throws InterruptedException {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            testClockDrift(new Clock.Fast(executor));
        } finally {
            executor.shutdown();
        }
    }

    private void testClockDrift(Clock clock) throws InterruptedException {

        long drift = clock.getTime() - System.currentTimeMillis();

        // Set the drift limit to twice as high as granularity,
        // plus 3ms for Thread.sleep() inaccuracy in the fast clock
        long granularity = getGranularity(clock);
        long limit = (2 * granularity) / 1000 + 3;
        assertTrue(clock + " unexpected drift: " + drift + "ms (estimated limit was " + limit + "ms, measured granularity was "
                + (granularity / 1000f) + "ms)", Math.abs(drift) <= limit);

        long waittime = 100;
        Thread.sleep(waittime);

        drift = clock.getTime() - System.currentTimeMillis();

        // Set the drift limit to twice as high as granularity,
        // plus 3ms for Thread.sleep() inaccuracy in the fast clock
        granularity = getGranularity(clock);
        limit = (2 * granularity) / 1000 + 3;
        assertTrue(clock + " unexpected drift after " + waittime + "ms: " + drift + "ms (estimated limit was " + limit
                + "ms, measured granularity was " + (granularity / 1000f) + "ms)", Math.abs(drift) <= limit);
    }

    private static long getGranularity(Clock clock) {
        return clock instanceof Fast
            ? FAST_CLOCK_GRANULARITY
            : SYSTEM_CLOCK_GRANULARITY;
    }

    @Test
    public void testClockIncreasingSimple() throws InterruptedException {
        testClockIncreasing(Clock.SIMPLE);
    }

    @Test
    public void testClockIncreasingAccurate() throws InterruptedException {
        testClockIncreasing(Clock.SIMPLE);
    }

    @Test
    public void testClockIncreasingFast() throws InterruptedException {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            testClockIncreasing(new Clock.Fast(executor));
        } finally {
            executor.shutdown();
        }
    }

    private void testClockIncreasing(Clock clock) throws InterruptedException {
        long time = 0;
        for (int i = 0; i < 10; i++) {
            long now = clock.getTimeIncreasing();
            assertTrue(time < now);
            time = now;
        }
    }

    /**
     * On some systems (for instance Windows), the granularity of {@code System.currentTimeMillis} depends
     * on system-wide settings that can change depending on what applications are running
     * (see, for instance <a href="http://www.lifehacker.com.au/2009/05/hidden-windows-7-tool-troubleshoots-sleep-mode-problems/">http://www.lifehacker.com.au/2009/05/hidden-windows-7-tool-troubleshoots-sleep-mode-problems/</a>).
     * This method tries to measure the granularity.
     * @return average granularity of {@code System.currentTimeMillis} in 1/1000 of milliseconds
     */
    private static long getAverageClockGranularity() {
        long sum = 0;
        int samples = 20; // number of samples to take
        long last = System.currentTimeMillis();

        for (int i = 0; i < samples; i++) {
            long now = System.currentTimeMillis();
            while (now == last) {
                // busy-wait until return value changes
                now = System.currentTimeMillis();
            }
            sum += (now - last); // add the actual difference
            last = now;
        }

        // return average in 1/1000ms
        return (sum * 1000) / samples;
    }
}