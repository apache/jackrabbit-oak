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

import org.junit.Test;

public class ClockTest {

    /**
     * Helper for checking how accurate the system clock is.
     */
    public static void main(String[] args) {
        System.out.println("average clock granularity: " + getAverageClockGranularity());
    }

    @Test
    public void testClockDrift() throws InterruptedException {
        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor();
        final long granularity = getAverageClockGranularity();
        final long limit = (2 * granularity) / 1000; // allow a drift twice as high
        final String diag = "(estimated limit was " + limit + "ms, measured granularity was " + ( granularity / 1000f) + "ms)";

        try {
            Clock[] clocks = new Clock[] {
                    Clock.SIMPLE,
                    Clock.ACCURATE,
                    new Clock.Fast(executor)
            };

            for (Clock clock : clocks) {
                long drift = clock.getTime() - System.currentTimeMillis();
                assertTrue("unexpected drift: " + Math.abs(drift) + "ms " + diag, Math.abs(drift) <= limit);
            }

            Thread.sleep(100);

            for (Clock clock : clocks) {
                long drift = clock.getTime() - System.currentTimeMillis();
                assertTrue("unexpected drift: " + Math.abs(drift) + "ms " + diag, Math.abs(drift) <= limit);
            }
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testClockIncreasing() throws InterruptedException {
        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor();
        try {
            Clock[] clocks = new Clock[] {
                    Clock.SIMPLE,
                    Clock.ACCURATE,
                    new Clock.Fast(executor)
            };

            long[] time = new long[clocks.length];
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < clocks.length; j++) {
                    long now = clocks[j].getTimeIncreasing();
                    assertTrue(time[j] < now);
                    time[j] = now;
                }
            }
        } finally {
            executor.shutdown();
        }
    }

    /**
     * On some systems (for instance Windows), the granularity of {@link System.currentTimeMillis} depends
     * on system-wide settings that can change depending on what applications are running
     * (see, for instance <a href="http://www.lifehacker.com.au/2009/05/hidden-windows-7-tool-troubleshoots-sleep-mode-problems/">http://www.lifehacker.com.au/2009/05/hidden-windows-7-tool-troubleshoots-sleep-mode-problems/</a>).
     * This method tries to measure the granularity.
     * @return average granularity of {@link System.currentTimeMillis} in 1/1000 of milliseconds
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