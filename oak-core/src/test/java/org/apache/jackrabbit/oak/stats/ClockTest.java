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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Ignore;
import org.junit.Test;

public class ClockTest {

    /**
     * Helper for checking how accurate the system clock is.
     */
    public static void main(String[] args) {
        List<Long> values = new ArrayList<Long>();
        long end = System.currentTimeMillis() + 10000; // 10 seconds
        long last = System.currentTimeMillis();
        long current = last;

        do {
            current = System.currentTimeMillis();
            if (current != last && current != last + 1) {
                values.add(current);
            }
            last = current;
        } while (current < end);
        for (int i = 1; i < values.size(); i++) {
            System.out.println(values.get(i) + " " + (values.get(i) - values.get(i - 1)));
        }
    }

    @Test
    @Ignore("OAK-1564")  // FIXME OAK-1564 ClockTest on Windows fails
    public void testClockDrift() throws InterruptedException {
        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor();
        final long limit = 20;
        try {
            Clock[] clocks = new Clock[] {
                    Clock.SIMPLE,
                    Clock.ACCURATE,
                    new Clock.Fast(executor)
            };

            for (Clock clock : clocks) {
                long drift = clock.getTime() - System.currentTimeMillis();
                assertTrue("unexpected drift: " + Math.abs(drift) + " (limit " + limit +")", Math.abs(drift) < limit); // Windows can have 15ms gaps
            }

            Thread.sleep(100);

            for (Clock clock : clocks) {
                long drift = clock.getTime() - System.currentTimeMillis();
                assertTrue("unexpected drift: " + Math.abs(drift) + " (limit " + limit +")", Math.abs(drift) < limit);
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

}