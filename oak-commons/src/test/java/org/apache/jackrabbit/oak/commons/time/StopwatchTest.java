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
package org.apache.jackrabbit.oak.commons.time;

import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StopwatchTest {

    private static final long CHECK_INTERVAL_SECONDS = 3;
    private static final long CHECK_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(CHECK_INTERVAL_SECONDS);

    @Test
    public void started() throws InterruptedException {
        testStarted(Stopwatch.createStarted());
    }

    @Test
    public void stopped() throws InterruptedException {
        Stopwatch sw = Stopwatch.createUnstarted();
        assertFalse(sw.isRunning());
        testStarted(sw.start());
    }

    @Test
    public void withClock() throws InterruptedException {
        testStarted(Stopwatch.createStarted(Clock.systemUTC()));
    }

    @Test
    public void specialTicker() {
        final AtomicLong time = new AtomicLong(0);
        Stopwatch sw = Stopwatch.createUnstarted(time::get);
        assertEquals(0, sw.elapsed().toNanos());
        sw.start();
        time.set(123);
        assertEquals(123, sw.elapsed().toNanos());
        time.set(456);
        sw.stop();
        assertEquals(456, sw.elapsed().toNanos());

        time.set(0);
        sw.reset().start();

        // tests internals
        assertEquals(Duration.ofNanos(0).toString(), sw.toString());
    }

    private void testStarted(Stopwatch sw) throws InterruptedException {
        letElapse(CHECK_INTERVAL_MILLIS);
        sw.stop();
        long millis = sw.elapsed().toMillis();
        assertTrue("elapsed " + millis + "ms, expected ~" + CHECK_INTERVAL_MILLIS,
                millis >= CHECK_INTERVAL_MILLIS - 1000 && millis <= CHECK_INTERVAL_MILLIS + 1000);
        long seconds = sw.elapsed(TimeUnit.SECONDS);
        assertTrue("elapsed " + seconds + "s, expected ~" + CHECK_INTERVAL_SECONDS,
                seconds >= CHECK_INTERVAL_SECONDS - 1 && seconds <= CHECK_INTERVAL_SECONDS + 1);
    }

    private static void letElapse(long delta) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() < start + delta) {
            Thread.sleep(1);
        }
    }
}
