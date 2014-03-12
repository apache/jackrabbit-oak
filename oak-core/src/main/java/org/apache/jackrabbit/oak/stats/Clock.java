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

import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Mechanism for keeping track of time at millisecond accuracy.
 */
public abstract class Clock {

    /**
     * Millisecond granularity of the {@link #ACCURATE} clock.
     * Configurable by the "accurate.clock.granularity" system property
     * to make it easier to test the effect of a slow-moving clock on
     * code that relies on millisecond timestamps.
     */
    private static final long ACCURATE_CLOCK_GRANULARITY =
            Long.getLong("accurate.clock.granularity", 1);

    private long monotonic = 0;

    private long increasing = 0;

    /**
     * Returns the current time in milliseconds since the epoch.
     *
     * @see System#currentTimeMillis()
     * @return current time in milliseconds since the epoch
     */
    public abstract long getTime();

    /**
     * Returns a monotonically increasing timestamp based on the current time.
     * A call to this method will always return a value that is greater than
     * or equal to a value returned by any previous call. This contract holds
     * even across multiple threads and in cases when the system time is
     * adjusted backwards. In the latter case the returned value will remain
     * constant until the previously reported timestamp is again reached.
     *
     * @return monotonically increasing timestamp
     */
    public synchronized long getTimeMonotonic() {
        long now = getTime();
        if (now > monotonic) {
            monotonic = now;
        } else {
            now = monotonic;
        }
        return now;
    }

    /**
     * Returns a strictly increasing timestamp based on the current time.
     * This method is like {@link #getTimeMonotonic()}, with the exception
     * that two calls of this method will never return the same timestamp.
     * Instead this method will explicitly wait until the current time
     * increases beyond any previously returned value. Note that the wait
     * may last long if this method is called frequently from many concurrent
     * thread or if the system time is adjusted backwards. The caller should
     * be prepared to deal with an explicit interrupt in such cases.
     *
     * @return strictly increasing timestamp
     * @throws InterruptedException if the wait was interrupted
     */
    public synchronized long getTimeIncreasing() throws InterruptedException {
        long now = getTime();
        while (now <= increasing) {
            wait(0, 100000); // 0.1ms
            now = getTime();
        }
        increasing = now;
        return now;
    }

    /**
     * Convenience method that returns the {@link #getTime()} value
     * as a {@link Date} instance.
     *
     * @return current time
     */
    public Date getDate() {
        return new Date(getTime());
    }

    /**
     * Convenience method that returns the {@link #getTimeMonotonic()} value
     * as a {@link Date} instance.
     *
     * @return monotonically increasing time
     */
    public Date getDateMonotonic() {
        return new Date(getTimeMonotonic());
    }

    /**
     * Convenience method that returns the {@link #getTimeIncreasing()} value
     * as a {@link Date} instance.
     *
     * @return strictly increasing time
     */
    public Date getDateIncreasing() throws InterruptedException {
        return new Date(getTimeIncreasing());
    }

    /**
     * Simple clock implementation based on {@link System#currentTimeMillis()},
     * which is known to be rather slow on some platforms.
     */
    public static Clock SIMPLE = new Clock() {
        @Override
        public long getTime() {
            return System.currentTimeMillis() & ~0xfL;
        }
    };

    /**
     * Accurate clock implementation that uses interval timings from the
     * {@link System#nanoTime()} method to calculate an as accurate as possible
     * time based on occasional calls to {@link System#currentTimeMillis()}
     * to prevent clock drift.
     */
    public static Clock ACCURATE = new Clock() {
        private static final long NS_IN_MS = 1000000;
        private static final long SYNC_INTERVAL = 1000; // ms
        private long ms = SIMPLE.getTime();
        private long ns = System.nanoTime();
        @Override
        public synchronized long getTime() {
            long nsIncrease = Math.max(System.nanoTime() - ns, 0); // >= 0
            long msIncrease = nsIncrease / NS_IN_MS;
            if (ACCURATE_CLOCK_GRANULARITY > 1) {
                msIncrease -= msIncrease % ACCURATE_CLOCK_GRANULARITY;
            }

            long now = ms + msIncrease;
            if (now > ms + SYNC_INTERVAL) {
                ms = SIMPLE.getTime();
                ns = System.nanoTime();
                long jump = ms - now;
                if (jump != 0 && Math.abs(jump) < SYNC_INTERVAL) {
                    // currentTimeMillis() jumped a little bit, which was
                    // probably caused by its lower granularity instead of
                    // an adjustment to system time, so we reduce the jump
                    // to just 0.5ms to make the reported time smoother
                    ms = now;
                    ns += Long.signum(jump) * NS_IN_MS / 2;
                } else {
                    now = ms;
                }
            }

            return now;
        }
    };

    /**
     * Fast clock implementation whose {@link #getTime()} method returns
     * instantaneously thanks to a background task that takes care of the
     * actual time-keeping work.
     */
    public static class Fast extends Clock {

        private volatile long time = ACCURATE.getTime();

        public Fast(ScheduledExecutorService executor) {
            executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    time = ACCURATE.getTime();
                }
            }, 1, 1, TimeUnit.MILLISECONDS);
        }

        @Override
        public long getTime() {
            return time;
        }

    }

}