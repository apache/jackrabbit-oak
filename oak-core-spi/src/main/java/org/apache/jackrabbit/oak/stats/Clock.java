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

import java.io.Closeable;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mechanism for keeping track of time at millisecond accuracy.
 */
public abstract class Clock {

    /**
     * Maximum amount (in ms) of random noise to include in the time
     * signal reported by the {@link #SIMPLE} clock. Configurable by the
     * "simple.clock.noise" system property to make it easier to test
     * the effect of an inaccurate system clock.
     */
    private static final int SIMPLE_CLOCK_NOISE =
            Integer.getInteger("simple.clock.noise", 0);

    /**
     * Millisecond granularity of the {@link #ACCURATE} clock.
     * Configurable by the "accurate.clock.granularity" system property
     * to make it easier to test the effect of a slow-moving clock on
     * code that relies on millisecond timestamps.
     */
    private static final long ACCURATE_CLOCK_GRANULARITY =
            Long.getLong("accurate.clock.granularity", 1);

    /**
     * Millisecond update interval of the {@link Fast} clock. Configurable
     * by the "fast.clock.interval" system property to to make it easier
     * to test the effect of different update frequencies.
     */
    static final long FAST_CLOCK_INTERVAL =
            Long.getLong("fast.clock.interval", 10);

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
     * Waits until the given point in time is reached. The current thread
     * is suspended until the {@link #getTimeIncreasing()} method returns
     * a time that's equal or greater than the given point in time.
     *
     * @param timestamp time in milliseconds since epoch
     * @throws InterruptedException if the wait was interrupted
     */
    public void waitUntil(long timestamp) throws InterruptedException {
        long now = getTimeIncreasing();
        while (now < timestamp) {
            Thread.sleep(timestamp - now);
            now = getTimeIncreasing();
        }
    }

    /**
     * Simple clock implementation based on {@link System#currentTimeMillis()},
     * which is known to be rather slow on some platforms.
     */
    public static Clock SIMPLE = createSimpleClock();

    private static Clock createSimpleClock() {
        final int noise = SIMPLE_CLOCK_NOISE;
        if (noise > 0) {
            return new Clock() {
                private final Random random = new Random();
                @Override
                public synchronized long getTime() {
                    return System.currentTimeMillis() + random.nextInt(noise);
                }
                @Override
                public String toString() {
                    return "Clock.SIMPLE (with noise)";
                }
            };
        } else {
            return new Clock() {
                @Override
                public long getTime() {
                    return System.currentTimeMillis();
                }
                @Override
                public String toString() {
                    return "Clock.SIMPLE";
                }
            };
        }
    }

    /**
     * Accurate clock implementation that uses interval timings from the
     * {@link System#nanoTime()} method to calculate an as accurate as possible
     * time based on occasional calls to {@link System#currentTimeMillis()}
     * to prevent clock drift.
     */
    public static Clock ACCURATE = new Clock() {
        private static final long NS_IN_MS = 1000000;
        private long ms = SIMPLE.getTime();
        private long ns = System.nanoTime();
        @Override
        public synchronized long getTime() {
            long nowns = System.nanoTime();
            long nsIncrease = Math.max(nowns - ns, 0); // >= 0

            long msIncrease = (nsIncrease + NS_IN_MS/2) / NS_IN_MS; // round up
            if (ACCURATE_CLOCK_GRANULARITY > 1) {
                msIncrease -= msIncrease % ACCURATE_CLOCK_GRANULARITY;
            }

            // If last clock sync was less than one second ago, the nanosecond
            // timer drift will be insignificant and there's no need to re-sync.
            if (msIncrease < 1000) {
                return ms + msIncrease;
            }

            // Last clock sync was up to ten seconds ago, so we synchronize
            // smoothly to avoid both drift and sudden jumps.
            long nowms = SIMPLE.getTime();
            if (msIncrease < 10000) {
                // 1) increase the ms and ns timestamps as if the estimated
                //    ms increase was entirely correct
                ms += msIncrease;
                ns += msIncrease * NS_IN_MS;
                // 2) compare the resulting time with the wall clock to see
                //    if we're out of sync and to adjust accordingly
                long jump = nowms - ms;
                if (jump == 0) {
                    // 2a) No deviation from wall clock.
                    return ms;
                } else if (0 < jump && jump < 100) {
                    // 2b) The wall clock is up to 100ms ahead of us, probably
                    // because of its low granularity. Adjust the ns timestamp
                    // 0.1ms backward for future clock readings to jump that
                    // much ahead to eventually catch up with the wall clock.
                    ns -= NS_IN_MS / 10;
                    return ms;
                } else if (0 > jump && jump > -100) {
                    // 2c) The wall clock is up to 100ms behind us, probably
                    // because of its low granularity. Adjust the ns timestamp
                    // 0.1ms forward for future clock readings to stay constant
                    // (because of the Math.max(..., 0) above) for that long
                    // to eventually catch up with the wall clock.
                    ns += NS_IN_MS / 10;
                    return ms;
                }
            }

            // Last clock sync was over 10s ago or the nanosecond timer has
            // drifted more than 100ms from the wall clock, so it's best to
            // to a hard sync with no smoothing.
            if (nowms >= ms + 1000) {
                ms = nowms;
                ns = nowns;
            } else {
                // Prevent the clock from moving backwards by setting the
                // ms timestamp to exactly 1s ahead of the last sync time
                // (to account for the time between clock syncs), and
                // adjusting the ns timestamp ahead so that the reported time
                // will stall until the clock would again move ahead.
                ms = ms + 1000; // the 1s clock sync interval from above
                ns = nowns + (ms - nowms) * NS_IN_MS;
            }
            return ms;
        }

        @Override
        public String toString() {
            return "Clock.ACCURATE";
        }
    };

    /**
     * Fast clock implementation whose {@link #getTime()} method returns
     * instantaneously thanks to a background task that takes care of the
     * actual time-keeping work.
     */
    public static class Fast extends Clock implements Closeable {

        private volatile long time = ACCURATE.getTime();

        private final ScheduledFuture<?> future;

        public Fast(ScheduledExecutorService executor) {
            future = executor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    time = ACCURATE.getTime();
                }
            }, FAST_CLOCK_INTERVAL, FAST_CLOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }

        @Override
        public long getTime() {
            return time;
        }

        @Override
        public String toString() {
            return "Clock.Fast";
        }

        public void close() {
            future.cancel(false);
        }
    }

    /**
     * A virtual clock that has no connection to the actual system time.
     * Instead the clock maintains an internal counter that's incremented
     * atomically whenever the current time is requested. This guarantees
     * that the reported time signal is always strictly increasing.
     */
    public static class Virtual extends Clock {

        private final AtomicLong time = new AtomicLong();

        @Override
        public long getTime() {
            return time.getAndIncrement();
        }

        @Override
        public void waitUntil(long timestamp) {
            long now = time.get();
            while (now < timestamp && !time.compareAndSet(now, timestamp)) {
                now = time.get();
            }
        }

        @Override
        public String toString() {
            return "Clock.Virtual";
        }

    };

}
