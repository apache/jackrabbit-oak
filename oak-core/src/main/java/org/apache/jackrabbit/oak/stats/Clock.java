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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Mechanism for keeping track of time at millisecond accuracy.
 */
public interface Clock {

    /**
     * Returns the current time in milliseconds.
     *
     * @see System#currentTimeMillis()
     * @return current time in milliseconds
     */
    long getTime();

    /**
     * Simple clock implementation based on {@link System#currentTimeMillis()},
     * which is known to be rather slow on some platforms.
     */
    Clock SIMPLE = new Clock() {
        @Override
        public long getTime() {
            return System.currentTimeMillis();
        }
    };

    /**
     * Accurate clock implementation that uses interval timings from the
     * {@link System#nanoTime()} method to calculate an as accurate as possible
     * time based on occasional calls to {@link System#currentTimeMillis()}
     * to prevent clock drift.
     */
    Clock ACCURATE = new Clock() {
        private static final long NS_IN_MS = 1000000;
        private static final long NS_IN_S = NS_IN_MS * 1000;
        private volatile long ms = System.currentTimeMillis();
        private volatile long ns = System.nanoTime();
        @Override
        public long getTime() {
            long diff = System.nanoTime() - ns;
            if (diff < NS_IN_S) {
                return ms + diff / NS_IN_MS;
            } else {
                ms = System.currentTimeMillis();
                ns = System.nanoTime();
                return ms;
            }
        }
    };

    /**
     * Fast clock implementation whose {@link #getTime()} method returns
     * instantaneously thanks to a background task that takes care of the
     * actual time-keeping work.
     */
    public static class Fast implements Clock {

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