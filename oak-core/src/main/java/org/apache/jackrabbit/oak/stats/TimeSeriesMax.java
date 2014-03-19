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

package org.apache.jackrabbit.oak.stats;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.api.stats.TimeSeries;

/**
 * Time series of the maximum value recorded in a period
 */
public class TimeSeriesMax implements TimeSeries {
    private final MaxValue max = new MaxValue(0);
    private final long[] perSecond = new long[60];
    private final long[] perMinute = new long[60];
    private final long[] perHour = new long[7 * 24];
    private final long[] perWeek = new long[3 * 52];

    /** Current second (index in {@link #perSecond}) */
    private int seconds;

    /** Current minute (index in {@link #perMinute}) */
    private int minutes;

    /** Current hour (index in {@link #perHour}) */
    private int hours;

    /** Current week (index in {@link #perWeek}) */
    private int weeks;

    public TimeSeriesMax(ScheduledExecutorService executor) {
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                perSecond[seconds++] = max.getAndSetValue(0);
                if (seconds == perSecond.length) {
                    seconds = 0;
                    perMinute[minutes++] = max(perSecond);
                }
                if (minutes == perMinute.length) {
                    minutes = 0;
                    perHour[hours++] = max(perMinute);
                }
                if (hours == perHour.length) {
                    hours = 0;
                    perWeek[weeks++] = max(perHour);
                }
                if (weeks == perWeek.length) {
                    weeks = 0;
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    public void recordValue(long value) {
        max.setIfMaximal(value);
    }

    @Override
    public synchronized long[] getValuePerSecond() {
        return cyclicCopyFrom(perSecond, seconds);
    }

    @Override
    public synchronized long[] getValuePerMinute() {
        return cyclicCopyFrom(perMinute, minutes);
    }

    @Override
    public synchronized long[] getValuePerHour() {
        return cyclicCopyFrom(perHour, hours);
    }

    @Override
    public synchronized long[] getValuePerWeek() {
        return cyclicCopyFrom(perWeek, weeks);
    }

    /**
     * Returns the maximum of all entries in the given array.
     */
    private static long max(long[] array) {
        long max = Long.MIN_VALUE;
        for (long v : array) {
            if (v > max) {
                max = v;
            }
        }
        return max;
    }

    /**
     * Returns a copy of the given cyclical array, with the element at
     * the given position as the first element of the returned array.
     *
     * @param array cyclical array
     * @param pos position of the first element
     * @return copy of the array
     */
    private static long[] cyclicCopyFrom(long[] array, int pos) {
        long[] reverse = new long[array.length];
        for (int i = 0; i < array.length; i++) {
            reverse[i] = array[(pos + i) % array.length];
        }
        return reverse;
    }

    private static class MaxValue {
        private long value;

        public MaxValue(long value) {
            this.value = value;
        }

        public synchronized long getAndSetValue(long value) {
            long v = this.value;
            this.value = value;
            return v;
        }

        public synchronized void setIfMaximal(long value) {
            if (value > this.value) {
                this.value = value;
            }
        }
    }
}
