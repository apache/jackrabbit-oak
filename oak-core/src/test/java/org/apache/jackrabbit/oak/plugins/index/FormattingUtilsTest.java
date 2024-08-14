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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.base.Ticker;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


public class FormattingUtilsTest {

    private static class TestTicker extends Ticker {
        private long time = 0;
        @Override
        public long read() {
            return time;
        }
        public void set(long nanos) {
            time = nanos;
        }
    }
    private final TestTicker ticker = new TestTicker();
    private final Stopwatch sw = Stopwatch.createStarted(ticker);

    @Test
    public void formatToSeconds() {
        testFormatToSeconds("00:00:00", 0);
        testFormatToSeconds("00:00:59", TimeUnit.MILLISECONDS.toNanos(59_567));
        testFormatToSeconds("00:01:00", TimeUnit.MILLISECONDS.toNanos(60_567));
        testFormatToSeconds("00:59:00", TimeUnit.MINUTES.toNanos(59));
        testFormatToSeconds("01:00:00", TimeUnit.MINUTES.toNanos(60));
        testFormatToSeconds("23:00:00", TimeUnit.HOURS.toNanos(23));
        testFormatToSeconds("24:00:00", TimeUnit.HOURS.toNanos(24));
        testFormatToSeconds("48:00:00", TimeUnit.HOURS.toNanos(48));
        testFormatToSeconds("23:59:59", TimeUnit.HOURS.toNanos(23) +
                TimeUnit.MINUTES.toNanos(59) +
                TimeUnit.SECONDS.toNanos(59) +
                TimeUnit.MILLISECONDS.toNanos(999)
        );
        testFormatToSeconds("-00:01:00", -TimeUnit.SECONDS.toNanos(60));
    }

    private void testFormatToSeconds(String expected, long nanos) {
        ticker.set(nanos);
        assertEquals(expected, FormattingUtils.formatToSeconds(sw));
    }

    @Test
    public void formatToMillis() {
        testFormatToMillis("00:00:00.000", 0);
        testFormatToMillis("00:00:59.567", TimeUnit.MILLISECONDS.toNanos(59_567));
        testFormatToMillis("00:01:00.567", TimeUnit.MILLISECONDS.toNanos(60_567));
        testFormatToMillis("00:59:00.000", TimeUnit.MINUTES.toNanos(59));
        testFormatToMillis("01:00:00.000", TimeUnit.MINUTES.toNanos(60));
        testFormatToMillis("23:00:00.000", TimeUnit.HOURS.toNanos(23));
        testFormatToMillis("24:00:00.000", TimeUnit.HOURS.toNanos(24));
        testFormatToMillis("48:00:00.000", TimeUnit.HOURS.toNanos(48));
        testFormatToMillis("23:59:59.999", TimeUnit.HOURS.toNanos(23) +
                TimeUnit.MINUTES.toNanos(59) +
                TimeUnit.SECONDS.toNanos(59) +
                TimeUnit.MILLISECONDS.toNanos(999)
        );
        testFormatToMillis("-00:01:00.000", -TimeUnit.SECONDS.toNanos(60));
    }

    private void testFormatToMillis(String expected, long nanos) {
        ticker.set(nanos);
        assertEquals(expected, FormattingUtils.formatToMillis(sw));
    }

    @Test
    public void testSafeComputePercentage() {
        assertEquals(50.0, FormattingUtils.safeComputePercentage(50, 100), 0.001);
        assertEquals(0.0, FormattingUtils.safeComputePercentage(0, 100), 0.001);
        assertEquals(-1.0, FormattingUtils.safeComputePercentage(50, 0), 0.001);
        assertEquals(100.0, FormattingUtils.safeComputePercentage(100, 100), 0.001);
        assertEquals(33.333, FormattingUtils.safeComputePercentage(1, 3), 0.001);
    }

    @Test
    public void testSafeComputeAverage() {
        assertEquals(50.0, FormattingUtils.safeComputeAverage(100, 2), 0.001);
        assertEquals(0.0, FormattingUtils.safeComputeAverage(0, 100), 0.001);
        assertEquals(-1.0, FormattingUtils.safeComputeAverage(100, 0), 0.001);
        assertEquals(100.0, FormattingUtils.safeComputeAverage(100, 1), 0.001);
        assertEquals(33.333, FormattingUtils.safeComputeAverage(100, 3), 0.001);
    }
}