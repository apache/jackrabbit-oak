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

package org.apache.jackrabbit.oak.plugins.metric;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompositeStatsTest {
    private MetricRegistry registry = new MetricRegistry();


    @Test
    public void counter() throws Exception {
        AtomicLong simpleStats = new AtomicLong();
        Counter counter = registry.counter("test");
        CompositeStats counterStats = new CompositeStats(simpleStats, counter);

        counterStats.inc();
        assertEquals(1, simpleStats.get());
        assertEquals(1, counter.getCount());
        assertEquals(1, counterStats.getCount());

        counterStats.inc();
        counterStats.inc();
        assertEquals(3, simpleStats.get());

        counterStats.dec();
        assertEquals(2, simpleStats.get());
        assertEquals(2, counter.getCount());

        counterStats.inc(7);
        assertEquals(9, simpleStats.get());
        assertEquals(9, counter.getCount());

        counterStats.dec(5);
        assertEquals(4, simpleStats.get());
        assertEquals(4, counter.getCount());

        assertFalse(counterStats.isMeter());
        assertFalse(counterStats.isTimer());
        assertTrue(counterStats.isCounter());
        assertNotNull(counterStats.getCounter());
    }

    @Test
    public void meter() throws Exception {
        AtomicLong simpleStats = new AtomicLong();
        Meter meter = registry.meter("test");
        CompositeStats meterStats = new CompositeStats(simpleStats, meter);

        meterStats.mark();
        assertEquals(1, simpleStats.get());
        assertEquals(1, meter.getCount());

        meterStats.mark(5);
        assertEquals(6, simpleStats.get());
        assertEquals(6, meter.getCount());
        assertTrue(meterStats.isMeter());
        assertFalse(meterStats.isTimer());
        assertFalse(meterStats.isCounter());
        assertNotNull(meterStats.getMeter());
    }

    @Test
    public void timer() throws Exception {
        AtomicLong counter = new AtomicLong();
        Timer time = registry.timer("test");
        CompositeStats timerStats = new CompositeStats(counter, time);

        timerStats.update(100, TimeUnit.SECONDS);
        assertEquals(1, time.getCount());
        assertEquals(TimeUnit.SECONDS.toMillis(100), counter.get());

        timerStats.update(100, TimeUnit.SECONDS);
        assertEquals(2, timerStats.getCount());

        assertFalse(timerStats.isMeter());
        assertTrue(timerStats.isTimer());
        assertFalse(timerStats.isCounter());
        assertNotNull(timerStats.getTimer());
    }

    @Test
    public void histogram() throws Exception {
        Histogram histo = registry.histogram("test");
        CompositeStats histoStats = new CompositeStats(new AtomicLong(), histo);

        histoStats.update(100);
        assertEquals(1, histo.getCount());
        assertEquals(100, histo.getSnapshot().getMax());

        assertFalse(histoStats.isMeter());
        assertFalse(histoStats.isTimer());
        assertFalse(histoStats.isCounter());
        assertTrue(histoStats.isHistogram());
        assertNotNull(histoStats.getHistogram());
    }

    @Test
    public void timerContext() throws Exception{
        AtomicLong counter = new AtomicLong();
        VirtualClock clock = new VirtualClock();
        Timer time = new Timer(new ExponentiallyDecayingReservoir(), clock);

        TimerStats timerStats = new CompositeStats(counter, time);
        TimerStats.Context context = timerStats.time();

        clock.tick = TimeUnit.SECONDS.toNanos(314);
        context.close();

        assertEquals(1, time.getCount());
        assertEquals(TimeUnit.SECONDS.toMillis(314), counter.get());
    }

    private static class VirtualClock extends com.codahale.metrics.Clock {
        long tick;
        @Override
        public long getTick() {
            return tick;
        }
    }
}
