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
import org.apache.jackrabbit.oak.stats.SimpleStats;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CompositeStatsTest {
    private MetricRegistry registry = new MetricRegistry();
    private SimpleStats simpleStats = new SimpleStats(new AtomicLong());

    @Test
    public void counter() throws Exception {
        Counter counter = registry.counter("test");
        CompositeStats counterStats = new CompositeStats(simpleStats, counter);

        counterStats.inc();
        assertEquals(1, simpleStats.getCount());
        assertEquals(1, counter.getCount());
        assertEquals(1, counterStats.getCount());

        counterStats.inc();
        counterStats.inc();
        assertEquals(3, simpleStats.getCount());

        counterStats.dec();
        assertEquals(2, simpleStats.getCount());
        assertEquals(2, counter.getCount());

        counterStats.inc(7);
        assertEquals(9, simpleStats.getCount());
        assertEquals(9, counter.getCount());

        counterStats.dec(5);
        assertEquals(4, simpleStats.getCount());
        assertEquals(4, counter.getCount());

        assertFalse(counterStats.isMeter());
        assertFalse(counterStats.isTimer());
        assertTrue(counterStats.isCounter());
        assertNotNull(counterStats.getCounter());
    }

    @Test
    public void meter() throws Exception {
        Meter meter = registry.meter("test");
        CompositeStats meterStats = new CompositeStats(simpleStats, meter);

        meterStats.mark();
        assertEquals(1, simpleStats.getCount());
        assertEquals(1, meter.getCount());

        meterStats.mark(5);
        assertEquals(6, simpleStats.getCount());
        assertEquals(6, meter.getCount());
        assertTrue(meterStats.isMeter());
        assertFalse(meterStats.isTimer());
        assertFalse(meterStats.isCounter());
        assertNotNull(meterStats.getMeter());
    }

    @Test
    public void timer() throws Exception {
        Timer time = registry.timer("test");
        CompositeStats timerStats = new CompositeStats(simpleStats, time);

        timerStats.update(100, TimeUnit.SECONDS);
        assertEquals(1, time.getCount());
        assertEquals(TimeUnit.SECONDS.toMillis(100), simpleStats.getCount());

        assertFalse(timerStats.isMeter());
        assertTrue(timerStats.isTimer());
        assertFalse(timerStats.isCounter());
        assertNotNull(timerStats.getTimer());
    }

    @Test
    public void histogram() throws Exception {
        Histogram histo = registry.histogram("test");
        CompositeStats histoStats = new CompositeStats(simpleStats, histo);

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
        VirtualClock clock = new VirtualClock();
        Timer time = new Timer(new ExponentiallyDecayingReservoir(), clock);

        TimerStats timerStats = new CompositeStats(simpleStats, time);
        TimerStats.Context context = timerStats.time();

        clock.tick = TimeUnit.SECONDS.toNanos(314);
        context.close();

        assertEquals(1, time.getCount());
        assertEquals(TimeUnit.SECONDS.toMillis(314), simpleStats.getCount());
    }

    private static class VirtualClock extends com.codahale.metrics.Clock {
        long tick;
        @Override
        public long getTick() {
            return tick;
        }
    }
}
