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

import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MetricImplTest {
    private MetricRegistry registry = new MetricRegistry();


    @Test
    public void counter() throws Exception {
        Counter counter = registry.counter("test");
        CounterImpl counterStats = new CounterImpl(counter);

        counterStats.inc();
        assertEquals(1, counter.getCount());
        assertEquals(1, counterStats.getCount());

        counterStats.inc();
        counterStats.inc();

        counterStats.dec();
        assertEquals(2, counter.getCount());

        counterStats.inc(7);
        assertEquals(9, counter.getCount());

        counterStats.dec(5);
        assertEquals(4, counter.getCount());
        assertEquals(4, counterStats.getCount());
    }

    @Test
    public void meter() throws Exception {
        Meter meter = registry.meter("test");
        MeterImpl meterStats = new MeterImpl(meter);

        meterStats.mark();
        assertEquals(1, meter.getCount());

        meterStats.mark(5);
        assertEquals(6, meter.getCount());
        assertEquals(6, meterStats.getCount());
    }

    @Test
    public void timer() throws Exception {
        Timer time = registry.timer("test");
        TimerImpl timerStats = new TimerImpl(time);

        timerStats.update(100, TimeUnit.SECONDS);
        assertEquals(1, time.getCount());
        assertEquals(TimeUnit.SECONDS.toNanos(100), time.getSnapshot().getMax());

        timerStats.update(100, TimeUnit.SECONDS);
        assertEquals(2, time.getCount());
        assertEquals(2, timerStats.getCount());
    }

    @Test
    public void histogram() throws Exception {
        Histogram histo = registry.histogram("test");
        HistogramImpl histoStats = new HistogramImpl(histo);

        histoStats.update(100);
        assertEquals(1, histo.getCount());
        assertEquals(100, histo.getSnapshot().getMax());
    }

    @Test
    public void timerContext() throws Exception{
        VirtualClock clock = new VirtualClock();
        Timer time = new Timer(new ExponentiallyDecayingReservoir(), clock);

        TimerStats timerStats = new TimerImpl(time);
        TimerStats.Context context = timerStats.time();

        clock.tick = TimeUnit.SECONDS.toNanos(314);
        context.close();

        assertEquals(1, time.getCount());
        assertEquals(TimeUnit.SECONDS.toNanos(314), time.getSnapshot().getMax());
    }

    private static class VirtualClock extends com.codahale.metrics.Clock {
        long tick;
        @Override
        public long getTick() {
            return tick;
        }
    }
}
