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
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.SimpleStats;
import org.apache.jackrabbit.oak.stats.TimerStats;

/**
 * Stats instances which delegates to both TimeSeries based counter
 * and Metrics based meters so as to allow both systems to collect
 * stats
 */
final class CompositeStats implements CounterStats, MeterStats, TimerStats {
    private final SimpleStats delegate;
    private final Counter counter;
    private final Timer timer;
    private final Meter meter;

    public CompositeStats(SimpleStats delegate, Counter counter) {
        this(delegate, counter, null, null);
    }

    public CompositeStats(SimpleStats delegate, Timer timer) {
        this(delegate, null, timer, null);
    }

    public CompositeStats(SimpleStats delegate, Meter meter) {
        this(delegate, null, null, meter);
    }

    private CompositeStats(SimpleStats delegate, Counter counter,
                           Timer timer, Meter meter) {
        this.delegate = delegate;
        this.counter = counter;
        this.timer = timer;
        this.meter = meter;
    }

    @Override
    public long getCount() {
        return delegate.getCount();
    }

    @Override
    public void inc() {
        delegate.inc();
        counter.inc();
    }

    @Override
    public void dec() {
        delegate.dec();
        counter.dec();
    }

    @Override
    public void mark() {
        delegate.mark();
        meter.mark();
    }

    @Override
    public void mark(long n) {
        delegate.mark(n);
        meter.mark(n);
    }

    @Override
    public void update(long duration, TimeUnit unit) {
        delegate.update(duration, unit);
        timer.update(duration, unit);
    }

    @Override
    public Context time() {
        return new StatsContext(timer.time(), delegate);
    }

    boolean isMeter() {
        return meter != null && timer == null && counter == null;
    }

    boolean isTimer() {
        return meter == null && timer != null && counter == null;
    }

    boolean isCounter() {
        return meter == null && timer == null && counter != null;
    }

    Counter getCounter() {
        return counter;
    }

    Timer getTimer() {
        return timer;
    }

    Meter getMeter() {
        return meter;
    }

    private static final class StatsContext implements Context {
        private final Timer.Context context ;
        private final SimpleStats simpleStats;

        private StatsContext(Timer.Context context, SimpleStats delegate) {
            this.context = context;
            this.simpleStats = delegate;
        }

        public long stop() {
            long nanos = context.stop();
            simpleStats.update(nanos, TimeUnit.NANOSECONDS);
            return nanos;
        }

        /** Equivalent to calling {@link #stop()}. */
        @Override
        public void close() {
            stop();
        }
    }
}
