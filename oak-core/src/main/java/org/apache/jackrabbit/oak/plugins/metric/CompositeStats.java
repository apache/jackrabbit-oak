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
import com.codahale.metrics.Counting;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.TimerStats;

/**
 * Stats instances which delegates to both TimeSeries based counter
 * and Metrics based meters so as to allow both systems to collect
 * stats
 */
final class CompositeStats implements CounterStats, MeterStats, TimerStats, HistogramStats {
    private final AtomicLong delegate;
    private final Counter counter;
    private final Timer timer;
    private final Meter meter;
    private final Histogram histogram;
    private final Counting counting;

    public CompositeStats(AtomicLong delegate, Counter counter) {
        this(delegate, counter, null, null, null, counter);
    }

    public CompositeStats(AtomicLong delegate, Timer timer) {
        this(delegate, null, timer, null, null, timer);
    }

    public CompositeStats(AtomicLong delegate, Meter meter) {
        this(delegate, null, null, meter, null, meter);
    }

    public CompositeStats(AtomicLong delegate, Histogram histogram) {
        this(delegate, null, null, null, histogram, histogram);
    }

    private CompositeStats(AtomicLong delegate, Counter counter,
                           Timer timer, Meter meter, Histogram histogram, Counting counting) {
        this.delegate = delegate;
        this.counter = counter;
        this.timer = timer;
        this.meter = meter;
        this.histogram = histogram;
        this.counting = counting;
    }

    @Override
    public long getCount() {
        return counting.getCount();
    }

    @Override
    public void inc() {
        delegate.getAndIncrement();
        counter.inc();
    }

    @Override
    public void dec() {
        delegate.getAndDecrement();
        counter.dec();
    }

    @Override
    public void inc(long n) {
        delegate.getAndAdd(n);
        counter.inc(n);
    }

    @Override
    public void dec(long n) {
        delegate.getAndAdd(-n);
        counter.dec(n);
    }

    @Override
    public void mark() {
        delegate.getAndIncrement();
        meter.mark();
    }

    @Override
    public void mark(long n) {
        delegate.getAndAdd(n);
        meter.mark(n);
    }

    @Override
    public void update(long duration, TimeUnit unit) {
        delegate.getAndAdd(unit.toMillis(duration));
        timer.update(duration, unit);
    }

    @Override
    public void update(long value) {
        delegate.getAndAdd(value);
        histogram.update(value);
    }

    @Override
    public Context time() {
        return new StatsContext(timer.time(), delegate);
    }

    boolean isMeter() {
        return meter != null;
    }

    boolean isTimer() {
        return timer != null;
    }

    boolean isCounter() {
        return counter != null;
    }

    boolean isHistogram(){
        return histogram != null;
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

    Histogram getHistogram(){
        return histogram;
    }


    private static final class StatsContext implements Context {
        private final Timer.Context context ;
        private final AtomicLong delegate;

        private StatsContext(Timer.Context context, AtomicLong delegate) {
            this.context = context;
            this.delegate = delegate;
        }

        public long stop() {
            long nanos = context.stop();
            delegate.getAndAdd(TimeUnit.NANOSECONDS.toMillis(nanos));
            return nanos;
        }

        /** Equivalent to calling {@link #stop()}. */
        @Override
        public void close() {
            stop();
        }
    }
}
