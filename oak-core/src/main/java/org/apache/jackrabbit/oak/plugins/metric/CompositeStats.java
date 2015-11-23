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

import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.SimpleStats;
import org.apache.jackrabbit.oak.stats.TimerStats;

/**
 * Stats instances which delegates to both TimeSeries based counter
 * and Metrics based meters so as to allow both systems to collect
 * stats
 */
class CompositeStats implements CounterStats, MeterStats, TimerStats {
    private final SimpleStats delegate;
    private final MetricCounterStats counter;
    private final MetricTimerStats timer;
    private final MetricMeterStats meter;

    public CompositeStats(SimpleStats delegate, MetricCounterStats counter) {
        this(delegate, counter, null, null);
    }

    public CompositeStats(SimpleStats delegate, MetricTimerStats timer) {
        this(delegate, null, timer, null);
    }

    public CompositeStats(SimpleStats delegate, MetricMeterStats meter) {
        this(delegate, null, null, meter);
    }

    private CompositeStats(SimpleStats delegate, MetricCounterStats counter,
                           MetricTimerStats timer, MetricMeterStats meter) {
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

    boolean isMeter() {
        return meter != null && timer == null && counter == null;
    }

    boolean isTimer() {
        return meter == null && timer != null && counter == null;
    }

    boolean isCounter() {
        return meter == null && timer == null && counter != null;
    }
}
