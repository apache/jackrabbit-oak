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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class SimpleStats implements TimerStats, MeterStats, CounterStats, HistogramStats {
    public enum Type {COUNTER, METER, TIMER, HISTOGRAM}
    private final AtomicLong statsHolder;
    private final AtomicLong counter = new AtomicLong();
    private final Type type;

    public SimpleStats(AtomicLong statsHolder, Type type) {
        this.statsHolder = statsHolder;
        this.type = type;
    }

    @Override
    public long getCount() {
        switch(type){
            case HISTOGRAM:
            case TIMER:
                //For timer and histogram we need to manage explicit
                //invocation count
                return counter.get();
            default :
                //For meter and counter the statsHolder value is
                //same as count
                return statsHolder.get();
        }
    }

    @Override
    public void inc() {
        statsHolder.getAndIncrement();
    }

    @Override
    public void dec() {
        statsHolder.getAndDecrement();
    }

    @Override
    public void inc(long n) {
        statsHolder.getAndAdd(n);
    }

    @Override
    public void dec(long n) {
        statsHolder.getAndAdd(-n);
    }

    @Override
    public void mark() {
        inc();
    }

    @Override
    public void mark(long n) {
        statsHolder.getAndAdd(n);
    }

    @Override
    public void update(long duration, TimeUnit unit) {
        counter.incrementAndGet();
        statsHolder.getAndAdd(unit.toMillis(duration));
    }

    @Override
    public Context time() {
        return new SimpleContext(this);
    }

    @Override
    public void update(long value) {
        counter.incrementAndGet();
        statsHolder.getAndAdd(value);
    }

    private static final class SimpleContext implements Context {
        private final TimerStats timer;
        private final long startTime;

        private SimpleContext(TimerStats timer) {
            this.timer = timer;
            this.startTime = System.nanoTime();
        }

        @Override
        public long stop() {
            final long elapsed = System.nanoTime() - startTime;
            timer.update(elapsed, TimeUnit.NANOSECONDS);
            return elapsed;
        }

        @Override
        public void close() {
            stop();
        }
    }
}
