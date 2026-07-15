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
package org.apache.jackrabbit.oak.commons.time;

import org.apache.jackrabbit.oak.commons.conditions.Validate;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A stop watch based either on a {@link Supplier} of nanoseconds, or a {@link Clock}.
 * <p>
 * The accuracy of measurements depends on the precision of the time source, which likely depends on platform and
 * configuration.
 * <p>
 * Inspired by Guava's.
 */
public final class Stopwatch {

    private long startTime;
    private long accumulated;
    private boolean running;
    private final Supplier<Long> ticker;

    private Stopwatch(Supplier<Long> ticker) {
        this.ticker = ticker;
        this.accumulated = 0L;
        this.startTime = ticker.get();
        this.running = false;
    }

    /**
     * @return a running stop watch, using {@link System#nanoTime()}.
     */
    public static Stopwatch createStarted() {
        return new Stopwatch(Stopwatch::tick).start();
    }

    /**
     * @return a running stop watch, using the supplied supplier.
     */
    public static Stopwatch createStarted(Supplier<Long> ticker) {
        return new Stopwatch(ticker).start();
    }

    /**
     * @return a running stop watch, using the supplied clock.
     * <p>
     * Note that only {@link Clock#millis()} will be used, thus the watch will have ms precision at most.
     */
    public static Stopwatch createStarted(Clock clock) {
        return new Stopwatch(clockAsLongSupplier(clock)).start();
    }

    /**
     * @return a non-running stop watch, using the supplied supplier.
     */
    public static Stopwatch createUnstarted(Supplier<Long> ticker) {
        return new Stopwatch(ticker);
    }

    /**
     * @return a non-running stop watch, using {@link System#nanoTime()}.
     */
    public static Stopwatch createUnstarted() {
        return new Stopwatch(Stopwatch::tick);
    }

    /**
     * Starts the stop watch, will fail when running.
     * @return the stop watch
     */
    public Stopwatch start() {
        Validate.checkState(!this.running, "Stopwatch already running.");
        this.startTime = this.ticker.get();
        this.running = true;
        return this;
    }

    /**
     * Stops the stop watch, will fail when not running.
     * @return the stop watch
     */
    public Stopwatch stop() {
        Validate.checkState(this.running, "Stopwatch not running.");
        this.accumulated += elapsedNanos();
        this.startTime = 0L;
        this.running = false;
        return this;
    }

    /**
     * Resets the stop watch, and puts it into stopped state.
     * @return the stop watch
     */
    public Stopwatch reset() {
        this.accumulated = 0L;
        this.startTime = 0;
        this.running = false;
        return this;
    }

    /**
     * @return whether the stop watch is running
     */
    public boolean isRunning() {
        return this.running;
    }

    /**
     * Gets elapsed time using the supplied {@link TimeUnit}.
     * @param timeunit time unit
     * @return elapsed time in the specified unit
     */
    public long elapsed(TimeUnit timeunit) {
        return timeunit.convert(elapsedNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Gets elapsed time as {@link Duration}.
     * @return elapsed time
     */
    public Duration elapsed() {
        return Duration.ofNanos(elapsedNanos());
    }

    @Override
    public String toString() {
        return Duration.ofNanos(elapsedNanos()).toString();
    }

    // private parts

    private long elapsedNanos() {
        long delta = this.running ? this.ticker.get() - this.startTime : 0;
        return this.accumulated + delta;
    }

    private static long tick() {
        return System.nanoTime();
    }

    private static Supplier<Long> clockAsLongSupplier(Clock clock) {
        return () -> TimeUnit.MILLISECONDS.toNanos(clock.millis());
    }
}
