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
package org.apache.jackrabbit.oak.commons;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;

/**
 * Format a time duration as human-readable string, inspired by
 * {@link Stopwatch#toString()}.
 */
public class TimeDurationFormatter {

    private final int threshold;
    private final boolean allowNonAscii;
    private final Locale locale;

    private static TimeUnit[] UNITS = { TimeUnit.DAYS, TimeUnit.HOURS, TimeUnit.MINUTES, TimeUnit.SECONDS, TimeUnit.MILLISECONDS,
            TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS };
    private static Map<TimeUnit, String> DISPLAYUNIT;
    private static Map<TimeUnit, String> DISPLAYUNIT_ASCII;

    static {
        HashMap<TimeUnit, String> t = Maps.newHashMap();
        t.put(TimeUnit.DAYS, "d");
        t.put(TimeUnit.HOURS, "h");
        t.put(TimeUnit.MINUTES, "min");
        t.put(TimeUnit.SECONDS, "s");
        t.put(TimeUnit.MILLISECONDS, "ms");
        t.put(TimeUnit.MICROSECONDS, "us");
        t.put(TimeUnit.NANOSECONDS, "ns");
        DISPLAYUNIT_ASCII = Collections.unmodifiableMap(t);
        t = Maps.newHashMap(t);
        // Unicode "MICRO SIGN"
        t.put(TimeUnit.MICROSECONDS, "\u00b5s");
        DISPLAYUNIT = Collections.unmodifiableMap(t);
    }

    private static TimeDurationFormatter FOR_LOGGING = new TimeDurationFormatter(Locale.US, 2, false);

    /**
     * Default formatter suitable for logging (ASCII-only)
     */
    public static TimeDurationFormatter forLogging() {
        return FOR_LOGGING;
    }

    /**
     * @param locale
     *            locale for formatting (affects the decimal point)
     * @param threshold
     *            integral value that needs to be exceeded to switch to a
     *            certain time unit for display (e.g., specify {@code 2} to
     *            switch to 'days' when the elapsed time is two or more days.
     * @param allowNonAscii
     *            set to {@code true} if the display unit can use the non-ASCII
     *            "micro" Unicode character
     */
    public TimeDurationFormatter(Locale locale, int threshold, boolean allowNonAscii) {
        this.locale = locale;
        this.threshold = threshold;
        this.allowNonAscii = allowNonAscii;
    }

    /**
     * Format the specified duration
     * @param amount number of time units
     * @param unit time unit
     */
    public String format(long amount, TimeUnit unit) {
        long nanos = unit.toNanos(amount);

        TimeUnit outputUnit = TimeUnit.NANOSECONDS;
        for (TimeUnit u : UNITS) {
            if (u.convert(nanos, TimeUnit.NANOSECONDS) >= this.threshold) {
                outputUnit = u;
                break;
            }
        }

        double convertedToUnit = ((double) nanos) / (TimeUnit.NANOSECONDS.convert(1, outputUnit));

        return String.format(this.locale, "%.4g %s", convertedToUnit,
                this.allowNonAscii ? DISPLAYUNIT.get(outputUnit) : DISPLAYUNIT_ASCII.get(outputUnit));
    }
}
