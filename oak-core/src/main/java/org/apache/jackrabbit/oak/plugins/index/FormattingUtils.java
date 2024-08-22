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

import java.util.concurrent.TimeUnit;

public class FormattingUtils {
    public static String formatNanosToSeconds(long nanos) {
        return formatToSeconds(nanos / 1_000_000_000);
    }

    public static String formatMillisToSeconds(long millis) {
        return formatToSeconds(millis / 1000);
    }

    public static String formatToSeconds(Stopwatch stopwatch) {
        return formatToSeconds(stopwatch.elapsed(TimeUnit.SECONDS));
    }

    public static String formatToSeconds(long seconds) {
        long absSeconds = Math.abs(seconds);
        long hoursPart = TimeUnit.SECONDS.toHours(absSeconds);
        long minutesPart = TimeUnit.SECONDS.toMinutes(absSeconds) % 60;
        long secondsPart = TimeUnit.SECONDS.toSeconds(absSeconds) % 60;
        String sign = seconds < 0 ? "-" : "";
        return String.format("%s%02d:%02d:%02d", sign, hoursPart, minutesPart, secondsPart);
    }

    public static String formatToMillis(Stopwatch stopwatch) {
        long millis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        long absMillis = Math.abs(millis);
        long hoursPart = TimeUnit.MILLISECONDS.toHours(absMillis);
        long minutesPart = TimeUnit.MILLISECONDS.toMinutes(absMillis) % 60;
        long secondsPart = TimeUnit.MILLISECONDS.toSeconds(absMillis) % 60;
        long millisPart = TimeUnit.MILLISECONDS.toMillis(absMillis) % 1000;
        String sign = millis < 0 ? "-" : "";
        return String.format("%s%02d:%02d:%02d.%03d", sign, hoursPart, minutesPart, secondsPart, millisPart);
    }

    public static double safeComputePercentage(long numerator, long denominator) {
        return denominator == 0 ? -1 : (double) numerator / denominator * 100;
    }

    public static double safeComputeAverage(long totalTime, long numberOfEvents) {
        return numberOfEvents == 0 ? -1 : ((double) totalTime / numberOfEvents);
    }
}
