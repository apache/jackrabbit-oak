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

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class FormattingUtils {
    public static String formatToSeconds(Stopwatch stopwatch) {
        LocalTime seconds = LocalTime.ofSecondOfDay(stopwatch.elapsed(TimeUnit.SECONDS));
        return DateTimeFormatter.ISO_TIME.format(seconds);
    }

    public static String formatToSeconds(long seconds) {
        return DateTimeFormatter.ISO_TIME.format(LocalTime.ofSecondOfDay(seconds));
    }

    public static String formatToMillis(Stopwatch stopwatch) {
        LocalTime nanoSeconds = LocalTime.ofNanoOfDay(stopwatch.elapsed(TimeUnit.MILLISECONDS) * 1000000);
        return DateTimeFormatter.ISO_TIME.format(nanoSeconds);
    }
}
