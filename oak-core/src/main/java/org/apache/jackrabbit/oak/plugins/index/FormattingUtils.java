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
    public static String formatToSeconds(Stopwatch stopwatch) {
        long seconds = stopwatch.elapsed(TimeUnit.SECONDS);
        long absSeconds = Math.abs(seconds);
        return String.format(
                "%02d:%02d:%02d",
                absSeconds / 3600,
                (absSeconds % 3600) / 60,
                absSeconds % 60);
    }

    public static String formatToMillis(Stopwatch stopwatch) {
        long millis = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        long absMillis = Math.abs(millis);
        return String.format(
                "%02d:%02d:%02d.%03d",
                absMillis / 3_600_000,
                (absMillis % 3_600_000) / 60_000,
                absMillis % 60_000/1_000,
                absMillis % 1_000
                );
    }
}
