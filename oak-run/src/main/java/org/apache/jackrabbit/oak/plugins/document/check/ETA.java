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
package org.apache.jackrabbit.oak.plugins.document.check;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.util.ISO8601;

/**
 * <code>ETA</code>...
 */
public final class ETA {

    private final long total;

    private final Stopwatch sw;

    public ETA(long total) {
        this.total = total;
        this.sw = Stopwatch.createStarted();
    }

    public long estimateArrival(long current) {
        long remaining = total - current;
        long timeTook = sw.elapsed(TimeUnit.MILLISECONDS);
        long timeRemaining = timeTook * remaining / current;
        return System.currentTimeMillis() + timeRemaining;
    }

    public String percentageComplete(long current) {
        float p = 0.0f;
        if (total != 0) {
            p = 100f * current / total;
        }
        return String.format("%.1f%%", p);
    }

    public String estimateArrivalAsISO8601(long current) {
        return ISO8601.format(estimateArrival(current));
    }
}
