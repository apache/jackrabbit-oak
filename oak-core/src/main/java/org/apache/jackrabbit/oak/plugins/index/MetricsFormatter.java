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
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;

import java.util.concurrent.TimeUnit;

public class MetricsFormatter {

    public static String createMetricsWithDurationOnly(Stopwatch taskStartWatch) {
        return createMetricsWithDurationOnly(taskStartWatch.elapsed(TimeUnit.SECONDS));
    }

    public static String createMetricsWithDurationOnly(long totalDurationSeconds) {
        return MetricsFormatter.newBuilder()
                .add("duration", FormattingUtils.formatToSeconds(totalDurationSeconds))
                .add("durationSeconds", totalDurationSeconds)
                .build();
    }

    public static MetricsFormatter newBuilder() {
        return new MetricsFormatter();
    }

    private final JsopBuilder jsopBuilder = new JsopBuilder();
    private boolean isWritable = true;

    private MetricsFormatter() {
        jsopBuilder.object();
    }

    public MetricsFormatter add(String key, String value) {
        Validate.checkState(isWritable, "Formatter already built, in read-only mode");
        jsopBuilder.key(key).value(value);
        return this;
    }

    public MetricsFormatter add(String key, int value) {
        Validate.checkState(isWritable, "Formatter already built, in read-only mode");
        jsopBuilder.key(key).value(value);
        return this;
    }

    public MetricsFormatter add(String key, long value) {
        Validate.checkState(isWritable, "Formatter already built, in read-only mode");
        jsopBuilder.key(key).value(value);
        return this;
    }

    public MetricsFormatter add(String key, boolean value) {
        Validate.checkState(isWritable, "Formatter already built, in read-only mode");
        jsopBuilder.key(key).value(value);
        return this;
    }

    public String build() {
        if (isWritable) {
            jsopBuilder.endObject();
            isWritable = false;
        }
        return jsopBuilder.toString();
    }
}
