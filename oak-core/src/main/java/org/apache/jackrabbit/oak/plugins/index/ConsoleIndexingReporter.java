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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.OakVersion;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.jetbrains.annotations.NotNull;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ConsoleIndexingReporter implements IndexingReporter {
    // Print configuration in alphabetical order
    private final Map<String, String> config = new TreeMap<>();
    // Print timings in the order they were added
    private final Map<String, String> timings = new LinkedHashMap<>();
    // Print configuration in alphabetical order
    private final Map<String, String> metrics = new TreeMap<>();
    private final List<String> envVariablesToLog;
    private List<String> indexes = List.of();

    public ConsoleIndexingReporter() {
        this(List.of());
    }

    /**
     * @param envVariablesToLog  These environment variables and their values will be included in the final report.
     */
    public ConsoleIndexingReporter(@NotNull List<String> envVariablesToLog) {
        this.envVariablesToLog = List.copyOf(envVariablesToLog);
    }

    public void setIndexNames(@NotNull List<String> indexes) {
        this.indexes = List.copyOf(indexes);
    }

    public void addConfig(String key, Object value) {
        config.put(key, value.toString());
    }

    public void addTiming(String stage, String time) {
        timings.put(stage, time);
    }

    public void addMetric(String name, long value) {
        metrics.put(name, String.valueOf(value));
    }

    public void addMetricByteSize(String name, long value) {
        String v = String.valueOf(value);
        if (value >= FileUtils.ONE_KB) {
            v += " (" + IOUtils.humanReadableByteCountBin(value) + ")";
        }
        metrics.put(name, v);
    }

    public String generateReport() {
        return "Indexes: " + String.join(", ", indexes) + "\n" +
                "Date: " + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now()) + "\n" +
                "OAK Version: " + OakVersion.getVersion() + "\n" +
                "Configuration:\n" + mapToString(config) + "\n" +
                "Environment Variables:\n" + genEnvVariables() + "\n" +
                "Timings:\n" + mapToString(timings) + "\n" +
                "Metrics:\n" + mapToString(metrics);
    }

    private String genEnvVariables() {
        return envVariablesToLog.stream()
                .sorted()
                .map(var -> "  " + var + ": " + System.getenv(var))
                .collect(Collectors.joining("\n"));
    }

    private String mapToString(Map<String, String> map) {
        return map.entrySet().stream()
                .map(entry -> "  " + entry.getKey() + ": " + entry.getValue())
                .collect(Collectors.joining("\n"));
    }
}
