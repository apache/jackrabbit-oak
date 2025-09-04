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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConsoleIndexingReporter implements IndexingReporter {
    // Print timings in the order they were added
    private final Map<String, String> timings = new LinkedHashMap<>();
    // Print metrics and configuration in alphabetical order. They are sorted when printed.
    private final Map<String, String> metrics = new HashMap<>();
    private final Map<String, String> config = new HashMap<>();
    private final List<String> envVariablesToLog;
    private List<String> indexes = List.of();
    private final List<String> informationStrings = new ArrayList<>();

    public ConsoleIndexingReporter() {
        this(List.of());
    }

    /**
     * @param envVariablesToLog These environment variables and their values will be included in the final report.
     */
    public ConsoleIndexingReporter(@NotNull List<String> envVariablesToLog) {
        this.envVariablesToLog = List.copyOf(envVariablesToLog);
    }

    public synchronized void setIndexNames(@NotNull List<String> indexes) {
        this.indexes = List.copyOf(indexes);
    }

    public synchronized void addConfig(String key, Object value) {
        config.put(key, value.toString());
    }

    public synchronized void addTiming(String stage, String time) {
        timings.put(stage, time);
    }

    public synchronized void addMetric(String name, long value) {
        metrics.put(name, String.valueOf(value));
    }

    @Override
    public synchronized void addInformation(String value) {
        informationStrings.add(value);
    }

    public void addMetricByteSize(String name, long value) {
        String v = String.valueOf(value);
        if (value >= FileUtils.ONE_KB) {
            v += " (" + IOUtils.humanReadableByteCountBin(value) + ")";
        }
        synchronized (this) {
            metrics.put(name, v);
        }
    }

    public synchronized String generateReport() {
        return "Indexes: " + String.join(", ", indexes) + "\n" +
                "Date: " + DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.now()) + "\n" +
                "OAK Version: " + OakVersion.getVersion() + "\n" +
                "Configuration:\n" + mapToString(config, true) + "\n" +
                "Environment Variables:\n" + genEnvVariables() + "\n" +
                "Information:\n" + listToString(informationStrings) + "\n" +
                "Timings:\n" + mapToString(timings, false) + "\n" +
                "Metrics:\n" + mapToString(metrics, true);
    }

    private String genEnvVariables() {
        return envVariablesToLog.stream()
                .sorted()
                .map(var -> "  " + var + ": " + System.getenv(var))
                .collect(Collectors.joining("\n"));
    }

    private String mapToString(Map<String, String> map, boolean sortKeys) {
        Stream<Map.Entry<String, String>> stream = map.entrySet().stream();
        if (sortKeys) {
            stream = stream.sorted(Map.Entry.comparingByKey());
        }
        return stream.map(entry -> "  " + entry.getKey() + ": " + entry.getValue())
                .collect(Collectors.joining("\n"));
    }

    private String listToString(List<String> map) {
        return map.stream()
                .sorted()
                .map(entry -> "  " + entry)
                .collect(Collectors.joining("\n"));
    }
}
