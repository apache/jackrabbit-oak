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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class BoundedHistogram {
    private static final Logger LOG = LoggerFactory.getLogger(PipelinedStrategy.class);

    private final ConcurrentHashMap<String, LongAdder> histogram = new ConcurrentHashMap<>();
    private final AtomicBoolean overflowed = new AtomicBoolean(false);
    private final String histogramName;
    private final int maxHistogramSize;

    public BoundedHistogram(String name, int maxHistogramSize) {
        this.histogramName = name;
        this.maxHistogramSize = maxHistogramSize;
    }

    public void addEntry(String key) {
        if (!overflowed.get() && histogram.size() >= maxHistogramSize) {
            overflowed.set(true);
            LOG.warn("{} histogram overflowed (Max entries: {}). No more entries will be added, current entries will still be updated.",
                    histogramName, maxHistogramSize);
        }
        if (overflowed.get()) {
            LongAdder counter = histogram.get(key);
            if (counter != null) {
                counter.increment();
            }
        } else {
            histogram.computeIfAbsent(key, k -> new LongAdder()).increment();
        }
    }

    public String prettyPrint() {
        return histogram.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().sum()))
                .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue())) // sort by value descending
                .limit(20)
                .map(e -> "\"" + e.getKey() + "\":" + e.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
    }

    public ConcurrentHashMap<String, LongAdder> getMap() {
        return histogram;
    }
}
