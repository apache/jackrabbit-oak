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
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * A histogram that keeps a maximum number of buckets (entries). When the histogram is at the limit, it will reject
 * new entries but keep updating the count of the existing entries. Therefore, the counts for the entries in the
 * histogram are correct but if the histogram overflowed, it may be missing some entries.
 */
public class BoundedHistogram {
    private static final Logger LOG = LoggerFactory.getLogger(BoundedHistogram.class);
    private final ConcurrentHashMap<String, LongAdder> histogram = new ConcurrentHashMap<>();
    private volatile boolean overflowed = false;
    private final String histogramName;
    private final int maxHistogramSize;

    public BoundedHistogram(String name, int maxHistogramSize) {
        this.histogramName = name;
        this.maxHistogramSize = maxHistogramSize;
    }

    public void addEntry(String key) {
        if (!overflowed && histogram.size() >= maxHistogramSize) {
            // Print the log message only the first time that the histogram overflows.
            // There is a race condition here, in rare cases this may print the message more than once but that's ok
            overflowed = true;
            LOG.warn("{} histogram overflowed (Max entries: {}). No more entries will be added, current entries will still be updated.",
                    histogramName, maxHistogramSize);
        }
        if (overflowed) {
            LongAdder counter = histogram.get(key);
            if (counter != null) {
                counter.increment();
            }
        } else {
            histogram.computeIfAbsent(key, k -> new LongAdder()).increment();
        }
    }

    public boolean isOverflowed() {
        return overflowed;
    }

    public ConcurrentHashMap<String, LongAdder> getMap() {
        return histogram;
    }

    public String prettyPrint() {
        return prettyPrintTopEntries(20);
    }

    public String prettyPrintTopEntries(int numEntries) {
        String str = histogram.entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue().sum()))
                .sorted((e1, e2) -> Long.compare(e2.getValue(), e1.getValue())) // sort by value descending
                .limit(numEntries)
                .map(e -> "\"" + e.getKey() + "\":" + e.getValue())
                .collect(Collectors.joining(", ", "{", "}"));
        if (overflowed) {
            return str + ". Histogram overflowed (max buckets " + maxHistogramSize + ") some buckets may be missing";
        } else {
            return str;
        }
    }
}
