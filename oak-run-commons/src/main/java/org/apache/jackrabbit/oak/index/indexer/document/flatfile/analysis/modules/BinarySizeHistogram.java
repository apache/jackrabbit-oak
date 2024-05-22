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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.modules;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.ValueType;

/**
 * Collects the histogram of binary sizes (embedded binaries and references to
 * the datastore). Collection is done for a certain number of path levels, in
 * addition to the root.
 */
public class BinarySizeHistogram implements StatsCollector {

    private final int pathLevels;
    private final Storage storage = new Storage();

    public static final String[] SIZES = new String[64];
    static {
        SIZES[0] = " 0";
        String[] mag = new String[] { "B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB" };
        String old = "1B";
        for (long x = 1; x > 0; x += x) {
            int n = 64 - Long.numberOfLeadingZeros(x);
            String m = (x >> ((n - 1) / 10 * 10)) + mag[(n - 1) / 10];
            SIZES[n] = String.format("%2d", n) + " (" + old + ".." + m + ")";
            old = m;
        }
    }

    public BinarySizeHistogram(int pathLevels) {
        this.pathLevels = pathLevels;
    }

    public void add(NodeData node) {
        ArrayList<Long> embedded = new ArrayList<>();
        ArrayList<Long> references = new ArrayList<>();
        for(NodeProperty p : node.getProperties()) {
            if (p.getType() == ValueType.BINARY) {
                for (String v : p.getValues()) {
                    if (!v.startsWith(":blobId:")) {
                        continue;
                    }
                    v = v.substring(":blobId:".length());
                    if (v.startsWith("0x")) {
                        // embedded
                        int hashIndex = v.lastIndexOf('#');
                        if (hashIndex >= 0) {
                            v = v.substring(0, hashIndex);
                        }
                        long size = (v.length() - 2) / 2;
                        embedded.add(size);
                    } else {
                        // reference
                        int hashIndex = v.lastIndexOf('#');
                        String length = v.substring(hashIndex + 1);
                        long size = Long.parseLong(length);
                        references.add(size);
                    }
                }
            }
        }
        if (embedded.isEmpty() && references.isEmpty()) {
            return;
        }
        add("/", embedded, references);
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < pathLevels && i < node.getPathElements().size(); i++) {
            String pe = node.getPathElements().get(i);
            buff.append('/').append(pe);
            String key = buff.toString();
            add(key, embedded, references);
        }
    }

    void add(String key, ArrayList<Long> embedded, ArrayList<Long> references) {
        for(long x : embedded) {
            int bits = 65 - Long.numberOfLeadingZeros(x);
            storage.add("embedded " + key + " " + SIZES[bits], 1L);
            if ("/".equals(key)) {
                storage.add("embedded total count", 1L);
                storage.add("embedded total size", x);
            }
        }
        for(long x : references) {
            int bits = 65 - Long.numberOfLeadingZeros(x);
            storage.add("refs " + key + " " + SIZES[bits], 1L);
            if ("/".equals(key)) {
                storage.add("refs total count", 1L);
                storage.add("refs total size", x);
            }
        }
    }

    public static List<String> getRecordsWithSizeAndCount(Storage storage) {
        List<String> result = new ArrayList<>();
        for(Entry<String, Long> e : storage.entrySet()) {
            if (e.getValue() > 0) {
                String k = e.getKey();
                long v = e.getValue();
                result.add(k + ": " + v);
                if (k.endsWith(" size")) {
                    k += " GiB";
                    v /= 1024 * 1024 * 1024;
                    result.add(k + ": " + v);
                } else if (k.endsWith(" count")) {
                    k += " million";
                    v /= 1_000_000;
                    result.add(k + ": " + v);
                }
            }
        }
        return result;
    }

    public List<String> getRecords() {
        return getRecordsWithSizeAndCount(storage);
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("BinarySizeHistogram\n");
        buff.append(getRecords().stream().map(s -> s + "\n").collect(Collectors.joining()));
        buff.append(storage);
        return buff.toString();
    }

}
