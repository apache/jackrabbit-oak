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
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.ValueType;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils.HyperLogLog;

/**
 * A histogram of distinct binaries. For each size range, we calculate the
 * number of entries and number of distinct entries. The number of distinct
 * entries is calculated using a set if the number of entries is smaller than
 * 1024, or HyperLogLog otherwise.
 */
public class DistinctBinarySizeHistogram implements StatsCollector {

    private final int pathLevels;
    private final Storage storage = new Storage();
    private final HashMap<String, HyperLogLog> distinctMap = new HashMap<>();

    public DistinctBinarySizeHistogram(int pathLevels) {
        this.pathLevels = pathLevels;
    }

    public void add(NodeData node) {
        ArrayList<BinaryId> list = new ArrayList<>();
        for(NodeProperty p : node.getProperties()) {
            if (p.getType() == ValueType.BINARY) {
                for (String v : p.getValues()) {
                    if (!v.startsWith(":blobId:")) {
                        continue;
                    }
                    v = v.substring(":blobId:".length());
                    if (v.startsWith("0x")) {
                        // embedded: ignore
                    } else {
                        // reference
                        list.add(new BinaryId(v));
                    }
                }
            }
        }
        if (list.isEmpty()) {
            return;
        }
        add("/", list);
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < pathLevels && i < node.getPathElements().size(); i++) {
            String pe = node.getPathElements().get(i);
            buff.append('/').append(pe);
            String key = buff.toString();
            add(key, list);
        }
        storage.add("total count", list.size());
        for (BinaryId id : list) {
            storage.add("total size", id.getLength());
        }
    }

    void add(String key, ArrayList<BinaryId> list) {
        for (BinaryId id : list) {
            int bits = 65 - Long.numberOfLeadingZeros(id.getLength());
            String k = key + " " + BinarySizeHistogram.SIZES[bits];
            HyperLogLog hll = distinctMap.computeIfAbsent(k, n -> new HyperLogLog(1024, 1024));
            hll.add(id.getLongHash());
            storage.add(k, 1L);
        }
    }

    public void end() {
        for(Entry<String, HyperLogLog> e : distinctMap.entrySet()) {
            storage.put(e.getKey() + " distinct", e.getValue().estimate());
        }
    }

    public List<String> getRecords() {
        return BinarySizeHistogram.getRecordsWithSizeAndCount(storage);
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("DistinctBinarySizeHistogram\n");
        buff.append(getRecords().stream().map(s -> s + "\n").collect(Collectors.joining()));
        buff.append(storage);
        return buff.toString();
    }

}
