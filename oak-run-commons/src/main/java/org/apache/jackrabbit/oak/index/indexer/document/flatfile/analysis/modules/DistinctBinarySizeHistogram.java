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
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property.ValueType;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.StatsCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Storage;

public class DistinctBinarySizeHistogram implements StatsCollector {

    int pathLevels;
    Storage storage;

    private static HashMap<String, HyperLogLog> distinctMap = new HashMap<>();
    private static final String[] SIZES = new String[64];
    
    static {
        SIZES[0] = "0";
        for (long x = 1; x > 0; x += x) {
            int n = 64 - Long.numberOfLeadingZeros(x);
            SIZES[n] = String.format("%2d", n) + " (" + (x / 2 + 1) + ".." + x + ")";
        }
    }
    
    public DistinctBinarySizeHistogram(int pathLevels) {
        this.pathLevels = pathLevels;
    }
    
    @Override
    public void setStorage(Storage storage) {
        this.storage = storage;
    }
    
    public void add(NodeData node) {
        ArrayList<Long> hashSizePairs = new ArrayList<>();
        for(Property p : node.getProperties()) {
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
                        int hashIndex = v.indexOf('#');
                        long hash = Hash.hash64(v.hashCode());
                        String length = v.substring(hashIndex + 1);
                        long size = Long.parseLong(length);
                        hashSizePairs.add(hash);
                        hashSizePairs.add(size);
                    }
                }
            }
        }
        if (hashSizePairs.isEmpty()) {
            return;
        }
        add("/", hashSizePairs);
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < pathLevels && i < node.getPathElements().size(); i++) {
            String pe = node.getPathElements().get(i);
            buff.append('/').append(pe);
            String key = buff.toString();
            add(key, hashSizePairs);
        }
    }
    
    void add(String key, ArrayList<Long> hashSizePairs) {
        for(int i=0; i<hashSizePairs.size(); i+=2) {
            long hash = hashSizePairs.get(i);
            long size = hashSizePairs.get(i + 1);
            int bits = 65 - Long.numberOfLeadingZeros(size);
            String k = key + " " + SIZES[bits];
            HyperLogLog hll = distinctMap.computeIfAbsent(k, n -> new HyperLogLog(128));
            hll.add(hash);
            long est = hll.estimate();
            storage.add(k, 1L);
            storage.put(k + " distinct", est);
            storage.add("total count", 1L);
            storage.add("total size", size);
        }
    }
    
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("DistinctBinarySizeHistogram\n");
        for(Entry<String, Long> e : storage.entrySet()) {
            if (e.getValue() > 0) {
                buff.append(e.getKey() + ": " + e.getValue()).append('\n');
            }
        }
        buff.append(storage);
        return buff.toString();
    }

    @Override
    public void end() {
    }    
    
}
