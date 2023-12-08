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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.StatsCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Storage;

public class PropertyStats implements StatsCollector {
    
    Storage storage;
    private final long seed = 42;
    List<IndexedProperty> indexedProperties;
    HashMap<String, ArrayList<IndexedProperty>> indexexPropertyMap;
    
    HashSet<String> indexProperties = new HashSet<String>();
    
    private final static boolean INDEXED_PROPERTIES_ONLY_WITHOUT_CHECKS = true;
    
    private final static int SKIP = 5;

    // we start collecting distinct values data once we have seen this many entries
    // (to speed up processing)
    private final static long MIN_PROPERTY_COUNT = 1000;

    // we start collecting top k values once we have seen this many entries
    // (to save memory and speed up processing)
    private final static long MIN_TOP_K = 10_000;

    private final static int TOP_K = 8;

    // we only consider the most common properties
    // to protect against millions of unique properties
    private final static long MAX_SIZE = 100_000;
    
    private final TreeMap<String, Stats> statsMap = new TreeMap<>();
    
    private int skip;

    @Override
    public void setStorage(Storage storage) {
        this.storage = storage;
    }
    
    public void setIndexedProperties(HashMap<String, ArrayList<IndexedProperty>> map) {
        this.indexexPropertyMap = map;
        indexProperties.addAll(map.keySet());
    }

    @Override
    public void add(NodeData node) {
        if (skip > 0) {
            skip--;
            return;
        }
        skip = SKIP;
        List<Property> properties = node.getProperties();
        // TODO maybe also consider path (first n levels)
        for(Property p : properties) {
            String name = p.getName();
            if (INDEXED_PROPERTIES_ONLY_WITHOUT_CHECKS) {
                if (indexProperties != null && !indexProperties.contains(name)) {
                    continue;
                }
                add(name, p);
            } else {
                if (indexexPropertyMap != null) {
                    ArrayList<IndexedProperty> list = indexexPropertyMap.get(name);
                    if (list != null) {
                        for(IndexedProperty ip : list) {
                            if (ip.matches(name, node)) {
                                add(name + " " + ip.toString(), p);
                            }
                        }
                    }
                }
                add(name + " nt:base*", p);
            }
        }
    }
    
    private void add(String name, Property p) {
        Stats stats = statsMap.computeIfAbsent(name, e -> new Stats(name));
        stats.count++;
        stats.values += p.getValues().length;
        if (stats.count > MIN_PROPERTY_COUNT) {
            for (String v : p.getValues()) {
                long hash = Hash.hash64(v.hashCode(), seed);
                stats.hll = HyperLogLog3Linear64.add(stats.hll, hash);
                stats.size += v.length();
                stats.maxSize = Math.max(stats.maxSize, v.length());
            }
        }
        if (stats.count >= MIN_TOP_K) {
            TopKValues top = stats.topValues;
            if (top == null) {
                top = new TopKValues(TOP_K);
                stats.topValues = top;
            }
            for (String v : p.getValues()) {
                top.add(v);
            }
        }
        removeRareEntries();
    }

    /**
     * Remove entries with a low count if there are too many entries.
     */
    private void removeRareEntries() {
        if (statsMap.size() < MAX_SIZE * 2) {
            return;
        }
        ArrayList<Entry<String, Stats>> list = new ArrayList<>(statsMap.entrySet());
        ArrayList<Long> counts = new ArrayList<>(list.size());
        for (Entry<String, Stats> e : list) {
            counts.add(e.getValue().count);
        }
        Collections.sort(counts);
        long mean = counts.get((int) MAX_SIZE);
        for (Entry<String, Stats> e : list) {
            if (e.getValue().count <= mean) {
                statsMap.remove(e.getKey());
            }
        }
    }
        
    @Override
    public void end() {
    }
    
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("PropertyStats\n");
        for(Stats stats : statsMap.values()) {
            if (stats.count < MIN_PROPERTY_COUNT) {
                continue;
            }
            long weight = 5;
            long count = stats.count;
            long distinct = HyperLogLog3Linear64.estimate(stats.hll);
            
            TopKValues top = stats.topValues;
            weight = distinct;
            if (top != null) {
                if (!top.isNotSkewed()) {
                    // we can not trust the number of distinct entries
                    long topTotalCount = top.getCount();
                    long avgOf2 = (top.getTopCount() + top.getSecondCount()) / 2;
                    weight = Math.min(distinct, topTotalCount / Math.max(1, avgOf2));
                }
            }
            if (weight >= 10000) {
                weight = 10000;
            } else if (weight >= 500) {
                weight = weight / 100 * 100;
            } else if (weight >= 40) {
                weight = weight / 10 * 10;
            } else if (weight == 0) {
                weight = 1;
            }
            buff.append(stats.name);
            buff.append(" weight ").append(weight);
            buff.append(" count ").append(count);
            buff.append(" distinct ").append(distinct);
            buff.append(" avgSize ").append(stats.size / Math.max(1, stats.values));
            buff.append(" maxSize ").append(stats.maxSize);
            if (stats.count != stats.values) {
                buff.append(" values ").append(stats.values);
            }
            if (top != null) {
                buff.append(" top ").append(top.toString());
            }
            buff.append("\n");
        }
        buff.append(storage);
        return buff.toString();
    }
    
    static class Stats {
        public Stats(String name) {
            this.name = name;
        }
        String name;
        long count;
        long values;
        long hll;
        long size;
        long maxSize;
        TopKValues topValues;
    }

}
