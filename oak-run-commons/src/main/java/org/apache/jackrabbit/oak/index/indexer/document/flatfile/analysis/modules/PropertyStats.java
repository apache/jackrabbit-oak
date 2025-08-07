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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils.Hash;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils.HyperLogLog3Linear64;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils.TopKValues;

/**
 * Property statistics.
 */
public class PropertyStats implements StatsCollector {

    // we start collecting distinct values data once we have seen this many entries
    // (to speed up processing)
    private static final long MIN_PROPERTY_COUNT = 500;

    // we start collecting top k values once we have seen this many entries
    // (to save memory and speed up processing)
    private static final long MIN_TOP_K = 10_000;

    // the number of top entries to collect
    private static final int TOP_K = 8;

    // we only consider the most common properties
    // to protect against millions of unique properties
    private static final long MAX_SIZE = 100_000;

    // only collect statistics for indexed properties
    private final boolean indexedPropertiesOnly;
    private HashMap<String, ArrayList<IndexedProperty>> indexexPropertyMap;
    private HashSet<String> indexedProperties = new HashSet<>();

    private final long seed;
    private final TreeMap<String, Stats> statsMap = new TreeMap<>();

    // skip this many entries (for sampling)
    private int skip = 1;

    private int skipRemaining;

    public PropertyStats(boolean indexedPropertiesOnly, long seed) {
        this.indexedPropertiesOnly = indexedPropertiesOnly;
        this.seed = seed;
    }

    public void setSkip(int skip) {
        this.skip = skip;
    }

    public void setIndexedPropertiesSet(Set<String> set) {
        indexedProperties.addAll(set);
    }

    public void setIndexedProperties(Map<String, ArrayList<IndexedProperty>> map) {
        indexexPropertyMap.putAll(map);
        indexedProperties.addAll(map.keySet());
    }

    @Override
    public void add(NodeData node) {
        if (skipRemaining > 0) {
            skipRemaining--;
            return;
        }
        skipRemaining = skip;
        List<NodeProperty> properties = node.getProperties();
        for(NodeProperty p : properties) {
            String name = p.getName();
            if (indexedPropertiesOnly) {
                if (indexedProperties != null && !indexedProperties.contains(name)) {
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
                add(name, p);
            }
        }
    }

    private void add(String name, NodeProperty p) {
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

    public List<String> getRecords() {
        List<String> result = new ArrayList<>();
        for(Stats stats : statsMap.values()) {
            StringBuilder buff = new StringBuilder();
            if (stats.count < MIN_PROPERTY_COUNT) {
                continue;
            }
            long count = stats.count;
            long distinct = HyperLogLog3Linear64.estimate(stats.hll);

            long weight = distinct;
            TopKValues top = stats.topValues;
            if (top != null && !top.isNotSkewed()) {
                // we can not trust the number of distinct entries
                long topTotalCount = top.getCount();
                long avgOf2 = (top.getTopCount() + top.getSecondCount()) / 2;
                weight = Math.min(distinct, topTotalCount / Math.max(1, avgOf2));
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
            result.add(buff.toString());
        }
        return result;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("PropertyStats\n");
        buff.append(getRecords().stream().map(s -> s + "\n").collect(Collectors.joining()));
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
