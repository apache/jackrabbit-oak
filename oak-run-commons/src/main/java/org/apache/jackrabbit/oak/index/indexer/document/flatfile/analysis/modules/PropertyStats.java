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
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.StatsCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Storage;

public class PropertyStats implements StatsCollector {
    
    private static final String COUNT = "count ";
    private static final String VALUES = "values ";
    private static final String DISTINCT = "distinct ";
    
    Storage storage;
    CountMinSketch propertyCountSketch = new CountMinSketch(63, 64);
    private final long seed = 42;
    List<IndexedProperty> indexedProperties;
    HashMap<String, ArrayList<IndexedProperty>> map;

    // we start collecting top k values once we have seen this many entries to save memory
    private final static long MIN_TOP_K = 5_000;

    private final static int TOP_K = 10;

    // we only consider the most common properties
    // to protect against millions of unique properties
    private final static long MAX_SIZE = 100_000;
    
    private final TreeMap<String, TopKValues> topEntries = new TreeMap<>();

    @Override
    public void setStorage(Storage storage) {
        this.storage = storage;
    }
    
    public void setIndexedProperties(HashMap<String, ArrayList<IndexedProperty>> map) {
        this.map = map;
    }

    @Override
    public void add(List<String> pathElements, List<Property> properties) {
        // TODO also consider path (first n levels)
        for(Property p : properties) {
            String name = p.getName();
            if (map != null) {
                ArrayList<IndexedProperty> list = map.get(name);
                if (list != null) {
                    for(IndexedProperty ip : list) {
                        if (ip.matches(name, pathElements)) {
                            add(ip.toString(), p);
                        }
                    }
                }
            }
            add(name, p);
        }
    }
    
    private void add(String name, Property p) {
        long count = storage.add(COUNT + name, 1L);
        storage.add(VALUES + name, (long) p.getValues().length);
        addValues(name, p);
        removeRareEntries();
        if (count >= MIN_TOP_K) {
            TopKValues top = topEntries.get(name);
            if (top == null) {
                top = new TopKValues(TOP_K);
                topEntries.put(name, top);
            }
            for (String v : p.getValues()) {
                top.add(v);
            }
        }
    }

    private void addValues(String name, Property p) {
        Long old = storage.get(DISTINCT + name);
        long hll = old == null ? 0 : old;
        for (String v : p.getValues()) {
            long hash = Hash.hash64(v.hashCode(), seed);
            hll = HyperLogLog3Linear64.add(hll, hash);
        }
        storage.put(DISTINCT + name, hll);
    }

    /**
     * Remove entries with a low count if there are too many entries.
     */
    private void removeRareEntries() {
        if (storage.size() < MAX_SIZE * 2) {
            return;
        }
        ArrayList<Entry<String, Long>> list = new ArrayList<>(storage.entrySet());
        ArrayList<Long> counts = new ArrayList<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            String n = list.get(i).getKey();
            if (n.startsWith(COUNT)) {
                counts.add(list.get(i).getValue());
            }
        }
        Collections.sort(counts);
        long mean = counts.get((int) MAX_SIZE);
        for (int i = 0; i < list.size(); i++) {
            Entry<String, Long> e = list.get(i);
            if (e.getValue() <= mean) {
                storage.remove(COUNT + e.getKey());
                storage.remove(VALUES + e.getKey());
                storage.remove(DISTINCT + e.getKey());
            }
        }
    }
        
    @Override
    public void end() {
    }
    
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("PropertyCount\n");
        for(Entry<String, Long> e : storage.entrySet()) {
            String key = e.getKey();
            if (key.startsWith(COUNT) && e.getValue() > 100) {
                String propertyName = key.substring(COUNT.length());
                long count = e.getValue();
                long values = storage.get(VALUES + propertyName);
                long hll = storage.get(DISTINCT + propertyName);
                buff.append(propertyName).append(": {count:").append(count);
                if (values != count) {
                    buff.append(", values:").append(values);
                }
                buff.append(", distinct:").append(HyperLogLog3Linear64.estimate(hll));
                TopKValues top = topEntries.get(propertyName);
                if (top != null) {
                    buff.append(", top:").append(top.toString());
                }
                buff.append("}\n");
            }
        }
        buff.append(storage);
        return buff.toString();
    }    

}
