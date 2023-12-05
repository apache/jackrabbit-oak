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

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.StatsCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Storage;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property.ValueType;

public class BinarySizeHistogram implements StatsCollector {

    int pathLevels;
    Storage storage;
    
    private static final String[] SIZES = new String[64];
    
    static {
        SIZES[0] = "0";
        for (long x = 1; x > 0; x += x) {
            int n = 64 - Long.numberOfLeadingZeros(x);
            SIZES[n] = String.format("%2d", n) + " (" + (x / 2 + 1) + ".." + x + ")";
        }
    }
    
    public BinarySizeHistogram(int pathLevels) {
        this.pathLevels = pathLevels;
    }
    
    @Override
    public void setStorage(Storage storage) {
        this.storage = storage;
    }
    
    public void add(List<String> pathElements, List<Property> properties) {
        ArrayList<Long> embedded = new ArrayList<>();
        ArrayList<Long> references = new ArrayList<>();
        for(Property p : properties) {
            if (p.getType() == ValueType.STRING) {
                for (String v : p.getValues()) {
                    if (!v.startsWith(":blobId:")) {
                        continue;
                    }
                    v = v.substring(":blobId:".length());
                    if (v.startsWith("0x")) {
                        // embedded
                        long size = (v.length() - 2) / 2;
                        embedded.add(size);
                    } else {
                        // reference
                        int hashIndex = v.indexOf('#');
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
        for (int i = 0; i < pathLevels && i < pathElements.size(); i++) {
            String pe = pathElements.get(i);
            buff.append('/').append(pe);
            String key = buff.toString();
            add(key, embedded, references);
        }
    }
    
    void add(String key, ArrayList<Long> embedded, ArrayList<Long> references) {
        for(long x : embedded) {
            int bits = 65 - Long.numberOfLeadingZeros(x);
            storage.add("embedded " + key + " " + SIZES[bits], 1L);
            storage.add("embedded total count", 1L);
            storage.add("embedded total size", x);
        }
        for(long x : references) {
            int bits = 65 - Long.numberOfLeadingZeros(x);
            storage.add("refs " + key + " " + SIZES[bits], 1L);
            storage.add("refs total count", 1L);
            storage.add("refs total size", x);
        }
    }
    
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("BinarySizeHistogram\n");
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
