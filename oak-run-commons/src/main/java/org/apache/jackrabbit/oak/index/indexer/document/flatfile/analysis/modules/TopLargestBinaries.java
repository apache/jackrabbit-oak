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
import java.util.List;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Property.ValueType;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.StatsCollector;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.Storage;

public class TopLargestBinaries implements StatsCollector {
    
    Storage storage;
    final int k;
    final ArrayList<TopEntry> top = new ArrayList<>();
    long totalCount;
    long totalSize;
    
    public TopLargestBinaries(int k) {
        this.k = k;
    }

    @Override
    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    @Override
    public void add(NodeData node) {
        List<Property> properties = node.properties;
        List<String> pathElements = node.pathElements;
        ArrayList<Long> references = new ArrayList<>();
        for(Property p : properties) {
            if (p.getType() == ValueType.BINARY) {
                for (String v : p.getValues()) {
                    if (!v.startsWith(":blobId:")) {
                        continue;
                    }
                    v = v.substring(":blobId:".length());
                    if (v.startsWith("0x")) {
                        // embedded
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
        for(long x : references) {
            if (top.size() >= k && x < top.get(0).size) {
                continue;
            }
            totalCount++;
            totalSize += x;
            TopEntry e = new TopEntry(x, pathElements);
            int index = Collections.binarySearch(top, e);
            if (index < 0) {
                index = -index - 1;
            }
            top.add(index, e);
            if (top.size() > k) {
                top.remove(0);
            }
        }
    }
    
    public void end() {
        int i = 0;
        for(TopEntry e: top) {
            String path = "#" + String.format("%3d", k - i) + ": /" + String.join("/", e.pathElements);
            storage.add(path, e.size);
            i++;
        }
        storage.add("total count", totalCount);
        storage.add("total size", totalSize);
    }
    
    static class TopEntry implements Comparable<TopEntry> {
        final long size;
        final List<String> pathElements;
        
        TopEntry(long count, List<String> pathElements) {
            this.size = count;
            this.pathElements = pathElements;
        }
        
        @Override
        public int compareTo(TopEntry o) {
            return Long.compare(size, o.size);
        }
    }
    
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("TopLargestBinaries\n");
        for(Entry<String, Long> e : storage.entrySet()) {
            buff.append(e.getKey() + ": " + e.getValue()).append('\n');
        }
        buff.append(storage);
        return buff.toString();
    }    
    

}
