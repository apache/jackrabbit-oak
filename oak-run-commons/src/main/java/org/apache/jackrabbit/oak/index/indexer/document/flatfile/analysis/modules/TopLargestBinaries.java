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
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeData;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.stream.NodeProperty.ValueType;

/**
 * Collect the top largest binaries.
 */
public class TopLargestBinaries implements StatsCollector {

    private final Storage storage = new Storage();
    private final int k;
    private final ArrayList<TopEntry> top = new ArrayList<>();

    public TopLargestBinaries(int k) {
        this.k = k;
    }

    @Override
    public void add(NodeData node) {
        List<NodeProperty> properties = node.getProperties();
        List<String> pathElements = node.getPathElements();
        ArrayList<Long> references = new ArrayList<>();
        for(NodeProperty p : properties) {
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

    @Override
    public void end() {
        int i = 0;
        for(TopEntry e: top) {
            String path = "#" + String.format("%3d", k - i) + ": /" + String.join("/", e.pathElements);
            storage.add(path, e.size);
            i++;
        }
    }

    public List<String> getRecords() {
        List<String> result = new ArrayList<>();
        for(Entry<String, Long> e : storage.entrySet()) {
            result.add(e.getKey() + ": " + e.getValue());
        }
        return result;
    }

    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("TopLargestBinaries\n");
        buff.append(getRecords().stream().map(s -> s + "\n").collect(Collectors.joining()));
        buff.append(storage);
        return buff.toString();
    }

    static class TopEntry implements Comparable<TopEntry> {
        private final long size;
        private final List<String> pathElements;

        TopEntry(long count, List<String> pathElements) {
            this.size = count;
            this.pathElements = pathElements;
        }

        @Override
        public int compareTo(TopEntry o) {
            return Long.compare(size, o.size);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pathElements, size);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            TopEntry other = (TopEntry) obj;
            return Objects.equals(pathElements, other.pathElements) && size == other.size;
        }

    }

}
